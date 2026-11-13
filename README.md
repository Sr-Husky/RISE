# RISE

Protótipo experimental de sistema de arquivos em espaço de usuário (FUSE) que combina io_uring, processamento assíncrono, batching e compressão LZ4 no caminho de dados. Não é um sistema de arquivos de produção. Não use com dados que você não pode perder. É uma implementação de referência do conceito avaliado no artigo:

Obs: O artigo foi apresentado no XXVII Simpósio em Sistemas Computacionais de Alto Desempenho (SSCAD 2026) da SBC no dia 05/11/2026, atualmente (13/11/2026) ainda não está disponível na SBC OpenLib, assim que a publicação for feita esse documento será atualizado para incluir o link do artigo.

>*Mitigação do Custo de I/O em Sistemas de Arquivos em Espaço de Usuário com Processamento Assíncrono, Batching e Compressão.*

**English summary.** RISE is an experimental FUSE-based user-space file system. Its data path combines io_uring (SQPOLL), batched writes through double buffering, 4 MiB blocks compressed with LZ4 (FAST mode) and a 256 MiB read cache. It was built to simulate the pipeline proposed in the paper and to collect measurements, not to be a complete file system. The usage constraints listed below were not implemented.

---

## Antes de tudo

- **A versão do artigo não é a versão publicada.** Os experimentos do artigo foram feitos com uma versão anterior deste código. Decidi publicar só agora, e o código já não é o mesmo. O **conceito não mudou**: este repositório serve como exemplo de como ele foi implementado, em linhas gerais. Os números do artigo **não** são reproduzidos diretamente por este código
- **O objetivo nunca foi criar um sistema de arquivos completo.** O objetivo era simular o pipeline proposto e coletar medidas. Não há consistência após falha, diretórios, persistência de metadados, controle de permissões nem conformidade POSIX completa.
- **As restrições de uso abaixo não foram implementadas**, porque não eram o propósito do projeto.


---

## Restrições de uso

As restrições abaixo são requisitos de uso. O sistema não verifica nem trata nenhuma delas.

1. **Escritas >= 4 MiB devem ser feitas em blocos de no mínimo 2,2 KiB**: Com mais de 2028 fragmentos acumulados num buffer e menos de 4 MiB no total (por exemplo, milhares de escritas de 1 KiB sem flush), o flush não consegue formar um bloco e não termina. Nesse caso, `fsync` e `close` podem não retornar.
2. **Escritas e `truncate` não invalidam o cache de leitura.** Não escreva ou trunque enquanto há leitura em andamento no arquivo. Depois de uma escrita sobre um bloco que já foi lido, a leitura pode devolver o conteúdo antigo.
3. **Não sobrescreva escritas anteriores.** Escritas que se sobrepõem diretamente a dados já escritos (mesmo intervalo de bytes na mesma atividade de escrita), e o resultado pode ficar incorreto.
4. **O pipeline de leitura não é acionado junto ao de escrita.** Não misture leituras e escritas concorrentes no mesmo arquivo.
5. **Não faça `truncate` durante uma escrita.**
6. **Dois processos não podem escrever no mesmo descritor de arquivo (fd).**

---

## O que o software faz

### Visão geral

O RISE monta um sistema de arquivos plano (apenas o diretório raiz) usando a API de alto nível do libfuse 3. Ele guarda os dados do usuário em blocos lógicos de 4 MiB, comprimidos com LZ4, dentro de arquivos esparsos de um diretório do sistema hospedeiro (`/var/tmp/rise_root`). A escrita é bufferizada e enviada em lotes; a leitura passa por um cache de blocos. O I/O com o hospedeiro é feito com io_uring em modo SQPOLL, e a compressão e a descompressão são feitas por 8 workers.

### Operações FUSE

| Operação          | Comportamento                                                                                                                                                                                                                            |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `create`          | Cria a entrada de metadados em RAM e, em seguida, abre o arquivo no backend com `O_CREAT \| O_TRUNC`.                                                                                                                                    |
| `open`            | Abre o arquivo no backend. `O_WRONLY` vira `O_RDWR` (pois frequentemente é necessário fazer RMW). `O_TRUNC` zera o arquivo antes. Cria o contexto do descritor: dois buffers de escrita, duas filas de fragmentos e uma thread de flush. |
| `write`           | Copia os dados para um dos dois buffers e registra um fragmento. Responde à aplicação assim que o fragmento entra no buffer (write-back).                                                                                                |
| `read`            | Região sem dados (buraco) é devolvida como zeros. Os demais blocos passam pelo cache; em miss, o bloco é lido via io_uring e descomprimido por um worker.                                                                                |
| `fsync`           | Espera as escritas em andamento, força o que restou nos buffers, chama `fsync` ou `fdatasync` no backend e devolve o erro, se houver.                                                                                                    |
| `release` (close) | Força o flush dos dois buffers, espera as escritas pendentes, limpa as entradas de cache deste descritor, fecha o descritor do backend e libera os buffers. Não chama `fsync`.                                                           |
| `truncate`        | Reduz: remove os blocos além do novo tamanho e libera o espaço com `PUNCH_HOLE`. Aumenta: só ajusta o tamanho lógico (os buracos leem como zeros).                                                                                       |
| `unlink`          | Remove os metadados e o arquivo do backend.                                                                                                                                                                                              |
| `getattr`         | Tamanho lógico como `st_size`; `st_blocks` pelo tamanho comprimido.                                                                                                                                                                      |
| `readdir`         | Lista os arquivos da raiz, mais `.` e `..`.                                                                                                                                                                                              |
| Demais            | `mkdir`, `rmdir`, `rename`, `chmod`, `chown`, `utimens`, `statfs`, `symlink`, `link`, atributos estendidos e similares não estão implementados: o libfuse responde com erro (ENOSYS).                                                    |

### Armazenamento

- Cada arquivo do ponto de montagem é um arquivo esparso dentro de `/var/tmp/rise_root`.
- Os dados são divididos em blocos lógicos de 4 MiB, comprimidos com LZ4 em modo FAST. Um bloco novo guarda os bytes até o fim dos dados conhecidos no momento do flush do buffer de escrita.
- Cada arquivo tem um ponteiro de alocação crescente, alinhado a 4 KiB. Ao reescrever um bloco (RMW), a nova versão é gravada no fim e a antiga é liberada com `fallocate(PUNCH_HOLE)`. Os offsets físicos nunca são reaproveitados, mas o espaço em disco é liberado.
- Os metadados (tamanho, blocos e offsets) ficam apenas em RAM. Ao desmontar, eles são perdidos.

### Caminho de escrita

1. `write` copia os dados para um dos dois buffers do arquivo (double buffering) e registra um fragmento: offset lógico, tamanho, posição no buffer e ordem de chegada.
2. O flush acontece quando o buffer acumula 8 MiB ou 2028 fragmentos, por inatividade (600 mil iterações de espera), ou em `fsync` ou `close`. Antes de drenar um buffer, o sistema troca o buffer ativo e espera os escritores que ainda estavam nele.
3. O flush ordena os fragmentos por offset; dentro de cada faixa contígua, aplica os dados em ordem de chegada, e forma blocos de até 4 MiB. Sem timeout, só blocos cheios são enviados; o restante volta para o outro buffer.
4. O `write_worker` decide, para cada bloco, entre bloco novo (zera o bloco e grava os dados) e RMW (bloco existente sobrescrito parcialmente: lê, descomprime, aplica a mudança e recomprime).
5. Os 8 workers comprimem ou descomprimem com LZ4. O scheduler envia WRITE e READ pelo io_uring e trata as respostas: atualiza os metadados, libera o espaço antigo e devolve os buffers.

### Caminho de leitura

1. A leitura é limitada ao tamanho lógico do arquivo e percorre os blocos de 4 MiB envolvidos.
2. Bloco sem dados (buraco) é devolvido como zeros.
3. O cache tem 64 slots de 4 MiB (256 MiB), chaveados por (descritor, bloco). Se o bloco estiver no cache (hit), os dados são copiados.
4. Em miss, um slot é escolhido; cada slot tem uma vida útil de até 5 acessos. O bloco comprimido é lido pelo io_uring, um worker o descomprime, o slot passa para o estado `CACHED` e a cópia é feita.
5. A leitura é síncrona para a aplicação.

### Durabilidade

- A escrita é respondida antes de chegar ao backend. Dados que ainda estão nos buffers são perdidos se o processo terminar sem desmontar o sistema.
- Os dados só são garantidos no backend depois de um `fsync` bem-sucedido. O `close` força o envio dos buffers, mas não chama `fsync`.
- Não há journal nem recuperação após falha.

### Concorrência e threads

| Thread         | Quantidade                                      | Função                                                                |
| -------------- | ----------------------------------------------- | --------------------------------------------------------------------- |
| libfuse        | padrão do libfuse (multithread)                 | Executa os callbacks (`read`, `write`, `open` etc).                   |
| flush          | uma por arquivo aberto (inclusive para leitura) | Decide quando trocar e drenar os buffers do arquivo.                  |
| `write_worker` | 1                                               | Converte os lotes em jobs de escrita e decide entre bloco novo e RMW. |
| scheduler      | 1                                               | Alimenta o io_uring (SQPOLL) e trata as respostas (CQEs).             |
| workers        | 8                                               | Compressão e descompressão LZ4. Afinidade de CPU em módulo.           |

- Filas limitadas do tipo MPMC (algoritmo de Dmitry Vyukov) e pools de buffers e requisições.
- Sincronização por spinlocks e variáveis atômicas; `std::atomic::wait` nas filas para acordar threads ociosas.
- **Espera ociosa:** `Wait::sleep` gira (spin por 10 mil iterações, depois `_mm_pause` até 60 mil, depois `yield`) antes de bloquear em `atomic::wait`. Por isso, threads ociosas consomem CPU por um tempo para priorizar alto desempenho.

### Opções de montagem

- `direct_io` ativado no mount: o kernel não usa o page cache para este sistema de arquivos.
- `FUSE_CAP_ASYNC_DIO` e `FUSE_CAP_PARALLEL_DIROPS` são solicitados quando disponíveis.
- Timeouts de atributos e de entradas: 2 s.

---

## Parâmetros e limites

| Parâmetro                                      | Valor                                                                               | Onde está no código                         |
| ---------------------------------------------- | ----------------------------------------------------------------------------------- | ------------------------------------------- |
| Tamanho de bloco lógico                        | 4 MiB                                                                               | `BufferQueue` (tamanho dos buffers do pool) |
| Buffers de escrita por arquivo                 | 2, alocados com 16 MiB cada; limite de escrita de 12 MiB por buffer                 | `filesystem.cpp` (`open`, `fuse_write`)     |
| Gatilho de flush por volume                    | 8 MiB                                                                               | `flush_counter`                             |
| Gatilho de flush por fragmentos                | 2028                                                                                | `flush_counter`                             |
| Gatilho de flush por inatividade               | 600 000 iterações de espera                                                         | `flush_counter`                             |
| Fila de fragmentos (por buffer)                | 2048 entradas                                                                       | `fragmentqueue.cpp`                         |
| Pool de buffers                                | 1023 buffers de (4 MiB + limite LZ4 + 4 KiB), alinhados a 4 KiB                     | `bufferqueue.cpp`                           |
| Pool de requisições                            | 2048                                                                                | `requestqueue.cpp`                          |
| Filas de I/O e de jobs                         | 32 e 32                                                                             | `ioqueue.cpp`, `jobqueue.cpp`               |
| Filas de escrita (`write_ring` e de repetição) | 32 e 32 (repetição até 31 entradas)                                                 | `filesystem.cpp`                            |
| io_uring                                       | 64 entradas; SQPOLL com idle de 100 ms; afinidade na CPU 10 (`IORING_SETUP_SQ_AFF`) | `io_engine.cpp`                             |
| Workers                                        | 8, com afinidade de CPU em módulo                                                   | `threadpool.cpp`                            |
| Cache de leitura                               | 64 slots de 4 MiB (256 MiB); vida útil de até 5 acessos                             | `filesystem.cpp`                            |
| Backpressure de escrita e leitura              | espera se houver 128 buffers livres ou menos, ou 100 escritores ativos              | `fuse_write`, `fuse_read`                   |
| Envio de escritas pelo `write_worker`          | só com mais de 5 buffers livres                                                     | `filesystem.cpp`                            |
| Compressão                                     | LZ4, modo FAST (`acceleration = 1`)                                                 | `compressor.cpp`                            |
| Caminho do backend                             | padrão `/var/tmp/rise_root`                                                         | `filesystem.cpp` (construtor)               |

---

## Arquitetura

| Módulo                    | Arquivos                                                                             | Responsabilidade                                                             |
| ------------------------- | ------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------- |
| Sistema de arquivos       | `src/fuse/filesystem.cpp`, `include/rise/fuse/filesystem.hpp`                        | Callbacks FUSE, buffers de escrita, flush, cache de leitura, `write_worker`. |
| Scheduler                 | `src/scheduler/`                                                                     | Thread única que alimenta o io_uring e trata as respostas.                   |
| I/O                       | `src/io/io_engine.cpp`, `include/rise/io/io_engine.hpp`                              | Anel io_uring: 64 entradas, SQPOLL, afinidade de CPU.                        |
| Workers e pool de threads | `src/worker/`, `src/threadpool/`                                                     | Compressão, descompressão e RMW.                                             |
| Compressão                | `src/compression/`, `include/rise/compression/`                                      | LZ4 em modo FAST.                                                            |
| Filas e pools             | `src/queue/`, `include/rise/queue/`, `include/rise/buffer/`, `include/rise/request/` | Filas, pool de buffers e pool de requisições.                                |
| Anel MPMC                 | `include/rise/ringbuffer/ringbuffer.hpp`                                             | Fila limitada (algoritmo de Vyukov) usada por todas as filas.                |
| Metadados                 | `src/metadata/`, `include/rise/metadata/`                                            | Mapa caminho → arquivo, em RAM.                                              |
| Espera                    | `src/wait/`, `include/rise/wait/`                                                    | Backoff: spin, `_mm_pause`, `yield`.                                         |
| Inicialização             | `src/main.cpp`                                                                       | Registro dos callbacks FUSE, criação das threads e desligamento.             |

---

## Não implementado

Além das restrições de uso, não estão implementados:

- Consistência após falha (journal, ordenação de escritas no disco) e durabilidade sem `fsync`.
- Persistência e recuperação de metadados: ao desmontar, os metadados são perdidos.
- Diretórios e namespace hierárquico (o sistema é plano).
- Permissões, donos, ACLs, atributos estendidos, links simbólicos e físicos, `rename` e `statfs`.
- Propagação de erros de escrita e de `close` à aplicação. Esses erros só aparecem nos logs do modo debug ou derrubam o processo.
- Reuso de espaço físico: o ponteiro de alocação só cresce (o espaço é liberado com `PUNCH_HOLE`).
- Limite de tamanho de arquivo, quotas e controle de espaço no backend.
- Coerência de leituras antes do `fsync`: dados que ainda não chegaram aos blocos podem ser lidos como zeros.

---

## Requisitos, compilação e execução

**Requisitos** (Ubuntu 24.04 ou equivalente):

- Linux com io_uring (testado usando kernel 7.0.0-28-generic).
- g++ com suporte a C++20 (testado usando o g++ 13.3.0).
- `pkg-config`, `libfuse3` (3.14.0), `liburing` (2.15) e `liblz4` (1.10.0).
- Permissão para montar sistemas de arquivos FUSE no seu usuário (depende da configuração do sistema).
- Arquitetura x86-64
- Baseline ext4

```bash
sudo apt install build-essential pkg-config fuse3 libfuse3-dev liburing-dev liblz4-dev
```

**Compilar:**

```bash
./build.sh release
```
ou 
```bash
./build.sh debug
```

**Executar:**

```bash
mkdir -p /var/tmp/rise_root
mkdir -p rise_mount # ou o diretorio que quiser usar
./build/RISE ./rise_mount -d -o default_permissions
```

Algumas flags para o comando de execução:

- `-f` (Foreground): Obriga o sistema de arquivos a rodar no terminal atual, ele não vai para o background, se o programa crashar, o erro aparece direto na tela.

- `-d` (Debug): Faz o FUSE printar cada comunicação entre o Kernel e o programa. Ele mostra o Opcode, os argumentos, e o código de retorno de cada função. (usar `-d` já embute o `-f`).

- `-s` (Single-threaded): O FUSE é multi-threaded por padrão. Essa flag desliga as threads do FUSE e processa um comando por vez.

- `-o default_permissions`: Para ativar o `default_permissions` (deixa a parte de checagem de acesso para o Kernel).

**Encerrar:**

```bash
fusermount3 -u -z ./rise_mount
```

Caso ocorra um travamento irreversível, use esse comando antes de desmontar:

```
killall -9 RISE
```

---
## Reprodução dos experimentos

**Ambiente do artigo:** Ubuntu 24.04.4 LTS; kernel 7.0.0-28-generic; sistema de arquivos ext4; processador AMD Ryzen 7 3800XT (8 núcleos, 16 threads); 32 GB de RAM (3000 MHz); HDD de 1 TB (Seagate BarraCuda); g++ 13.3.0; FUSE 3.14.0; LZ4 1.10.0; liburing 2.15.

**Cargas de trabalho**:

| Teste               | Padrão     | Concorrência     | Bloco   |
| ------------------- | ---------- | ---------------- | ------- |
| `seq-st-128k`       | sequencial | 1 job (st)       | 128 KiB |
| `seq-mt-128k`       | sequencial | 10 arquivos (mt) | 128 KiB |
| `ran-st-4k`         | aleatório  | 1 job (st)       | 4 KiB   |
| `ran-mt-4k`         | aleatório  | 10 arquivos (mt) | 4 KiB   |
| `seq-st-1m`         | sequencial | 1 job (st)       | 1 MiB   |
| `seq-st-4k`         | sequencial | 1 job (st)       | 4 KiB   |
| `seq-st-128k-dr-al` | sequencial | 1 job (st)       | 128 KiB |

**Parâmetros do FIO**: engine `sync`; `end_fsync=1`; `group_reporting`; `blockalign` no teste de 4 KiB; `direct=0` no baseline ext4; volumes de 50 GiB (sequencial) e de 2 a 10 GiB (aleatório); limite de 1 hora por execução; média de 10 execuções independentes; caches limpos antes de cada teste; montagem com `noatime` para diminuir overhead de metadados.

---
## Resultados do artigo (resumo)

- **Escrita sequencial multithread, 128 KiB:** 77,2 MiB/s contra 52,1 MiB/s do ext4 (+48,18%).
- **Escrita aleatória de 4 KiB:** queda de throughput, com 5,9 MiB/s contra 15,7 MiB/s do ext4 no cenário multithread.
- **Leitura sequencial multithread, 128 KiB:** 136,4 MiB/s contra 109 MiB/s do ext4 (+25,14%).
- **Dados reais (Silesia):** o ganho depende da compressibilidade dos dados.

Consulte o artigo para as tabelas completas, a análise de ablação e a metodologia.
