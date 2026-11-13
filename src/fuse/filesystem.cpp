#define FUSE_USE_VERSION 30

#include <fuse.h>
#include <algorithm>
#include <rise/fuse/filesystem.hpp>
#include <rise/request/request.hpp>
#include <rise/queue/jobqueue.hpp>
#include <rise/queue/ioqueue.hpp>
#include <rise/queue/bufferqueue.hpp>
#include <rise/queue/requestqueue.hpp>
#include <rise/queue/fragmentqueue.hpp>
#include <rise/compression/compressor.hpp>
#include <rise/io/io_engine.hpp>
#include <rise/scheduler/scheduler.hpp>
#include <errno.h>
#include <stdexcept>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <iostream>
#include <thread>
#include <immintrin.h>
#include <shared_mutex>
#include <mutex>

#ifdef ENABLE_DEBUG
    #define LOG_DEBUG(msg) std::cerr << "[DEBUG] " << msg << "\n"
    #define LOG_ERROR(msg) std::cerr << "[ERROR] " << msg << "\n"
#else
    #define LOG_DEBUG(msg) ((void)0)
    #define LOG_ERROR(msg) ((void)0)
#endif

FileSystem::FileSystem(Scheduler *scheduler, ThreadPool *threadPool, Metadata *metadata, IOEngine *ioEngine, JobQueue *jobQueue, IOQueue *ioQueue, BufferQueue *bufferQueue, RequestQueue *requestQueue, Compressor *compressor):
scheduler(scheduler), threadPool(threadPool), metadata(metadata), ioEngine(ioEngine), jobQueue(jobQueue), ioQueue(ioQueue), compressor(compressor), write_ring(32), write_ring_rt(32), bufferQueue(bufferQueue), requestQueue(requestQueue){
    root_path = "/var/tmp/rise_root";

    // Inicia a thread de background responsável por enviar requisições para send_write
    wt = std::thread(&FileSystem::write_worker, this);
    
    // Cache de leitura
    readCache = new ReadCache[64];

    // Pega o ponteiro para acordar o worker ao finalizar escritas
    scheduler->set_wake(&wakeup_worker);

    LOG_DEBUG("FileSystem: Inicializado com sucesso. Root path mapeado para: " << root_path);
}

int FileSystem::fuse_create(const char *path, mode_t mode, struct fuse_file_info *fi){
    
    // Cria o arquivo nos metadados
    File *file = nullptr;
    {
        std::unique_lock<std::shared_mutex> map_lock(*metadata->get_mutex());
        file = metadata->get_file(path); // get_file cria em caso de não existir
    }
    if (!file) {
        LOG_ERROR("create: get_file retornou nullptr para o path: " << path);
        return -EIO;
    }

    // Modifica os metadados
    while(file->lock.test_and_set(std::memory_order_acquire)) _mm_pause();
    file->mode = mode | S_IFREG;
    file->block_size = bufferQueue->get_size();
    file->lock.clear(std::memory_order_release);

    // Garante que o arquivo fisico seja criado e truncado
    fi->flags |= O_CREAT | O_TRUNC;

    LOG_DEBUG("create: Arquivo alocado nos metadados: " << path << ". Repassando para open...");

    // Chama a função para abrir (e criar) o arquivo antes de retornar
    return fuse_open(path, fi);
}

int FileSystem::fuse_open(const char *path, fuse_file_info *fi){

    // Coleta metadados do arquivo
    File *file = get_valid_file(path, fi);
    if (!file) {
        LOG_DEBUG("open: Rejeitado. Arquivo inexistente ou marcado como deletado: " << path);
        return -ENOENT;
    }
    
    // Incrementa contador de processos com arquivo aberto
    file->ative_count.fetch_add(1, std::memory_order_relaxed);

    // Configurações de abertura do arquivo
    std::string host_path = root_path + path;
    fi->direct_io = 1;
    int host_flags = fi->flags | O_DIRECT;

    if ((host_flags & O_ACCMODE) == O_WRONLY) {
        host_flags = (host_flags & ~O_ACCMODE) | O_RDWR;
    }

    // Trunca o arquivo caso necessário
    if(host_flags & O_TRUNC){
        fuse_truncate(path, 0, fi);
        while (file->lock.test_and_set(std::memory_order_acquire)) _mm_pause();
        file->chunk_index.clear();
        file->logical_size = 0;
        file->compressed_size.store(0,std::memory_order_relaxed);
        file->physical_tail_offset.store(0, std::memory_order_relaxed);
        file->lock.clear(std::memory_order_release);
    }

    // Abre o arquivo com permissões padrão ou com permissões do create
    int fd = open(host_path.c_str(), host_flags, file->mode ? file->mode : 0664);
    if(fd < 0){
        LOG_ERROR("open: Falha ao abrir no host OS! Path FUSE: " << path 
                  << " | Path Real: " << host_path 
                  << " | Motivo: " << strerror(errno) << " (errno: " << errno << ")");
                  
        file->ative_count.fetch_sub(1, std::memory_order_relaxed);
        return -errno;
    }

    LOG_DEBUG("open: Arquivo FD " << fd << " aberto com escrita");

    // Tenta criar o contexto do arquivo
    try {
        OpenContext *arq = new OpenContext{
            fd, 
            file, 
            {new uint8_t[file->block_size * 4], new uint8_t[file->block_size * 4]},
            {0,0}, 
            0, 0,
            {new FragmentQueue, new FragmentQueue},
            {0,0}
        };
        
        arq->ft = std::thread(&FileSystem::flush_counter, this, arq);
        LOG_DEBUG("open: Thread de flush iniciada");

        fi->fh = reinterpret_cast<uint64_t>(arq);
        
    } catch (const std::exception& e) {
        LOG_ERROR("open: EXCECAO CRITICA durante alocacao de contexto: " << e.what());
        
        close(fd);
        file->ative_count.fetch_sub(1, std::memory_order_relaxed);
        return -ENOMEM;
        
    } catch (...) {
        LOG_ERROR("open: EXCECAO DESCONHECIDA durante alocacao de contexto!");
        close(fd);
        file->ative_count.fetch_sub(1, std::memory_order_relaxed);
        return -EIO;
    }

    return 0;
}

int FileSystem::fuse_close(const char *path, fuse_file_info *fi){

    // Coleta o contexto de execussão do arquiov
    OpenContext* ctx = reinterpret_cast<OpenContext*>(fi->fh);
    File *file = ctx->file_meta;

    //std::cout << "cache hit: " << hit_cache.load(std::memory_order_relaxed) << std::endl;

    LOG_DEBUG("close: Solicitado close para fd " << ctx->fd);

    // Desativa thread de flush
    ctx->shutdown_flush.store(true, std::memory_order_relaxed);
    ctx->wakeup_flush.store(true, std::memory_order_relaxed);
    ctx->active_fsync.store(true, std::memory_order_release); // Ativa a flag do fsync apenas para não precisar esperar inatividade
    ctx->wakeup_flush.notify_one();
    
    if(ctx->ft.joinable()) ctx->ft.join();

    // Força o flush dos fragmentos de dados para escrita
    force_flush_buffer(ctx, 0, true);
    force_flush_buffer(ctx, 1, true);

    LOG_DEBUG("close: flush concluido e gravado");

    // Espera todas escritas serem feitas
    uint32_t idle = 0;
    while(ctx->pending_writes.load(std::memory_order_acquire)) {
        wait.sleep(idle++);
    }
    LOG_DEBUG("close Todas escritas terminaram");

    // Remove todos os caches do arquivo
    for(int x=0; x < 64; x++){
        if(readCache[x].fd == ctx->fd){
            idle = 0;
            while(true) {
                CacheState stt = readCache[x].state.load(std::memory_order_relaxed);

                // Caso já esteja vazio ou seja cache de outro arquivo
                if(stt == EMPTY || readCache[x].fd != ctx->fd) break;

                // Espera os processor lerem o buffer
                if(readCache[x].reading.load(std::memory_order_relaxed) > 0 || stt != CACHED) {
                    wait.sleep(idle++);
                    continue;
                }
                
                // Caso não tenha leitores, tenta mudar o estado do buffer para OFF e esvazia o buffer
                CacheState expected = CACHED;
                if(readCache[x].state.compare_exchange_strong(expected, OFF, std::memory_order_acquire)){
                    LOG_DEBUG("buffer: Devolvendo buffer " << readCache[x].buf->id);
                    while(!bufferQueue->push(readCache[x].buf));
                    readCache[x].buf = 0;
                    readCache[x].state.store(EMPTY, std::memory_order_release);
                    readCache[x].state.notify_all();
                    break;
                }
            }
        }
    }

    // Fecha o arquivo
    if(close(ctx->fd) < 0){
        LOG_ERROR("close: Syscall close() falhou no FD " << ctx->fd 
                  << " | Motivo: " << strerror(errno));
    } //###else LOG_DEBUG("close: Arquivo de FD " << ctx->fd << " fechado!");

    // Limpeza de memória
    delete [] ctx->buffer[0];
    delete [] ctx->buffer[1];
    delete ctx->fragQueue[0];
    delete ctx->fragQueue[1];
    delete ctx;
    LOG_DEBUG("Memoria limpa com sucesso");
    
    return 0;
}

int FileSystem::fuse_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi){

    // Coleta contexto de abertura do arquivo
    OpenContext *ctx = reinterpret_cast<OpenContext*>(fi->fh);
    bool expected = false;

    // Esperando caso chegue no limite de buffer (deixa margem de segurança)
    if(bufferQueue->get_available() <= 128 || ctx->active_writers[0] + ctx->active_writers[1] >= 100){
        LOG_DEBUG("buffer: Os buffers esgotaram os fragmentos estão congestionados! active_writers é " << ctx->active_writers[0] + ctx->active_writers[1]);
        uint32_t idle = 0;
        while(bufferQueue->get_available() <= 128 || ctx->active_writers[0] + ctx->active_writers[1] >= 100) wait.sleep(idle++);
    }

    // Avisa que vai escrever
    ctx->active_writers[0].fetch_add(1);
    ctx->active_writers[1].fetch_add(1);
    bool buffer_num = ctx->active_buffer.load();
    ctx->active_writers[!buffer_num].fetch_sub(1);

    uint32_t block_size = ctx->file_meta->block_size;
    uint32_t idle = 0;

    // Pega o Bilhete atômico
    uint64_t my_ticket = ctx->request_counter.fetch_add(1, std::memory_order_relaxed);

    // Reserva espaço no buffer
    uint64_t my_buf_offset;
    while(true){
        my_buf_offset = ctx->buffer_head[buffer_num].load(std::memory_order_relaxed);
        // O tamanho máximo de escrita é até o offset (block_size * 3)
        if(my_buf_offset + size > block_size * 3){
            // Avisa que não vai mais escrever
            ctx->active_writers[buffer_num].fetch_sub(1, std::memory_order_release);
            
            //###if(idle == 0) LOG_DEBUG("write_fuse: Buffer " << buffer_num << " cheio. Aguardando...");

            while(ctx->active_buffer.load(std::memory_order_acquire) == buffer_num){
                wait.sleep(idle++);
            }

            // O buffer virou
            ctx->active_writers[0].fetch_add(1);
            ctx->active_writers[1].fetch_add(1);
            buffer_num = ctx->active_buffer.load();
            ctx->active_writers[!buffer_num].fetch_sub(1);
            idle = 0;
            continue;
        }
        // Tenta alocar um espaço para escrever no buffer
        if(ctx->buffer_head[buffer_num].compare_exchange_weak(my_buf_offset, my_buf_offset + size, std::memory_order_relaxed)){
            break;
        }
    }

    // Guarda os dados no buffer
    std::memcpy(ctx->buffer[buffer_num] + my_buf_offset, buf, size);

    // Registra no ring de fragmentos
    idle = 0;
    while(!ctx->fragQueue[buffer_num]->push({
        (uint64_t)offset, 
        (uint32_t)size, 
        my_buf_offset, 
        my_ticket
    })) wait.sleep(idle++);

    // Avisa que terminou de copiar
    ctx->active_writers[buffer_num].fetch_sub(1, std::memory_order_release);

    // Acorda thread de flush se nescessário
    if(!ctx->wakeup_flush.load(std::memory_order_acquire)){
        LOG_DEBUG("fuse_write: acordando thread de flush");
        ctx->wakeup_flush.store(true, std::memory_order_release);
        ctx->wakeup_flush.notify_one();
    }

    // Aumenta o tamanho logico total do arquivo se for necessário
    uint64_t current_eof = ctx->file_meta->logical_size.load(std::memory_order_relaxed);
    uint64_t new_eof = offset + size;
    
    while(current_eof < new_eof){
        // Se conseguir substituir, ótimo, se falhar, o current_eof é atualizado com o valor novo
        if(ctx->file_meta->logical_size.compare_exchange_weak(current_eof, new_eof, std::memory_order_release, std::memory_order_relaxed)){
            LOG_DEBUG("write: EOF expandido no aruqivo " << path << ": " << current_eof << " -> " << new_eof << " bytes.");
            break;
        }
        _mm_pause();
    }

    return size;
}

void FileSystem::flush_counter(OpenContext *ctx) {
    LOG_DEBUG("flush_counter: Ativado");

    uint64_t idle = 0;
    uint32_t active_writers;
    uint32_t block_size = ctx->file_meta->block_size;
    uint64_t prev_ticket = ctx->request_counter.load(std::memory_order_relaxed);
    bool buffer_num;

    // Loop para verificar cada condição de flush
    while(true){
        buffer_num = ctx->active_buffer.load(std::memory_order_acquire);
        active_writers = ctx->active_writers[buffer_num].load(std::memory_order_relaxed);

        // Se o buffer ja bateu a cota de tamanho (block_size * 2 ou apenas 20 fragmentos livres restantes), força o flush
        if(ctx->fragQueue[buffer_num]->total_size() >= block_size * 2 || 
           ctx->fragQueue[buffer_num]->frag_count() >= (2048 - 20)) {

            LOG_DEBUG("flush_counter: Flush engatilhado (limite de buffer/fragmentos)");
            
            // Espera se o outro buffer ainda estiver no processo de flush
            uint32_t idle_head = 0;
            while(ctx->buffer_head[!buffer_num].load(std::memory_order_relaxed) > block_size*2) wait.sleep(idle_head++);

            // Troca os buffers
            bool old = ctx->active_buffer.load();
            ctx->active_buffer.store(!old);

            // Espera se ainda tiver algum processo escrevendo no buffer e força o flush
            uint32_t drain_idle = 0;
            while(ctx->active_writers[old].load() > 0) wait.sleep(drain_idle++);
            force_flush_buffer(ctx, old, false);

            continue;
        }

        // Se já passou tempo demais sem ninguém escrever ou algum processo deu fsync
        if((ctx->active_fsync.load(std::memory_order_acquire) || idle > 600000) && !active_writers){
            
            LOG_DEBUG("flush_counter: Flush engatilhado (timeout de inatividade)!");

            // Espera se o outro buffer ainda estiver no processo de flush
            uint32_t idle_head = 0;
            while(ctx->buffer_head[!buffer_num].load(std::memory_order_relaxed) > block_size*2) wait.sleep(idle_head++);
            
            // Se algum processo tiver começado a escrever, espera
            idle = 0;
            while(ctx->active_writers[buffer_num].load(std::memory_order_acquire) > 0) wait.sleep(idle++);

            // Troca os buffers e força o flush com a flag de inatividade
            LOG_DEBUG("flush_counter: Buffer atual agora é " << !buffer_num);
            bool old = ctx->active_buffer.load();
            ctx->active_buffer.store(!old);
            while(ctx->active_writers[old].load() > 0) wait.sleep(idle++);
            force_flush_buffer(ctx, old, true);

            if(ctx->fragQueue[!buffer_num]->frag_count() == 0){
                LOG_DEBUG("flush_counter: Domindo");
                ctx->wakeup_flush.wait(false, std::memory_order_acquire);
                ctx->wakeup_flush.store(false, std::memory_order_relaxed);
                LOG_DEBUG("flush_counter: Acordando");
                if(ctx->shutdown_flush.load(std::memory_order_relaxed)) break;
            }
        }

        // Verifica se algum processo fez um write e reseta o contador de ociosidade
        uint64_t current_ticket = ctx->request_counter.load(std::memory_order_relaxed);
        if (current_ticket == prev_ticket) {
            wait.sleep(idle++);
        } else {
            idle = 0;
            prev_ticket = current_ticket;
        }
    }

    LOG_DEBUG("flush_counter: Desativado");
}

void FileSystem::force_flush_buffer(OpenContext* ctx, bool buffer_num, bool is_timeout){
    uint32_t block_size = ctx->file_meta->block_size;
    uint32_t current_frags = ctx->fragQueue[buffer_num]->frag_count();
    uint64_t remaining_size = ctx->fragQueue[buffer_num]->total_size();
    
    if(current_frags == 0){
        LOG_DEBUG("force_flush: Não há fragmentos para flush, retornando...");
        return;
    }

    LOG_DEBUG("force_flush: Iniciando flush (Buffer " << buffer_num << ") | Frags: " << current_frags << " | Timeout: " << (is_timeout ? "SIM" : "NAO"));

    // Coleta os fragmentos
    std::vector<Fragment> batch;
    Fragment temp_frag;
    batch.reserve(current_frags);
    ctx->pending_writes.fetch_add(1);
    while(ctx->fragQueue[buffer_num]->pop(temp_frag)){
        batch.push_back(temp_frag);
    }

    // Ordena por sequencia e offset (Ordem de chegada, do menor offset ao maior)
    std::sort(batch.begin(), batch.end(), [](const Fragment& a, const Fragment& b) {
        if (a.logical_offset == b.logical_offset) return a.sequence_id < b.sequence_id;
        return a.logical_offset < b.logical_offset;
    });

    uint32_t processed_idx = 0; // Índice to fragmento atual sendo processado
    uint32_t batch_size = batch.size(); // Quantidade de bytes para serem processados

    // Loop para aplicar os fragmentos no buffer e enviar para ring do write_worker
    while(processed_idx < batch_size){

        // Se não for timeout e o espaço restante for menor que o tamanho de um bloco, sai (se for timeout, faz o flush dos fragmentos até o fim)
        if(!is_timeout && remaining_size < block_size) break;

        uint64_t start_offset = batch[processed_idx].logical_offset;
        uint64_t end_offset   = start_offset;

        uint32_t scan_idx = processed_idx;

        // Coleta um buffer para aplicar os fragmentos
        uint32_t pop_idle = 0;
        Buffer* buffer_tmp;
        while(!bufferQueue->pop(buffer_tmp)){
            wait.sleep(pop_idle++);
        }
        LOG_DEBUG("buffer: Peguei o buffer " << buffer_tmp->id << " para requisição de offset " << start_offset);

        // Adiciona 1 na flag para indicar que tem uma escrita pendente
        int a = ctx->pending_writes.fetch_add(1, std::memory_order_relaxed);
        LOG_DEBUG("force_flush: pending_write incrementado como placeholder, peding write agora é " << a+1 << " para requisição de offset " << start_offset);

        // Escanea blocos contiguos (offset + size do primeiro fragmento é extamente o incio do offset do proximo)
        uint32_t chunk_end_idx = processed_idx;
        while(chunk_end_idx < batch_size){
            Fragment& frag = batch[chunk_end_idx];
            uint64_t frag_start = frag.logical_offset;
            uint64_t frag_end   = frag.logical_offset + frag.size;

            if(frag_start > end_offset) break;
            if(frag_end - start_offset > block_size) break;

            end_offset = std::max(end_offset, frag_end);
            chunk_end_idx++;
        }

        // Ordena pelo tempo cronologico (tickets)
        std::sort(batch.begin() + processed_idx, batch.begin() + chunk_end_idx, 
            [](const Fragment& a, const Fragment& b) {
                return a.sequence_id < b.sequence_id;
            });

        // Aplica as copias respeitando a ordem do tempo (o mais novo sobrescreve o velho)
        for(uint32_t i = processed_idx; i < chunk_end_idx; i++){
            Fragment& frag = batch[i];
            memcpy(
                buffer_tmp->data + (frag.logical_offset - start_offset),
                ctx->buffer[buffer_num] + frag.buffer_offset,
                frag.size
            );
            remaining_size -= frag.size;
        }

        // Atualiza índice do valor sendo analizado
        scan_idx = chunk_end_idx;

        uint32_t current_chunk_size = static_cast<uint32_t>(end_offset - start_offset);

        LOG_DEBUG("force_flush_buffer: Despachando chunk para a thread de I/O | Offset logico: " << start_offset << " | Tamanho: " << current_chunk_size << " bytes");

        // Incrementa a quantidade de write prontos para ser despachados e adiciona no ring do write_worker
        uint32_t idle = 0;
        while(!write_ring.push({
            buffer_tmp,
            current_chunk_size,
            static_cast<int64_t>(start_offset),
            ctx
        })) wait.sleep(idle++);

        // Acorda o write_worker
        wakeup_worker.store(1, std::memory_order_relaxed);
        wakeup_worker.notify_one();

        processed_idx = scan_idx;
    }

    // Caso não seja flush por inatividade e tenha sobrado fragmentos que somam um valor de bytes < block_size
    if(processed_idx < batch_size){
        LOG_DEBUG("force_flush: " << (batch_size - processed_idx) << " fragmentos restantes transferidos para o buffer alternativo (" << !buffer_num << ")");
        
        uint64_t pos = ctx->buffer_head[!buffer_num].fetch_add(remaining_size, std::memory_order_relaxed);
      
        // Envia os dados restantes para o outro buffer e atualiza os offsets do buffer de fragmentos
        for(uint32_t i = processed_idx; i < batch_size; i++){
            memcpy(ctx->buffer[!buffer_num] + pos, ctx->buffer[buffer_num] + batch[i].buffer_offset, batch[i].size);
            batch[i].buffer_offset = pos;
            uint32_t push_idle = 0;
            while(!ctx->fragQueue[!buffer_num]->push(batch[i])) wait.sleep(push_idle++);
            pos += batch[i].size;
        }
    }
    
    // Reseta o índice de escrita do buffer
    ctx->buffer_head[buffer_num].store(0, std::memory_order_relaxed);
    ctx->pending_writes.fetch_sub(1);
    LOG_DEBUG("force_flush: Buffer de número " << buffer_num << " resetado!");
}

void FileSystem::write_worker(){
    WriteJob job;
    size_t rt_count = 0;  // Contador de ocupação da write_ring_rt
    
    auto can_write = [](WriteJob &job) -> bool {
        uint32_t block_size = job.ctx->file_meta->block_size;
        uint64_t start_chunk = job.offset / block_size;
        uint64_t end_chunk = (job.offset + job.size - 1) / block_size;

        bool start_busy = false;
        bool end_busy = false;

        // Lê os valores busy com segurança
        while(job.ctx->file_meta->lock.test_and_set(std::memory_order_acquire)) _mm_pause();
        
        // Só tenta ler a flag busy se o bloco já existir no vector
        if(start_chunk < job.ctx->file_meta->chunk_index.size()){
            start_busy = job.ctx->file_meta->chunk_index[start_chunk].busy;
        }
        if(end_chunk < job.ctx->file_meta->chunk_index.size()){
            end_busy = job.ctx->file_meta->chunk_index[end_chunk].busy;
        }
        
        job.ctx->file_meta->lock.clear(std::memory_order_release);
        return !start_busy && !end_busy;
    };

    while(true){
        bool worked = false;

        if(bufferQueue->get_available() > 5){
            // So pega uma requisição se o ring de retry não estiver cheio
            if(rt_count < 31 && write_ring.pop(job)){
                worked = true;
                // Verifica se os blocos que serão usados estão livres
                if(can_write(job)){
                    send_write(job.buf, job.size, job.offset, job.ctx);
                    int a = job.ctx->pending_writes.fetch_sub(1, std::memory_order_relaxed);
                    LOG_DEBUG("write_worker: pending_write decrementado, placeholder removido, peding write agora é " << a-1 << " para requisição de offset " << job.offset);
                    LOG_DEBUG("write_worker: Requisição de offest " << job.offset << " enviada para o send_write");
                } else {
                    write_ring_rt.push(job);
                    rt_count++;
                    LOG_DEBUG("write_worker: Requisição de offset " << job.offset << " precisa usar blocos que estão ocupados");
                }
            }
            
            // Loop passa por todas escritas pendentes
            for(size_t x = rt_count; x > 0; x--){
                write_ring_rt.pop(job);

                // Verifica se os blocos que serão usados estão livres
                if(can_write(job)){
                    send_write(job.buf, job.size, job.offset, job.ctx);
                    int a = job.ctx->pending_writes.fetch_sub(1, std::memory_order_relaxed);
                    LOG_DEBUG("write_worker: pending_write decrementado, placeholder removido, peding write agora é " << a-1 << " para requisição de offset " << job.offset);
                    LOG_DEBUG("write_worker: Requisição RT de offset " << job.offset << " liberada e enviada");
                    rt_count--;
                } else {
                    write_ring_rt.push(job);
                    LOG_DEBUG("write_worker: Requisição de offset " << job.offset << " ainda não pode ser enviada");
                }
            }

            if(worked) continue;
        } else {
            uint32_t idle = 0;
            while(bufferQueue->get_available() <= 5) wait.sleep(idle++);
            continue;
        }

        // Flag para interromper o worker no desligamento do filesystem
        if (!is_running.load(std::memory_order_relaxed)) {
            break;
        }

        // Suspende o processamento caso não tenha mais 
        wakeup_worker.wait(0, std::memory_order_acquire);
        wakeup_worker.store(0, std::memory_order_relaxed);
    }
}

void FileSystem::send_write(Buffer *buf, size_t size, off_t offset, OpenContext *ctx){
    
    uint32_t block_size = ctx->file_meta->block_size;
    bool rmw = (((offset & (block_size - 1)) != 0) || (size < block_size));

    //###LOG_DEBUG("send_write: Iniciando " << (rmw ? "RMW (Read-Modify-Write)" : "Escrita Direta") 
    //          << " | Offset: " << offset << " | Size: " << size << " | FD: " << ctx->fd);

    // Caso precise de RMW
    if(rmw){
        uint32_t esc = 0;

        uint32_t write_offset_in_block = offset & (block_size - 1);
        uint32_t num_requests = (write_offset_in_block + size + block_size - 1) / block_size;
        buf->refs.store(num_requests, std::memory_order_relaxed);
        LOG_DEBUG("send_write: RMW será necessário, numero de referencias do buffer " << buf->id << " é " << num_requests << " para ser usado no offset " << offset);

        while(esc < size){

            // Calcula offsets
            uint64_t current_offset = offset + esc; // Offset de escrita atual
            uint64_t current_chunk = current_offset / block_size; // Chunk que será usada para escrever 
            uint32_t write_offset_in_block = current_offset & (block_size - 1); // Offset usado para escrita dentro do bloco (resto da divisão )
            uint32_t write_size = std::min((uint32_t)(size - esc), (uint32_t)(block_size - write_offset_in_block)); // Quantidade de byte para escrever (do offset de escrita no bloco até o final do bloco)

            // Verifica se a escrita acontecerá numa chunk nova (caso o RMW tenha acontecido na iteração passada)
            while(ctx->file_meta->lock.test_and_set(std::memory_order_acquire)) _mm_pause();
            bool needs_resize = (current_chunk >= ctx->file_meta->chunk_index.size()); // Verifica se precisamos expandir a capacidade do vetor
            bool is_new_chunk = needs_resize || (ctx->file_meta->chunk_index[current_chunk].logical_size == 0); // É uma chunk "nova" se estiver fora do vetor OU se estiver dentro, mas vazia (Hole)
            uint32_t comp_size = 0;
            uint64_t phys_offset = 0;
            if(!is_new_chunk){
                comp_size = ctx->file_meta->chunk_index[current_chunk].compressed_size;
                phys_offset = ctx->file_meta->chunk_index[current_chunk].physical_offset;
            }
            if(needs_resize){
                // Redimensionamento se estivermos fora dos limites
                ctx->file_meta->chunk_index.resize(current_chunk + 16);
            }
            ctx->file_meta->lock.clear(std::memory_order_release);

            // Prepara a Request
            Request *req;
            uint32_t idle = 0;
            while(!requestQueue->pop(req)) wait.sleep(idle++);
            LOG_DEBUG("send_write: (requisição " << req->id << ") coletada no RMW para offset" << offset << " de bloco " << current_chunk);

            req->cqe_res = 0;
            req->file_chunks_ptr = &ctx->file_meta->chunk_index;
            req->total_compressed_size = &ctx->file_meta->compressed_size;
            req->physical_tail_offset = &ctx->file_meta->physical_tail_offset;
            req->file_lock_ptr = &ctx->file_meta->lock;
            req->fd = ctx->fd;
            req->ctx = ctx;
            req->chunk_index = current_chunk;

            // Se for chunk nova, apenas enche de 0s
            if(is_new_chunk){
                LOG_DEBUG("send_write: Foi necessário uma chunk nova durante RMW para o offset " << offset << ", preparando (requisição " << req->id << ")");
                req->type = WRITE;
                req->is_rmw = false;
                req->old_physical_offset = 0;
                req->old_compressed_size = 0;
                req->physical_offset = 0;
                req->compressed_size = 0;
                
                Buffer *decomp_buf = nullptr;
                idle = 0;
                while(!bufferQueue->pop(decomp_buf)) wait.sleep(idle++);
                LOG_DEBUG("buffer: Peguei o buffer " << decomp_buf->id << " para usar para comprimir 0s para offset " << offset);
                
                std::memset(decomp_buf->data, 0, block_size);
                std::memcpy(decomp_buf->data + write_offset_in_block, buf->data + esc, write_size);

                uint64_t chunk_start = current_chunk * block_size; // Começo do bloco
                uint64_t current_eof = std::max((uint64_t)ctx->file_meta->logical_size, (uint64_t)(offset + size)); // Se o ultimo byte está expandindo o arquivo
                uint32_t valid_bytes = std::min((uint32_t)(current_eof - chunk_start), block_size); // Tamanho da escrita

                req->logical_size = valid_bytes;
                req->logical_offset = current_chunk * block_size;
                req->user_buffer = (void*)decomp_buf->data;
                req->buffer = decomp_buf;
                req->origin_buf = buf;

                int a = ctx->pending_writes.fetch_add(1, std::memory_order_relaxed);
                LOG_DEBUG("send_write: pending_write incrementado para escrita no offset " << req->logical_offset << ", peding write agora é " << a+1 << " e a requisição original tem offset " << offset);

                // Bloqueia o bloco para evitar corrupção
                while(ctx->file_meta->lock.test_and_set(std::memory_order_acquire)) _mm_pause();
                ctx->file_meta->chunk_index[current_chunk].busy = true;
                ctx->file_meta->lock.clear(std::memory_order_release);

                idle = 0;
                while(!jobQueue->push(req)) wait.sleep(idle++);

                LOG_DEBUG("send_write: Requisição " << req->id << " enviada para jobQueue");
            
            // Se for RMW
            } else {
                LOG_DEBUG("send_write: preparando (requisição " << req->id << ") para RMW do offset " << offset);
                req->type = READ;
                req->is_rmw = true;
                
                req->user_buffer = (void*)(buf->data + esc); 
                req->logical_size = write_size; 
                req->logical_offset = write_offset_in_block; 
                
                req->compressed_size = comp_size;
                req->physical_offset = phys_offset;
                req->old_compressed_size = comp_size;
                req->old_physical_offset = phys_offset;
                
                req->origin_buf = buf;
                uint32_t idle_buf = 0;
                while(!bufferQueue->pop(req->buffer)) wait.sleep(idle_buf++);
                LOG_DEBUG("buffer: Peguei o buffer " << req->buffer->id << " para leitura RMW");

                int a = ctx->pending_writes.fetch_add(1, std::memory_order_relaxed);
                LOG_DEBUG("send_write: pending_write incrementado para escrita no offset " << req->logical_offset << ", peding write agora é " << a+1 << " e a (requisição " << req->id << ") original tem offset " << offset);

                // Bloqueia o bloco para evitar corrupção
                while(ctx->file_meta->lock.test_and_set(std::memory_order_acquire)) _mm_pause();
                ctx->file_meta->chunk_index[current_chunk].busy = true;
                ctx->file_meta->lock.clear(std::memory_order_release);

                idle = 0;
                while(!ioQueue->push(req)) wait.sleep(idle++);

                LOG_DEBUG("send_write: Requisição " << req->id << " enviada para ioQueue");
            }

            esc += write_size;
    
        }

    // Caso não seja RMW
    } else {
        uint64_t current_chunk = offset / block_size;
        
        // Preenche dados iniciais da request
        Request *req;
        uint32_t idle = 0;
        while(!requestQueue->pop(req)) wait.sleep(idle++);
        LOG_DEBUG("send_write: (requisição " << req->id << ") coletada para escrita direta no offset " << offset);
        req->type = WRITE;
        req->cqe_res = 0;
        req->file_chunks_ptr = &ctx->file_meta->chunk_index;
        req->file_lock_ptr = &ctx->file_meta->lock;
        req->fd = ctx->fd;
        req->physical_tail_offset = &ctx->file_meta->physical_tail_offset;
        req->total_compressed_size = &ctx->file_meta->compressed_size;
        req->buffer = nullptr; 
        req->ctx = ctx;
        req->chunk_index = offset / block_size;
        req->logical_offset = offset;
        req->user_buffer = (void*)buf->data;
        req->logical_size = block_size;
        req->is_rmw = false;
        req->old_compressed_size = 0;
        req->old_physical_offset = 0;
        req->origin_buf = buf;

        bool needs_resize;
        bool is_new_chunk;

        // Se for um bloco novo, adiciona no vector
        while(ctx->file_meta->lock.test_and_set(std::memory_order_acquire)) _mm_pause();
        needs_resize = (current_chunk >= ctx->file_meta->chunk_index.size());
        is_new_chunk = needs_resize || (ctx->file_meta->chunk_index[current_chunk].logical_size == 0);
        if(needs_resize){
            ctx->file_meta->chunk_index.resize(req->chunk_index + 16);
        }
        if(is_new_chunk){
            req->old_compressed_size = 0;
            req->old_physical_offset = 0;
        } else {
            LOG_DEBUG("send_write: A (requisição " << req->id << ") cobre um offset já usado (" << offset << ") e precisara de fallocate");
            req->old_compressed_size = ctx->file_meta->chunk_index[current_chunk].compressed_size;
            req->old_physical_offset = ctx->file_meta->chunk_index[current_chunk].physical_offset;
        }
        ctx->file_meta->lock.clear(std::memory_order_release);

        buf->refs.store(1, std::memory_order_relaxed);
        LOG_DEBUG("send_write: RMW NÃO será necessário, numero de referencias do buffer " << buf->id << " é 1 para ser usado no offset " << offset);

        // Envia para a fila
        req->physical_offset = 0;
        req->compressed_size = 0;

        int a = ctx->pending_writes.fetch_add(1, std::memory_order_relaxed);
        LOG_DEBUG("send_write: pending_write incrementado para escrita no offset " << req->logical_offset << ", peding write agora é " << a+1 << " e a (requisição " << req->id << ") original tem offset " << offset);

        // Bloqueia o bloco para evitar corrupção
        while(ctx->file_meta->lock.test_and_set(std::memory_order_acquire)) _mm_pause();
        ctx->file_meta->chunk_index[current_chunk].busy = true;
        ctx->file_meta->lock.clear(std::memory_order_release);

        idle = 0;
        while(!jobQueue->push(req)) wait.sleep(idle++);
        LOG_DEBUG("jobQueue: Esperou " << idle << " iterações no send_write !RMW");

    }
}

int FileSystem::fuse_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi){

    // Coleta o contexto de execução do arquivo
    OpenContext *ctx = reinterpret_cast<OpenContext*>(fi->fh);
    uint32_t block_size = ctx->file_meta->block_size;

    // Esperando caso chegue no limite de buffer (deixa margem de segurança)
    if(bufferQueue->get_available() <= 128 || ctx->active_writers[0] + ctx->active_writers[1] >= 100){
        LOG_DEBUG("buffer: Os buffers esgotaram os fragmentos estão congestionados! active_writers é " << ctx->active_writers[0] + ctx->active_writers[1]);
        uint32_t idle = 0;
        while(bufferQueue->get_available() <= 128 || ctx->active_writers[0] + ctx->active_writers[1] >= 100) wait.sleep(idle++);
    }

    // Trata limites de leitura
    uint64_t file_logical_size = ctx->file_meta->logical_size.load(std::memory_order_relaxed);
    if(offset >= file_logical_size) return 0;
    if(offset + size > file_logical_size){
        size = file_logical_size - offset;
        LOG_DEBUG("fuse_read: Leitura limitada a " << size);
    }

    Request *req;
    uint32_t idle;
    uint32_t escrito = 0;

    // Loop para escrever no buffer do usuário
    while(escrito < size){

        uint32_t current_block = (offset + escrito) / block_size;
        int32_t idx = -1;
        int32_t x;
        uint32_t chunk_stored_size = 0;
        uint64_t chunk_phys_offset = 0;
        
        // Verifica se a leitura requisitada fica em um espaço não alocado do arquivo
        while(ctx->file_meta->lock.test_and_set(std::memory_order_acquire)) _mm_pause();
        bool is_hole = (current_block >= ctx->file_meta->chunk_index.size()) || !ctx->file_meta->chunk_index[current_block].logical_size;

        // Se não for um buraco, coleta offset e size do bloco atual
        if(!is_hole){
            chunk_phys_offset = ctx->file_meta->chunk_index[current_block].physical_offset;
            chunk_stored_size = ctx->file_meta->chunk_index[current_block].compressed_size;
        }
        ctx->file_meta->lock.clear(std::memory_order_release);

        // Se for um buraco, apenas devolve um monte de 0s pro usuário
        if(is_hole){
            uint32_t read_offset_in_block = (offset + escrito) & (block_size - 1);
            
            // Quantidade de bytes para preencher (pega primeiro o size entre o offset e o final do primeiro bloco, depois pega o restante)
            uint32_t bytes_to_fill = std::min(
                (uint32_t)(size - escrito),
                (uint32_t)(block_size - read_offset_in_block)
            );
            
            // Escreve no buffer (apenas o pedaço que tem o buraco)
            memset(buf + escrito, 0, bytes_to_fill);
            escrito += bytes_to_fill;
            
            continue;
        }

        // Procura o bloco requisitado no cache
        for(x=0; x < 64; x++){
            CacheState stt = readCache[x].state.load(std::memory_order_relaxed);
            if(stt != EMPTY && readCache[x].fd == ctx->fd && current_block == readCache[x].logic_block_num){
                // Tenta reservar o cache
                idx = get_cache(x);
                break;
            }
        }

        // Não achou ou não conseguiu pegar
        if(idx == -1){
            // Não achou (Cache Miss)
            if(x == 64){
                // Pega indice para gravar os novos dados no cache
                idx = get_new_cache();
                readCache[idx].fd = ctx->fd;
                readCache[idx].logic_block_num = current_block;

                if(readCache[idx].buf != nullptr){
                    uint32_t idle_recycle = 0;
                    while(!bufferQueue->push(readCache[idx].buf)) wait.sleep(idle_recycle++);
                }

                // Reserva um buffer para esse cache
                idle = 0;
                while(!bufferQueue->pop(readCache[idx].buf)) wait.sleep(idle++);
                LOG_DEBUG("buffer: Peguei buffer " << readCache[idx].buf->id);

                LOG_DEBUG("read: Cache Miss no FD " << ctx->fd << " (Bloco Lógico: " << current_block << "). Fazendo fetch no disco real...");

                // Cria requisição
                idle = 0;
                while(!requestQueue->pop(req)) wait.sleep(idle++);

                req->type = READ;
                req->cqe_res = 0;
                req->file_chunks_ptr = &ctx->file_meta->chunk_index;
                req->file_lock_ptr = &ctx->file_meta->lock;
                req->fd = ctx->fd;
                req->ctx = ctx;
                req->logical_offset = 0;
                req->physical_offset = chunk_phys_offset;
                req->old_physical_offset = 0;
                req->logical_size = 0;
                req->compressed_size = chunk_stored_size;
                req->old_compressed_size = 0;
                req->is_rmw = false;
                req->total_compressed_size = &ctx->file_meta->compressed_size;
                req->physical_tail_offset = &ctx->file_meta->physical_tail_offset;

                req->buffer = readCache[idx].buf;
                req->chunk_index = current_block;
                req->user_buffer = (void*)(buf + escrito);
                req->readCache = &readCache[idx];

                // Envia para a fila para leitura, ao retornar o cache vai ser BUSY_
                idle = 0;
                while(!ioQueue->push(req)) wait.sleep(idle++);
                
                req->readCache->state.wait(OFF, std::memory_order_acquire);

                uint32_t aligned_expected = (req->compressed_size + 4095) & ~4095;
                // Caso a leitura falhe
                if(req->cqe_res != static_cast<ssize_t>(aligned_expected)){
                    LOG_ERROR("read: Erro ao ler dados para o page cache | FD: " << ctx->fd 
                              << " | Lidos: " << req->cqe_res << " / Esperados: " << chunk_stored_size
                              << " | Motivo: " << (req->cqe_res < 0 ? strerror(errno) : "EOF Inesperado (Arquivo truncado)"));
                    
                    throw std::runtime_error("Erro durante a leitura para cache");
                }
                LOG_DEBUG("read: Busca para FD " << ctx->fd << " concluida, (Bloco Lógico: " << current_block << "). Iniciando descompactação...");

                // Worker descompacta para o cache, incrementa "reading" e muda para CACHED
                idle = 0;
                while(!jobQueue->push(req)) wait.sleep(idle++);
                LOG_DEBUG("jobQueue: Esperou " << idle << " iterações no fuse_read");

                // Espera os dados serem decompactados
                readCache[idx].state.wait(BUSY_, std::memory_order_acquire);

                // Devolve a requisição para a pool
                while(!requestQueue->push(req));

                // Verifica se o worker conseguiu descompactar o arquivo
                if(readCache[idx].state.load(std::memory_order_relaxed) != CACHED){
                    return -EIO;
                }
                LOG_DEBUG("read: Descompactação para fd " << ctx->fd << " concluida, (Bloco Lógico: " << current_block << ")");

            // Não conseguiu pegar (estado = OFF)
            } else {
                // Espera para ver se vai ou não ter um cache aqui
                readCache[x].state.wait(OFF, std::memory_order_relaxed);

                // Se tiver cache, vê se ainda é oque estava esperando
                CacheState stt = readCache[x].state.load(std::memory_order_relaxed);
                if(stt != EMPTY && readCache[x].fd == ctx->fd && current_block == readCache[x].logic_block_num){
                    // Se for, espera descompactar
                    readCache[x].state.wait(BUSY_, std::memory_order_relaxed);
                } else continue;

                // Se continua sendo o bloco pretendido, tenta pegar de novo
                if(stt != EMPTY && readCache[x].fd == ctx->fd && current_block == readCache[x].logic_block_num){
                    idx = get_cache(x);
                    if(idx == -1) continue;
                } else continue;
            }
            
        } else hit_cache.fetch_add(1,std::memory_order_relaxed);
        
        // Aumenta a quantidade de usos do buffer (aumenta o tempo de vida)
        uint32_t uses = readCache[idx].uses.load(std::memory_order_relaxed);
        if(uses < 5) readCache[idx].uses.store(uses+1, std::memory_order_relaxed);

        // Calcula offsets
        uint32_t size_read;
        uint64_t offset_read = (offset + escrito) & (block_size - 1);
        if(offset_read + (size - escrito) > block_size){
            size_read = block_size - offset_read; 
        } else {
            size_read = size - escrito;
        }

        // Escreve no buffer do usuário
        memcpy(
            buf+escrito,
            readCache[idx].buf->data+offset_read,
            size_read
        );

        escrito += size_read;
        
        // Sinaliza que terminou de ler
        readCache[idx].reading.fetch_sub(1, std::memory_order_relaxed);
    }

    return size;
}

int32_t FileSystem::get_cache(uint32_t x){
    // Avisa que vai ler
    readCache[x].reading.fetch_add(1, std::memory_order_acquire);
    
    // Se o cache ainda estiver disponivel, retorna o indice dele
    if(readCache[x].state.load(std::memory_order_relaxed) == CACHED){
        return x; 
    }
    
    // Se der errado, revoga a leitura
    readCache[x].reading.fetch_sub(1, std::memory_order_release);
    return -1;
}

uint32_t FileSystem::get_new_cache(){
    CacheState stt;
    uint8_t expected_uses;
    uint32_t idx;
    uint32_t idle = 0; 

    // Loop principal para tentar pegar um cache
    while(true){
        idx = next_victim.fetch_add(1, std::memory_order_relaxed) & (64 - 1); // Índice do cache analisado
        stt = readCache[idx].state.load(std::memory_order_relaxed); // Estado do cache analisado

        // Se for BUSY_ ou OFF
        if(stt > 1){
            wait.sleep(idle++);
            continue;
        }

        // Se for EMPTY
        if(stt == EMPTY){
            if(readCache[idx].state.compare_exchange_strong(stt, OFF, std::memory_order_acq_rel, std::memory_order_relaxed)){
                return idx;
            }
            continue;
        }

        // Se for CACHED
        expected_uses = readCache[idx].uses.load(std::memory_order_relaxed);
        
        // Se ainda tiver vida útil, reduz a vida útil
        if(expected_uses != 0){
            if(readCache[idx].uses.compare_exchange_weak(expected_uses, expected_uses - 1, std::memory_order_relaxed, std::memory_order_relaxed)){
                // Se o valor não zerou, volta e pega o proximo índice
                if(expected_uses != 1) continue;
            // Se não conseguiu muda o valor, algum processo já mudou, volta do início e pega o próximo
            } else continue;
        }

        // Se ainda tiver leitores no cache, volta e tenta o próximo cache
        if(readCache[idx].reading.load(std::memory_order_acquire) > 0) continue;
        
        // Caso não tenha leitores e vida útil for zero, tenta fechar o cache
        if(readCache[idx].state.compare_exchange_strong(stt, OFF, std::memory_order_acq_rel, std::memory_order_relaxed)){
            // Se conseguir fechar o cache mas algum processo começou a ler antes, revoga tudo e volta a procurar
            if(readCache[idx].reading.load(std::memory_order_acquire) > 0){
                readCache[idx].state.store(CACHED, std::memory_order_release);
                readCache[idx].state.notify_all();
                continue;
            }
            return idx;
        }
    }
}

int FileSystem::fuse_getattr(const char *path, struct stat *stbuf, fuse_file_info *fi){

    memset(stbuf, 0, sizeof(struct stat));

    if(path[0] == '/' && path[1] == '\0'){
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2; 
        
        const fuse_context* ctx = fuse_get_context();
        stbuf->st_uid = ctx->uid; 
        stbuf->st_gid = ctx->gid;
        return 0;
    }

    // Pega os metadados do arquivo
    File *file = get_valid_file(path, fi);
    if (!file) {
        LOG_DEBUG("getattr: Arquivo inexistente solicitado pelo S.O: " << path);
        return -ENOENT;
    }

    // Preenche a struct de metadados
    const fuse_context* ctx = fuse_get_context();
    stbuf->st_uid = ctx->uid;
    stbuf->st_gid = ctx->gid;
    stbuf->st_nlink = 1;
    
    stbuf->st_mode = file->mode;

    stbuf->st_size = file->logical_size.load(std::memory_order_relaxed);
    stbuf->st_blocks = (file->compressed_size.load(std::memory_order_relaxed) + 511) / 512;
    stbuf->st_blksize = file->block_size;

    return 0;
}

int FileSystem::fuse_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi, enum fuse_readdir_flags flags){
    
    if(path[0] != '/' || path[1] != '\0'){
        LOG_DEBUG("readdir: Tentativa de listar diretorio invalido: " << path);
        return -ENOENT; 
    }

    // Para as pastas "." e "..", só o nome (passar NULL no struct stat)
    filler(buf, ".", NULL, 0, static_cast<fuse_fill_dir_flags>(0));
    filler(buf, "..", NULL, 0, static_cast<fuse_fill_dir_flags>(0));

    // Coleta os dados do diretorio
    const fuse_context* ctx = fuse_get_context();
    {
        std::shared_lock<std::shared_mutex> map_lock(*metadata->get_mutex());
        
        for (auto& pair : *metadata->get_all_files()) {
            
            File *file = &pair.second;

            struct stat st;
            memset(&st, 0, sizeof(struct stat));
            
            st.st_nlink = 1;
            st.st_uid = ctx->uid;
            st.st_gid = ctx->gid;

            while(file->lock.test_and_set(std::memory_order_acquire)) _mm_pause();
            st.st_size = file->logical_size.load(std::memory_order_relaxed);
            st.st_mode = file->mode;
            file->lock.clear(std::memory_order_release);

            const char* clean_name = pair.first.c_str() + 1;
            
            if(filler(buf, clean_name, &st, 0, FUSE_FILL_DIR_PLUS) != 0) break; 
        }
    }

    return 0;
}

int FileSystem::fuse_truncate(const char *path, off_t size, fuse_file_info *fi) {

    // Recupera os metadados do arquivo em memória
    File *file = get_valid_file(path, fi);
    if (!file) {
        LOG_DEBUG("truncate: Arquivo inexistente: " << path);
        return -ENOENT;
    }

    // Pega o fd
    int fd;
    if(fi && fi->fh) fd = reinterpret_cast<OpenContext*>(fi->fh)->fd;
    else {
        char host_path[PATH_MAX];
        snprintf(host_path, PATH_MAX, "%s%s", root_path.c_str(), path);
        fd = open(host_path, O_RDWR);
        if(fd < 0) return -errno;
    }

    LOG_DEBUG("truncate: Redimensionando arquivo '" << path << "' para " << size << " bytes (logico)");

    uint32_t blocos_restantes = (size + file->block_size - 1) / file->block_size;

    while(file->lock.test_and_set(std::memory_order_acquire)) _mm_pause();

    uint64_t tamanho_antigo = file->logical_size.load();

    // Verifica se o arquivo diminuiu de tamanho e apaga os blocos dos metadados
    if(size < tamanho_antigo){
        uint64_t cs = file->compressed_size.load(std::memory_order_relaxed);
        for(size_t i = blocos_restantes; i < file->chunk_index.size(); i++){
            cs -= file->chunk_index[i].compressed_size;
            uint32_t tam_m = (file->chunk_index[i].compressed_size + 4095) & ~4095 ;
            
            if(::fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, file->chunk_index[i].physical_offset, tam_m) < 0)
                LOG_ERROR("truncate: punch falhou: " << strerror(errno));

        }
        file->compressed_size.store(cs, std::memory_order_relaxed);
        file->chunk_index.resize(blocos_restantes);
    }

    // Atualiza o tamanho logico do arquivo
    file->logical_size.store(size, std::memory_order_relaxed);

    file->lock.clear(std::memory_order_release);

    if(!(fi && fi->fh)) close(fd);

    return 0;
}

int FileSystem::fuse_unlink(const char *path) {
    
    std::string host_path = root_path + path; 

    // Tenta deletar o arquivo real no disco usando a syscall nativa 'unlink'
    if(unlink(host_path.c_str()) < 0){
        // Registra o erro utilizando as macros de debug já estabelecidas no seu projeto
        LOG_ERROR("unlink: Falha ao deletar no host OS! Path FUSE: " << path 
                  << " | Path Real: " << host_path 
                  << " | Motivo: " << strerror(errno)); 
        return -errno;
    }

    // Remove o arquivo das estruturas de metadados em memória.
    {
        std::unique_lock<std::shared_mutex> map_lock(*metadata->get_mutex()); 

        auto* all_files = metadata->get_all_files(); 
        
        if(all_files != nullptr){
            all_files->erase(path);
        }
    }

    LOG_DEBUG("unlink: Arquivo deletado com sucesso dos metadados e do host: " << path); 

    return 0;
}

FileSystem::~FileSystem(){
    LOG_DEBUG("FileSystem: Iniciando desligamento. Sinalizando thread de background...");
    
    // Define is_running como false para interrromper write_worker e flush_counter
    is_running.store(false, std::memory_order_relaxed);
    wakeup_worker.store(true, std::memory_order_relaxed);
    wakeup_worker.notify_all(); // Acorda write_worker para ele retornar
    
    // Espera write_worker voltar
    if(wt.joinable()) {
        wt.join();
        LOG_DEBUG("FileSystem: Thread de I/O finalizada com sucesso (join concluido).");
    }
    
    LOG_DEBUG("FileSystem: Desligamento completo.");
}

File* FileSystem::get_valid_file(const char* path, fuse_file_info* fi){
    File *file = nullptr;
    
    // Se tiver fi->fh, pega os metadados do arquivo direto do OpenContext
    if(fi != nullptr && fi->fh != 0){
        OpenContext *ctx = reinterpret_cast<OpenContext*>(fi->fh);
        file = ctx->file_meta;

    // Se não tiver fi->fh, busca os metadados do arquivo na classe Metadata
    } else {
        std::shared_lock<std::shared_mutex> map_lock(*metadata->get_mutex());
        if(!metadata->find_file(path)) return nullptr;
        
        file = metadata->get_file(path);
    }

    // Retorna um ponteiro para os metadados do arquivo se o arquivo existir
    return file;
}

int FileSystem::fuse_fsync(const char *path, int isdatasync, struct fuse_file_info *fi){
    
    // Coleta o contexto de execussão do arquivo
    OpenContext *ctx = reinterpret_cast<OpenContext*>(fi->fh);
    ctx->active_fsync.store(true, std::memory_order_release);

    uint32_t idle = 0;
    
    LOG_DEBUG("fsync: Solicitado para o arquivo '" << path << "'");

    while(true){
        // Confirma que NÃO há escritores copiando dados neste momento
        if(ctx->active_writers[0].load() || ctx->active_writers[1].load()) {wait.sleep(idle++); continue;}

        // Garante que a thread de flush acorde para esvaziar a fila
        if(ctx->fragQueue[0]->frag_count() || ctx->fragQueue[1]->frag_count()){
            if(!ctx->wakeup_flush.load()) {ctx->wakeup_flush.store(true); ctx->wakeup_flush.notify_one();}
            wait.sleep(idle++); continue;
        }

        if(ctx->pending_writes.load() == 0) break;
        wait.sleep(idle++);
    }
    
    // Executa Fsync
    LOG_DEBUG("fsync: Ativando FSYNC para o arquivo '" << path << "'");
    int ret;
    if(isdatasync){
        ret = fdatasync(ctx->fd); // Sincroniza só o conteudo
    } else {
        ret = fsync(ctx->fd);     // Sincroniza conteudo + Inodes nativos do ext4
    }
    ctx->active_fsync.store(false, std::memory_order_release);

    LOG_DEBUG("fsync: Finalizado!");

    int err = errno;
    return ret < 0 ? -err : 0;
}
