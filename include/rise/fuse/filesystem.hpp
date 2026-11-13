#pragma once

#define FUSE_USE_VERSION 30

#include <rise/ringbuffer/ringbuffer.hpp>
#include <rise/wait/wait.hpp>
#include <rise/metadata/metadata.hpp>
#include <mutex>
#include <atomic>
#include <thread>
#include <fuse.h>
#include <vector>

class Scheduler;
class ThreadPool;
class IOQueue;
class JobQueue;
class BufferQueue;
class RequestQueue;
class Compressor;
class IOEngine;
class FragmentQueue;
struct Buffer;
struct OpenContext;
struct File;

struct WriteJob {
    Buffer* buf;        // Buffer para escrever
    size_t size;        // Tamanho da escrita
    off_t offset;       // Offset da escrita
    OpenContext* ctx;   // Contexto de abertura do arquivo
};

// EMPTY=Sem cahce, CACHED=Cache pronto, BUSY_=Cache ocupado, OFF=Cache momentâneamente inacessível
enum CacheState {EMPTY, CACHED, BUSY_, OFF};
struct alignas(64) ReadCache{
    Buffer* buf = 0;                            // Buffer onde os dados estão em cache
    int fd = 0;                                 // File descriptor do arquivo em cache
    uint32_t logic_block_num;                   // Bloco lógico em cache
    std::atomic <CacheState> state{EMPTY};      // Estado do cache
    std::atomic <uint8_t> uses{0};              // Vida útil do cache
    std::atomic <uint32_t> reading{0};          // Quantidade de leitura atuais
};

struct OpenContext {
    int fd;                                     // File descriptor do arquivo
    File* file_meta;                            // Ponteiro para os metadados do arquivo
    uint8_t *buffer[2];                         // Buffer duplo para escrita
    std::atomic<uint64_t> buffer_head[2];       // Índice de escrita de cada buffer
    std::atomic<uint64_t> request_counter;      // Número sequencial das requisições
    std::atomic<bool> active_buffer{0};         // Número do buffer ativo no momento
    FragmentQueue *fragQueue[2];                // Filas de fragmentos de cada buffer
    std::atomic<uint32_t> active_writers[2];    // Requisições de write que ainda estão copiando os dados em cada buffer
    std::atomic<int> pending_writes{0};         // Operações de write ainda não concluídas
    std::thread ft;                             // Thread do flush_counter
    std::atomic<bool> active_fsync{false};      // Flag para saber se fsync foi ativo ou não
    std::atomic <bool> wakeup_flush{false};     // Flag para acordar flush_counter
    std::atomic <bool> shutdown_flush{false};   // Flag para interromper flush_counter
};

class FileSystem {
    
    public:
        FileSystem(Scheduler*, ThreadPool*, Metadata*, IOEngine*, JobQueue*, IOQueue*, BufferQueue*, RequestQueue*, Compressor*);
        
        // Callbacks para o fuse
        int fuse_create(const char*, mode_t, struct fuse_file_info*);                                                   // Criar e abrir arquivos
        int fuse_open(const char*, fuse_file_info*);                                                                    // Abrir/Truncar arquivos
        int fuse_close(const char*, fuse_file_info*);                                                                   // Fechar arquivos
        int fuse_write(const char*, const char*, size_t, off_t, fuse_file_info*);                                       // Escrever arquivos (Hit and Run)
        int fuse_read(const char*, char*, size_t, off_t, struct fuse_file_info*);                                       // Ler arquivos
        int fuse_getattr(const char*, struct stat*, fuse_file_info*);                                                   // Ler metadados do arquivo
        int fuse_readdir(const char*, void*, fuse_fill_dir_t, off_t, struct fuse_file_info*, enum fuse_readdir_flags);  // Ler diretorios
        int fuse_truncate(const char*, off_t, fuse_file_info*);                                                         // Truncar arquivos
        int fuse_unlink(const char *);                                                                                  // Deleta arquivos
        int fuse_fsync(const char*, int, struct fuse_file_info*);                                                       // Sincronizar dados

        // Funções auxiliares
        void flush_counter(OpenContext*);                       // Writes, Filas e tempo limite para engatilhar o flush
        void force_flush_buffer(OpenContext*, bool, bool);      // Limpa as filas de fragmentos e os buffers, envia os dados ordenados para a fila
        void write_worker();                                    // Fila de dados para escrever
        void send_write(Buffer*, size_t, off_t, OpenContext*);  // Faz RMW se necessário e monta as requisições para os Workers
        uint32_t get_new_cache();                               // Procura um buffer disponivel para usar como cache
        int32_t get_cache(uint32_t);                            // Procura um dado especifico no cache
        File* get_valid_file(const char*, fuse_file_info*);     // Cola metadados de um arquivo se ele existir
        Scheduler *scheduler;
        ThreadPool *threadPool;
        std::thread schedThread;
        
        ~FileSystem();

    private:
        IOQueue *ioQueue;
        JobQueue *jobQueue;
        RequestQueue *requestQueue;
        BufferQueue *bufferQueue;
        IOEngine *ioEngine;
        Compressor *compressor;
        Metadata *metadata;
        Wait wait;

        RingBuffer <WriteJob> write_ring;           // Fila de requisições do flush
        RingBuffer <WriteJob> write_ring_rt;        // Fila de requisições bloqueadas
        std::atomic <bool> wakeup_worker{false};    // Flag para acordar write_worker
        std::atomic <bool> is_running{true};        // Flag para interromper execução do write_worker e do flush_counter
        std::thread wt;                             // Thread do write_worker

        std::string root_path;                      // String como caminho padrão dos arquivos
        
        ReadCache *readCache;                       // Vetor de caches
        std::atomic <uint32_t> next_victim{0};      // Índice do proximo buffer que podera ser sobrescrito
        std::atomic <uint32_t> hit_cache{0};        // Contador de hit cache
};