#pragma once

#include <cstdint>
#include <atomic>
#include <vector>
#include <rise/buffer/buffer.hpp>
#include <rise/metadata/metadata.hpp>

struct OpenContext;
struct ReadCache;

enum RequestType {READ, WRITE, TIMER};

struct Request{
    int id;                                             // Id da requisição para debug
    RequestType type;                                   // Tipo de requisição
    int cqe_res;                                        // Retorno da CQE
    uint32_t chunk_index;                               // Index na chunk
    std::atomic <uint64_t> *total_compressed_size;      // Tamanho total comprimido
    std::vector <Chunk> *file_chunks_ptr;               // Vector com os blocos
    std::atomic_flag* file_lock_ptr;                    // Lock para acessar o vector com segurança

    int fd;                                             // File descriptor
    void* user_buffer;                                  // Buffer do usuário
    OpenContext *ctx;                                   // Contexto de abertura do arquivo
    uint64_t logical_offset;                            // Offset no arquivo descompactado
    uint64_t physical_offset;                           // Offset no arquivo compactado
    uint64_t old_physical_offset;                       // Offset antigo para desalocar memoria
    uint64_t logical_size;                              // Tamanho da requisição no arquivo descompactado
    uint64_t compressed_size;                           // Tamanho da requisiçao no arquivo compactado
    uint64_t old_compressed_size;                       // Size antigo para desalocar memoria
    bool is_rmw;                                        // Booleano para decidir se é necessário desalocar memória
    Buffer *buffer;                                     // Buffer da BufferQueue
    std::atomic<uint64_t> *physical_tail_offset;        // Offset do úlitmo byte do arquivo
    Buffer* origin_buf;                                 // Quando for preciso fazer um RMW, 2 requisições vão usar o mesmo buffer
    ReadCache *readCache;                               // Ponteiro para o cache usado nessa requisição
};