#include <rise/queue/bufferqueue.hpp>
#include <rise/buffer/buffer.hpp>
#include <lz4.h>
#include <iostream>
#include <stdexcept>
#include <cstdlib>
#include <cstring>

#ifdef ENABLE_DEBUG
    #define LOG_DEBUG(msg) std::cerr << "[DEBUG] " << msg << "\n"
    #define LOG_ERROR(msg) std::cerr << "[ERROR] " << msg << "\n"
#else
    #define LOG_DEBUG(msg) ((void)0)
    #define LOG_ERROR(msg) ((void)0)
#endif

BufferQueue::BufferQueue() : ring(1024), available(0), amount(1023), size(4*1024*1024), align(4096){

    // Aloca a pool de buffers
    buffers = new Buffer[amount];

    // Aloca uma memoria alinhada para cada buffer
    for(size_t x=0; x<amount; x++){
        buffers[x].id = x;
        // posix_memalign retorna o erro diretamente, não seta o errno
        int ret = posix_memalign(
            reinterpret_cast<void**>(&buffers[x].data),
            align,                          // Alinhamento
            LZ4_compressBound(size)+4096    // size+bound+4096
        );
        
        if(ret != 0){
            LOG_ERROR("BufferQueue: Erro critico ao alocar buffer alinhado | Motivo: " << strerror(ret));
            throw std::runtime_error("Erro ao alocar buffer alinhado na inicializacao da BufferQueue.");
        }
        
        // Adiciona o buffer pronto no ring e incrementa na quantidade de buffers disponíveis
        ring.push(&buffers[x]);
        available.fetch_add(1,std::memory_order_relaxed);
    }

    LOG_DEBUG("BufferQueue: Fila inicializada com " << amount << " buffers disponiveis. (Align: " << align << ")");
}

BufferQueue::~BufferQueue(){
    LOG_DEBUG("BufferQueue: Iniciando destruicao, liberando buffers da memoria...");
    // Desaloca todos os buffers
    for(size_t x=0; x<amount; x++){
        free(buffers[x].data);
    }
    // Deleta o vetor de buffers
    delete[] buffers;
    LOG_DEBUG("BufferQueue: Destruicao completa.");
}

bool BufferQueue::push(Buffer *buffer){
    // Devolver um buffer para a pool
    if(ring.push(buffer)){
        available.fetch_add(1,std::memory_order_relaxed);
        return true;
    } return false;
}

bool BufferQueue::pop(Buffer *&buffer){
    // Tira um buffer da pool
    if(ring.pop(buffer)){
        available.fetch_sub(1,std::memory_order_relaxed);
        return true;
    } return false;
}

uint64_t BufferQueue::get_size(){
    // Retorna o tamanho dos buffers
    return size;
}

uint32_t BufferQueue::get_available(){
    return available.load(std::memory_order_relaxed);
}