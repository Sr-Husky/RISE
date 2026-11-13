#include <rise/queue/jobqueue.hpp>
#include <rise/request/request.hpp>
#include <iostream>

#ifdef ENABLE_DEBUG
    #define LOG_DEBUG(msg) std::cerr << "[DEBUG] " << msg << "\n"
    #define LOG_ERROR(msg) std::cerr << "[ERROR] " << msg << "\n"
#else
    #define LOG_DEBUG(msg) ((void)0)
    #define LOG_ERROR(msg) ((void)0)
#endif

JobQueue::JobQueue() : ring(32), count(0){
    LOG_DEBUG("JobQueue: Inicializada com capacidade para " << 32 << " jobs.");
}

bool JobQueue::push(Request *req){
    // Adiciona uma requisição de compressão/descompressão na fila
    if(ring.push(req)){
        // Incrementa no contador de requisições e acorda um worker
        count.fetch_add(1,std::memory_order_seq_cst);
        count.notify_one();
        return true;
    } return false;
}

uint32_t JobQueue::get_count(){
    return count.load(std::memory_order_relaxed);
}

bool JobQueue::pop(Request *&req){
    // Remove uma requisição de compressão/descompressão da fila
    if(ring.pop(req)){
        // Decrementa no contador de requisições
        count.fetch_sub(1,std::memory_order_seq_cst);
        return true;
    } return false;
}

void JobQueue::suspend(std::atomic<bool> &shutdown){
    uint32_t estado_atual = count.load(std::memory_order_seq_cst);
    
    if(!shutdown.load(std::memory_order_relaxed) && estado_atual == 0){
        count.wait(estado_atual, std::memory_order_seq_cst);
    }
}

void JobQueue::notify(){
    count.notify_all();
}