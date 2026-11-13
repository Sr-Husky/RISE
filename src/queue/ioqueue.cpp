#include <rise/queue/ioqueue.hpp>
#include <rise/request/request.hpp>
#include <iostream>

#ifdef ENABLE_DEBUG
    #define LOG_DEBUG(msg) std::cerr << "[DEBUG] " << msg << "\n"
    #define LOG_ERROR(msg) std::cerr << "[ERROR] " << msg << "\n"
#else
    #define LOG_DEBUG(msg) ((void)0)
    #define LOG_ERROR(msg) ((void)0)
#endif

IOQueue::IOQueue() : ring(32), count(0), sleeping(false) {
    LOG_DEBUG("IOQueue: Inicializada com capacidade para " << 32 << " requisicoes.");
}

bool IOQueue::push(Request *req){
    // Adiciona uma requisição de I/O na fila
    if(ring.push(req)){
        // Incrementa o contado de I/O na fila e acorda o scheduler
        count.fetch_add(1,std::memory_order_seq_cst);
        if(sleeping.load(std::memory_order_seq_cst)){
            sleeping.store(false, std::memory_order_release);
            sleeping.notify_one();
        }
        return true;
    } return false;
}

bool IOQueue::pop(Request *&req){
    // Retira uma requisição de I/O da fila
    if(ring.pop(req)){
        // Decrementa o contador de I/O pendentes
        count.fetch_sub(1,std::memory_order_release);
        return true;
    } return false;
}

void IOQueue::suspend(std::atomic<bool>& shutdown_flag){
    // Se não tiver requisições pendendentes e não estiver em estado de encerramento
    if(count.load(std::memory_order_seq_cst) == 0 && !shutdown_flag.load(std::memory_order_relaxed)){
        sleeping.store(true, std::memory_order_seq_cst);

        // Double Check se realmente não tem requisição
        if(count.load(std::memory_order_seq_cst) == 0 && !shutdown_flag.load(std::memory_order_relaxed)){
            // Suspende o scheduler
            LOG_DEBUG("IOQueue: Schedueler dormiu...");
            sleeping.wait(true, std::memory_order_acquire); 
        } else {
            // Se chegou requisição de I/O, revoga a suspenção
            sleeping.store(false, std::memory_order_release);
        }
    }
}

void IOQueue::notify(){
    // Acorda o scheduler
    sleeping.store(false, std::memory_order_relaxed);
    sleeping.notify_one();
}