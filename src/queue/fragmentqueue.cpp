#include <rise/queue/fragmentqueue.hpp>
#include <iostream>

#ifdef ENABLE_DEBUG
    #define LOG_DEBUG(msg) std::cerr << "[DEBUG] " << msg << "\n"
    #define LOG_ERROR(msg) std::cerr << "[ERROR] " << msg << "\n"
#else
    #define LOG_DEBUG(msg) ((void)0)
    #define LOG_ERROR(msg) ((void)0)
#endif

FragmentQueue::FragmentQueue() : current_total_size(0), current_frag_count(0), ring(2048){
    LOG_DEBUG("FragmentQueue: Inicializada com capacidade para " << 2048 << " fragmentos.");
}

bool FragmentQueue::push(const Fragment &f) {
    // Adiciona um fragmento na fila
    if(ring.push(f)){
        // Incrementa o numero de fragmentos e o tamanho total dos fragmentos da fila
        current_total_size.fetch_add(f.size, std::memory_order_relaxed);
        current_frag_count.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    return false;
}

bool FragmentQueue::pop(Fragment &f) {
    // Remove um fragmento da fila
    if(ring.pop(f)){
        // Decrementa o numero de fragmentos e o tamanho total dos fragmentos da fila
        current_total_size.fetch_sub(f.size, std::memory_order_relaxed);
        current_frag_count.fetch_sub(1, std::memory_order_relaxed);
        return true;
    }
    return false;
}

uint32_t FragmentQueue::frag_count(){
    // Retorna a quantidade de fragmentos pendentes na fila
    return current_frag_count.load(std::memory_order_relaxed);
}

uint64_t FragmentQueue::total_size(){
    // Retorna a quantidade de bytes total de fragmentos pendentes na fila
    return current_total_size.load(std::memory_order_relaxed);
}