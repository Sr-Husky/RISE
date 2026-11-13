#pragma once

#include <atomic>
#include <cstdint>
#include <stdio.h>
#include <cstdlib>

#define tmp template<typename T>

tmp
struct Cell{
    T val;                      // Valor guardado
    std::atomic<uint64_t> seq;  // Número sequencial
};

tmp
class RingBuffer {
    
    public:
        RingBuffer(uint32_t size);
        ~RingBuffer();
        bool push(const T&);        // Adiciona um valor no ring
        bool pop(T&);               // Remove um valor do ring

    private:
        Cell <T> *data;                             // Ponteiro para o vetor do ring
        alignas(64) std::atomic<uint64_t> head;     // Índice de inserção
        alignas(64) std::atomic<uint64_t> tail;     // Índice de remoção
        const uint32_t MASK;                        // Mascara para operação de modulo
        const uint32_t size;                        // Quantidade máxima de valores do ring

};

tmp
RingBuffer<T>::RingBuffer(uint32_t size) : 
    size(size), MASK(size - 1), head(0), tail(0){

    if(!size || (size & (size-1)) != 0){
        printf("Erro, tamanho do Ring Buffer não é potência de 2\n");
        exit(EXIT_FAILURE);
    }

    data = new Cell<T>[size];
    
    for(uint32_t x=0; x<size; x++){
        data[x].seq.store(x, std::memory_order_relaxed);
    }
}

tmp
RingBuffer<T>::~RingBuffer(){
    delete[] data;
}

tmp
bool RingBuffer<T>::push(const T &val){
    uint64_t hd;
    uint64_t next;
    uint32_t index;

    while(true){
        hd = head.load(std::memory_order_acquire);
        next = hd + 1;
        index = hd & MASK;

        if(data[index].seq.load(std::memory_order_acquire) == hd){
            if(head.compare_exchange_weak(hd, next, std::memory_order_acq_rel, std::memory_order_relaxed)){
                data[index].val = val;
                data[index].seq.store(next, std::memory_order_release);
                return true;
            }
        } else return false;
    }
}

tmp
bool RingBuffer<T>::pop(T &val){
    uint64_t tl;
    uint64_t next;
    uint32_t index;

    while(true){
        tl = tail.load(std::memory_order_acquire);
        next = tl + 1;
        index = tl & MASK;

        if(data[index].seq.load(std::memory_order_acquire) == next){
            if(tail.compare_exchange_weak(tl, next, std::memory_order_acq_rel, std::memory_order_relaxed)){
                val = data[index].val;
                data[index].seq.store(tl+size, std::memory_order_release);
                return true;
            }
        } else return false;
    }
}