#pragma once

#include <rise/ringbuffer/ringbuffer.hpp>
#include <rise/request/request.hpp>

class JobQueue{

    public:
        JobQueue();
        uint32_t get_count();
        bool push(Request*);                // Adiciona uma requisição de compressão/descompressão na fila
        bool pop(Request*&);                // Remove uma requisição de compressão/descompressão na fila
        void suspend(std::atomic<bool>&);   // Suspende um worker
        void notify();                      // Acorda todos os workers

    private:
        RingBuffer <Request*> ring;
        std::atomic <uint32_t> count;       // Conta quantas requisições há na fila

};