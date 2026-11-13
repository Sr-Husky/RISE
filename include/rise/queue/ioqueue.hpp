#pragma once

#include <rise/ringbuffer/ringbuffer.hpp>
#include <rise/request/request.hpp>

class IOQueue{

    public:
        IOQueue();
        bool push(Request*);                // Adiciona requisições de I/O
        bool pop(Request*&);                // Retira uma requisição de I/O
        void suspend(std::atomic<bool>&);   // Suspende o scheduler
        void notify();                      // Acorda o scheduler

    private:
        RingBuffer <Request*> ring;
        std::atomic<uint32_t> count;        // Conta as requisições de I/O da fila
        std::atomic<bool> sleeping;         // Flag que indica se o scheduler está ou não suspenso

};