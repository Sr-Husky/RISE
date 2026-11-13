#pragma once

#include <rise/queue/ioqueue.hpp>
#include <rise/queue/bufferqueue.hpp>
#include <rise/queue/jobqueue.hpp>
#include <rise/queue/requestqueue.hpp>
#include <rise/metadata/metadata.hpp>
#include <rise/io/io_engine.hpp>
#include <rise/request/request.hpp>
#include <rise/wait/wait.hpp>

class Scheduler{

    public:
        Scheduler(JobQueue*, IOQueue*, BufferQueue*, RequestQueue*, Metadata*, IOEngine*);
        void run();     // Inicia a execução do scheduler
        void stop();    // Interrompe a execução do scheduler
        void set_wake(std::atomic<bool>*);      // Recebe um ponteiro para acordar write_worker

    private:
        JobQueue *jobQueue;
        IOQueue *ioQueue;
        BufferQueue *bufferQueue;
        RequestQueue *requestQueue;
        IOEngine *ioEngine;
        Metadata *metadata;
         
        Wait wait;
        uint32_t pending;                       // Requisições de write pendentes
        std::atomic <bool> shutdown{false};     // Flag que indica se o scheduler deve encerrar
        std::atomic <bool> *wakeup_worker;      // Variavel para acordar write_worker quanto requisição terminar

        std::vector<Request*> pending_job_retries;               // Vector para reenviar requisições para IOQueue em caso de falha

};