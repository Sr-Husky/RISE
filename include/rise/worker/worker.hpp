#pragma once

#include <rise/queue/jobqueue.hpp>
#include <rise/queue/ioqueue.hpp>
#include <rise/queue/bufferqueue.hpp>
#include <rise/queue/requestqueue.hpp>
#include <rise/wait/wait.hpp>
#include <rise/compression/compressor.hpp>
#include <atomic>
#include <cstdint>

struct WorkerConfig;
struct WaitConfig;
struct FSConfig;
class IOEngine;

class Worker{

    public:
        Worker(JobQueue*, IOQueue*, BufferQueue*, RequestQueue*, IOEngine*, int);
        void run();     // Inicia execução do worker
        void stop();    // Interrompe execução do worker

    private:
        JobQueue *jobQueue;
        IOQueue *ioQueue;
        BufferQueue *bufferQueue;
        RequestQueue *requestQueue;
        IOEngine *ioEngine;
        Compressor compressor;
        Wait wait;
        int id;
        std::atomic <bool> shutdown{false};     // Flag que indica se os worker devem encerrar

};