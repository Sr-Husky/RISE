#pragma once

#include <rise/worker/worker.hpp>
#include <memory>
#include <thread>

struct ThreadPoolConfig;

class ThreadPool {

    public:
        ThreadPool(JobQueue*, IOQueue*, BufferQueue*, RequestQueue*, IOEngine*);
        ~ThreadPool();
        void run();         // Executa worker(s)
        void stop();        // Interrompe worke(s)

    private:
        std::vector<std::thread> pool;                  // Pool de threads para os workers
        std::vector<std::unique_ptr<Worker>> workers;   // Vetor de workers

};