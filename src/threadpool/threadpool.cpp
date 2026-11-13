#include <rise/threadpool/threadpool.hpp>
#include <iostream>

#ifdef ENABLE_DEBUG
    #define LOG_DEBUG(msg) std::cerr << "[DEBUG] " << msg << "\n"
    #define LOG_ERROR(msg) std::cerr << "[ERROR] " << msg << "\n"
#else
    #define LOG_DEBUG(msg) ((void)0)
    #define LOG_ERROR(msg) ((void)0)
#endif

ThreadPool::ThreadPool(JobQueue *jobQueue, IOQueue *ioQueue, BufferQueue *bufferQueue, RequestQueue *requestQueue, IOEngine *ioEngine){

    // Reserva memoria no vector para threads pool e vector
    workers.reserve(8);
    pool.reserve(8);

    // Inicializa workers
    for(int x = 0; x < 8; x++){
        workers.push_back(std::make_unique<Worker>(jobQueue, ioQueue, bufferQueue, requestQueue, ioEngine, x));
    }
    
    LOG_DEBUG("ThreadPool: Inicializado com " << 8 << " workers na memoria.");
}

void ThreadPool::run(){
    LOG_DEBUG("ThreadPool: Iniciando execucao, subindo " << 8 << " threads...");

    // Descobre quantos núcleos lógicos a máquina tem disponível
    unsigned int num_cores = std::thread::hardware_concurrency();
    if (num_cores == 0) num_cores = 1;

    // Executa workers
    for(int x = 0; x < 8; x++){
        pool.emplace_back(&Worker::run, workers[x].get());

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset); // Zera o conjunto de CPUs

        // Define em qual Core esta thread vai rodar (distribuição circular)
        int core_id = x % num_cores;
        CPU_SET(core_id, &cpuset);

        // Aplica a afinidade
        int rc = pthread_setaffinity_np(pool[x].native_handle(), sizeof(cpu_set_t), &cpuset);
        
        if (rc != 0) {
            LOG_ERROR("ThreadPool: Falha ao setar afinidade do worker " << x << " no core " << core_id << " | Erro: " << rc);
        } else {
            LOG_DEBUG("ThreadPool: Worker " << x << " travado exclusivamente no Core " << core_id);
        }

    }
    LOG_DEBUG("ThreadPool: Todas as threads estao rodando.");
}

void ThreadPool::stop(){
    LOG_DEBUG("ThreadPool: Sinal de parada (stop) recebido. Parando workers e aguardando threads (join)...");
    // Interrompe workers
    for(int x = 0; x < 8; x++){
        workers[x]->stop();
    }

    // Interrompe thread pool
    for(int x = 0; x < 8; x++){
        if(pool[x].joinable()){
            pool[x].join();
        }
    }
    LOG_DEBUG("ThreadPool: Todas as threads foram encerradas com sucesso.");
}

ThreadPool::~ThreadPool(){
    LOG_DEBUG("ThreadPool: Destruindo pool de threads e liberando recursos...");
    // Garante que todas threads foram encerradas
    for(auto& t : pool){
        if(t.joinable()){
            t.join();
        }
    }
}