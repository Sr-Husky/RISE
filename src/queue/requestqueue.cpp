#include <rise/queue/requestqueue.hpp>
#include <rise/request/request.hpp>
#include <iostream>

#ifdef ENABLE_DEBUG
    #define LOG_DEBUG(msg) std::cerr << "[DEBUG] " << msg << "\n"
    #define LOG_ERROR(msg) std::cerr << "[ERROR] " << msg << "\n"
#else
    #define LOG_DEBUG(msg) ((void)0)
    #define LOG_ERROR(msg) ((void)0)
#endif

RequestQueue::RequestQueue() : requestQueue(2048){
    // Aloca a pool de request
    request_pool = new Request[2048];
    for(size_t x=0; x<2048; x++){
        // Adiciona as request no ring
        request_pool[x].id = x;
        requestQueue.push(&request_pool[x]);
    }
    LOG_DEBUG("RequestQueue: Pool de requisicoes pre-alocado com sucesso (" << 2048 << " elementos).");
}

RequestQueue::~RequestQueue(){
    LOG_DEBUG("RequestQueue: Iniciando destruicao, liberando pool de requisicoes da memoria...");
    // Desaloca a pool de requests
    delete [] request_pool;
    LOG_DEBUG("RequestQueue: Destruicao completa.");
}

bool RequestQueue::push(Request *req){
    // Devolve request para a pool
    return requestQueue.push(req);
}

bool RequestQueue::pop(Request *&req){
    // Tira uma resquest da pool
    return requestQueue.pop(req);
}