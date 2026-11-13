#pragma once

#include <rise/ringbuffer/ringbuffer.hpp>

struct Request;

class RequestQueue {

    public:
        RequestQueue();
        ~RequestQueue();
        bool push(Request*);        // Devolve uma requisição para a pool
        bool pop(Request*&);        // Retira um ponteiro para uma requisição da pool

    private:
        RingBuffer <Request*> requestQueue;
        Request *request_pool;

};