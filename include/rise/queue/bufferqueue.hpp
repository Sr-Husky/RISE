#pragma once

#include <rise/ringbuffer/ringbuffer.hpp>
#include <rise/buffer/buffer.hpp>

struct BufferConfig;

class BufferQueue{

    public:
        BufferQueue();
        ~BufferQueue();
        bool push(Buffer*);                 // Devolver um buffer para a pool
        bool pop(Buffer*&);                 // Retira um ponteiro para um buffer da pool
        uint64_t get_size();                // Retorna o tamnho do buffer
        uint32_t get_available();           // Retorna a quantidade de buffers disponíveis

    private:
        RingBuffer <Buffer*> ring;
        Buffer *buffers;
        std::atomic <uint32_t> available;   // Quantidade de buffers disponívies
        uint32_t amount;                    // Quantidade de bufers
        uint64_t size;                      // Tamanho dos buffers
        uint64_t align;                     // Alinhamento dos buffers

};