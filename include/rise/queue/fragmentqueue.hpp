#pragma once

#include <atomic>
#include <rise/ringbuffer/ringbuffer.hpp>

struct FragmentQueueConfig;

struct Fragment {
    uint64_t logical_offset = 0;    // Offset dos dados no arquivo
    uint32_t size = 0;              // Tamanho do fragmento
    uint64_t buffer_offset = 0;     // Offset do fragmento no buffer 
    uint64_t sequence_id = 0;       // Identificador sequencial
};

class FragmentQueue {

    public:
        FragmentQueue();
        bool push(const Fragment&);             // Adiciona um fragmento pendente na fila
        bool pop(Fragment&);                    // Remove um fragmento
        uint32_t frag_count();                  // Retorna quantos fragmentos pendentes há na fila
        uint64_t total_size();                  // Retorna quantos bytes os fragmentos pendentes ocupam no total

    private:
        RingBuffer<Fragment> ring;
        std::atomic<uint64_t> current_total_size;   // Conta quantos fragmentos pendentes há na fila
        std::atomic<uint32_t> current_frag_count;   // Conta quantos bytes os fragmentos pendentes ocupam no total

};