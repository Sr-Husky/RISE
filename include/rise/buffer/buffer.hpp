#pragma once

#include <cstdint>
#include <atomic>

struct Buffer{
    uint32_t id;                    // Id
    uint8_t* data;                  // Dados
    uint64_t used = 0;              // Quantidade de bytes usados
    std::atomic<uint8_t> refs{0};   // Qunatas requisições estão usando esse buffer
};