#pragma once

#include <cstdint>

// Motor de compressão LZ4 prealocado para o Worker
class Compressor{

    public:
        // Inicializa estados LZ4 baseados no config e limite de buffer
        Compressor(uint64_t);
        ~Compressor();
        
        // Comprime dados em RAM e retorna tamanho absoluto gerado
        uint64_t compress(char*, char*, uint32_t);

        // Descomprime dados do disco e retorna bytes restaurados
        int64_t decompress(char*, char*, uint32_t, uint32_t);

    private:
        void *lz4_state;            // Estado prealocado para LZ4 Fast
        uint64_t buffer_size;       // Limite superior seguro (compressBound)

};