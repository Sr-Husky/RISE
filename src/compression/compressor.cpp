#include <rise/compression/compressor.hpp>
#include <stdexcept>
#include <cstdlib>
#include <stdio.h>
#include <lz4hc.h>
#include <lz4.h>
#include <iostream>

#ifdef ENABLE_DEBUG
    #define LOG_DEBUG(msg) std::cerr << "[DEBUG] " << msg << "\n"
    #define LOG_ERROR(msg) std::cerr << "[ERROR] " << msg << "\n"
#else
    #define LOG_DEBUG(msg) ((void)0)
    #define LOG_ERROR(msg) ((void)0)
#endif

Compressor::Compressor(uint64_t buffer_size){
    // Evita alocação dinâmica no hot-path de I/O
    posix_memalign(&lz4_state, 64, LZ4_sizeofState()); // usando alinhamento para tentar melhorar performance
    if(!lz4_state){
        if(lz4_state) free(lz4_state);
        
        LOG_ERROR("Compressor: Falha critica de memoria ao alocar estados para o LZ4.");
        throw std::runtime_error("Compressor: Falha crítica de memória ao alocar estados para o LZ4.");
    }

    // Define o tamanho máximo de segurança para buffers de saída
    this->buffer_size = LZ4_compressBound(buffer_size);
    LOG_DEBUG("Compressor: Inicializado com sucesso | CompressBound: " << this->buffer_size << " bytes");
}

uint64_t Compressor::compress(char *src, char *dst, uint32_t size){

    int bytes_comprimidos;

    bytes_comprimidos = LZ4_compress_fast_extState(
        lz4_state,
        src,
        dst,
        size,
        buffer_size,
        1
    );

    if(bytes_comprimidos <= 0){
        LOG_ERROR("Compressor: Falha na compressao LZ4 (possivel buffer de destino insuficiente)");
        throw std::runtime_error("Compressor: Falha na compressão LZ4 (possível buffer de destino insuficiente)");
    }

    return static_cast<uint64_t>(bytes_comprimidos);

}

int64_t Compressor::decompress(char* src, char* dst, uint32_t size, uint32_t target){

    int bytes_descomprimidos;

    // decompress_safe protege contra falhas de segmentação ou disco corrompido
    bytes_descomprimidos = LZ4_decompress_safe(
        src,
        dst,
        size,
        target
    );

    if(bytes_descomprimidos < 0){
        LOG_ERROR("Compressor: Falha na descompressao LZ4 | Codigo: " << bytes_descomprimidos << " | Dados corrompidos ou mal formados.");
        throw std::runtime_error("Compressor: Falha na descompressão LZ4. Os dados estão corrompidos ou mal formados");
    }

    return static_cast<int64_t>(bytes_descomprimidos);
}

Compressor::~Compressor(){
    LOG_DEBUG("Compressor: Desligando e liberando estados do LZ4...");
    free(lz4_state);
}