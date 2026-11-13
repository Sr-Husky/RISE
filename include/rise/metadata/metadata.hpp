#pragma once

#include <vector>
#include <atomic>
#include <sys/stat.h>
#include <unordered_map>
#include <string>
#include <shared_mutex>

struct Request;

struct Chunk {
    uint64_t physical_offset = 0;       // Offset físico do bloco
    uint64_t compressed_size = 0;       // Tamanho comprimido do bloco
    uint64_t logical_size = 0;          // Tamanho descomprimido do bloco
    bool busy = false;                  // Inicia se o bloco está ou não sendo usado
};

struct File {
    std::atomic <uint64_t> logical_size;        // Tamanho descomprimido do arquivo
    std::atomic <uint64_t> compressed_size{0};  // Tamanho comprimido do arquivo
    uint64_t block_size;                        // Tamanho dos blocos do arquivo
    mode_t mode;                    // Permissões e tipo (ex: arquivo regular ou diretório)

    std::atomic <uint64_t> physical_tail_offset{0};     // Offset do ultimo byte do arquivo
    std::atomic <uint32_t> ative_count{0};              // Quantidade de processos com esse arquivo aberto
    
    std::vector<Chunk> chunk_index;             // Vetor de chunks do arquivo
    std::atomic_flag lock = ATOMIC_FLAG_INIT;   // Trava para ulitlizar o vector com segurança
};

class Metadata {

    public:
        
        File* get_file(std::string);    // Retorna um ponteiro para o arquivo nos metadados através do path
        bool find_file(std::string);    // Retorna se um arquivo existe ou não através do path

        std::shared_mutex* get_mutex();  // Retorna um ponteiro pro mutex
        std::unordered_map<std::string, File>* get_all_files(); // Retorna um ponteiro para para o mapa de arquivos

    private:
        std::unordered_map <std::string, File> metadataFiles;   // Mapa de arquivos
        std::shared_mutex metadata_mutex;                       // Mutex para proteger metadados
};
