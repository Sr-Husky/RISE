#include <rise/metadata/metadata.hpp>
#include <shared_mutex>
#include <mutex>

#ifdef ENABLE_DEBUG
    #define LOG_DEBUG(msg) std::cerr << "[DEBUG] " << msg << "\n"
    #define LOG_ERROR(msg) std::cerr << "[ERROR] " << msg << "\n"
#else
    #define LOG_DEBUG(msg) ((void)0)
    #define LOG_ERROR(msg) ((void)0)
#endif

File* Metadata::get_file(std::string path){
    // Retorna um ponteiro para o file de atraves do path (cria se não existir)
    auto [it, exist] = metadataFiles.try_emplace(path);
    return &(it->second);
}

bool Metadata::find_file(std::string path){
    // Retorna true caso o arquivo exista nos metadados
    if(metadataFiles.find(path) == metadataFiles.end()) return false;
    return true;
}

std::unordered_map<std::string, File>* Metadata::get_all_files(){
    // Retorna um ponteiro para o mapa de metadados
    return &metadataFiles;    
}

std::shared_mutex* Metadata::get_mutex(){
    return &metadata_mutex;
}