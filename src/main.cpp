#define FUSE_USE_VERSION 30

#include <rise/fuse/filesystem.hpp>
#include <rise/metadata/metadata.hpp>
#include <rise/io/io_engine.hpp>
#include <rise/queue/bufferqueue.hpp>
#include <rise/compression/compressor.hpp>

#include <rise/scheduler/scheduler.hpp>
#include <rise/queue/jobqueue.hpp>
#include <rise/queue/ioqueue.hpp>
#include <rise/queue/requestqueue.hpp>

#include <rise/threadpool/threadpool.hpp>

#include <fuse.h>
#include <thread>
#include <iostream>

#ifdef ENABLE_DEBUG
    #define LOG_DEBUG(msg) std::cerr << "[DEBUG] " << msg << "\n"
    #define LOG_ERROR(msg) std::cerr << "[ERROR] " << msg << "\n"
#else
    #define LOG_DEBUG(msg) ((void)0)
    #define LOG_ERROR(msg) ((void)0)
#endif

static int wrap_create(const char* path, mode_t mode, struct fuse_file_info *fi){
    FileSystem *fs = static_cast<FileSystem*>(fuse_get_context()->private_data);
    return fs->fuse_create(path, mode, fi);
}
static int wrap_open(const char *path, fuse_file_info *fi){
    FileSystem *fs = static_cast<FileSystem*>(fuse_get_context()->private_data);
    return fs->fuse_open(path, fi);
}
static int wrap_close(const char *path, fuse_file_info *fi){
    FileSystem *fs = static_cast<FileSystem*>(fuse_get_context()->private_data);
    return fs->fuse_close(path, fi);
}
static int wrap_write(const char *path, const char *buf, size_t size, off_t offset, fuse_file_info *fi){
    FileSystem *fs = static_cast<FileSystem*>(fuse_get_context()->private_data);
    return fs->fuse_write(path, buf, size, offset, fi);
}
static int wrap_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
    FileSystem *fs = static_cast<FileSystem*>(fuse_get_context()->private_data);
    return fs->fuse_read(path, buf, size, offset, fi);
}
static int wrap_fsync(const char *path, int isdatasync, struct fuse_file_info *fi) {
    FileSystem *fs = static_cast<FileSystem*>(fuse_get_context()->private_data);
    return fs->fuse_fsync(path, isdatasync, fi);
}
static int wrap_getattr(const char *path, struct stat *stbuf, fuse_file_info *fi){
    FileSystem *fs = static_cast<FileSystem*>(fuse_get_context()->private_data);
    return fs->fuse_getattr(path, stbuf, fi);
}
static int wrap_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi, enum fuse_readdir_flags flags){
    FileSystem *fs = static_cast<FileSystem*>(fuse_get_context()->private_data);
    return fs->fuse_readdir(path, buf, filler, offset, fi, flags);
}
static int wrap_truncate(const char *path, off_t size, fuse_file_info *fi){
    FileSystem *fs = static_cast<FileSystem*>(fuse_get_context()->private_data);
    return fs->fuse_truncate(path, size, fi);
}
static int wrap_unlink(const char *path){
    FileSystem *fs = static_cast<FileSystem*>(fuse_get_context()->private_data);
    return fs->fuse_unlink(path);
}

static void* wrap_init(struct fuse_conn_info *conn, struct fuse_config *cfg) {
    FileSystem *fs = static_cast<FileSystem*>(fuse_get_context()->private_data);

    cfg->direct_io = 1;

    if(conn->capable & FUSE_CAP_PARALLEL_DIROPS){
        conn->want |= FUSE_CAP_PARALLEL_DIROPS;
    }

    if (conn->capable & FUSE_CAP_ASYNC_DIO) {
        conn->want |= FUSE_CAP_ASYNC_DIO;
        LOG_DEBUG("init: Direct I/O Assincrono (ASYNC_DIO) ATIVADO!");
    }
    
    cfg->attr_timeout = 2.0;
    cfg->entry_timeout = 2.0;

    fs->schedThread = std::thread([fs]{ fs->scheduler->run(); });
    fs->threadPool->run();

    LOG_DEBUG("SYSTEM: FUSE montado com sucesso. Engine de I/O rodando!");

    return fs;
}

static void wrap_destroy(void *private_data){
    FileSystem *fs = static_cast<FileSystem*>(private_data);

    LOG_DEBUG("SYSTEM: Recebido sinal de desligamento (unmount). Parando as maquinas...");

    fs->threadPool->stop();
    fs->scheduler->stop();
    if(fs->schedThread.joinable()) fs->schedThread.join();
}

int main(int argc, char *argv[]){

    LOG_DEBUG("Main: Iniciando construcao dos modulos do sistema RISE FUSE...");

    Metadata metadata;
    IOEngine ioEngine;
    JobQueue jobQueue;
    IOQueue ioQueue;
    BufferQueue bufferQueue;
    RequestQueue requestQueue;
    Scheduler scheduler(&jobQueue, &ioQueue, &bufferQueue, &requestQueue, &metadata, &ioEngine);

    Compressor compressor(bufferQueue.get_size());
    ThreadPool threadPool(&jobQueue, &ioQueue, &bufferQueue, &requestQueue, &ioEngine);
    FileSystem *fuse = new FileSystem(&scheduler, &threadPool, &metadata, &ioEngine, &jobQueue, &ioQueue, &bufferQueue, &requestQueue, &compressor);

    static struct fuse_operations fs_ops = {
        .getattr = wrap_getattr,
        .unlink = wrap_unlink,
        .truncate = wrap_truncate,
        .open = wrap_open,
        .read = wrap_read,
        .write = wrap_write,
        .release = wrap_close,
        .fsync = wrap_fsync,
        .readdir = wrap_readdir,
        .init = wrap_init,
        .destroy = wrap_destroy,
        .create = wrap_create,
    };

    LOG_DEBUG("Main: Passando controle para o kernel (fuse_main)...");

    int status = fuse_main(
        argc, 
        argv, 
        &fs_ops, 
        fuse
    );

    LOG_DEBUG("Main: fuse_main retornou. Limpando FileSystem principal e encerrando com status " << status << ".");

    delete fuse;
    return status;
}