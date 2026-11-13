#include <rise/io/io_engine.hpp>
#include <rise/request/request.hpp>
#include <rise/fuse/filesystem.hpp>
#include <errno.h>
#include <stdio.h>
#include <stdexcept>
#include <iostream>
#include <cstring>
#include <cstdio>

#ifdef ENABLE_DEBUG
    #define LOG_DEBUG(msg) std::cerr << "[DEBUG] " << msg << "\n"
    #define LOG_ERROR(msg) std::cerr << "[ERROR] " << msg << "\n"
#else
    #define LOG_DEBUG(msg) ((void)0)
    #define LOG_ERROR(msg) ((void)0)
#endif

IOEngine::IOEngine(){
    
    io_uring_params params{};

    // Configurações de execução do io-uring
    params.flags = IORING_SETUP_SQPOLL | IORING_SETUP_SQ_AFF;
    params.sq_thread_idle = 100;
    params.sq_thread_cpu = 10;

    // Inicia io-uring
    int ret = io_uring_queue_init_params(
        64,
        &ring,
        &params
    );

    if (ret < 0) {
        char error_msg[256];
        snprintf(error_msg, sizeof(error_msg), 
                 "Falha fatal ao inicializar io_uring: Erro %d (%s)", 
                 ret, strerror(-ret));
        throw std::runtime_error(error_msg);
    }
    
    LOG_DEBUG("IOEngine: io_uring inicializado com sucesso | Fila SQ/CQ: " << 64);
}

bool IOEngine::write(Request *req){
    io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if(!sqe) return false;

    // Faz alinhamento
    void *src;
    uint32_t size;
    src = req->buffer->data;
    size = (req->compressed_size + 4095) & ~4095;

    // Envia write para SQ
    io_uring_prep_write(
		sqe,
		req->fd,
		src,
		size,
		req->physical_offset
	);

    io_uring_sqe_set_data(sqe, req);
    return true;
}

bool IOEngine::read(Request *req){
    io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if(!sqe) return false;

    // Faz alinhamento
    void *dst;
    uint32_t size;
    dst = req->buffer->data;
    size = (req->compressed_size + 4095) & ~4095;

    // Envia read para SQ
    io_uring_prep_read(
		sqe,
		req->fd,
		dst,
		size,
		req->physical_offset
	);

    io_uring_sqe_set_data(sqe, req);
    return true;
}

bool IOEngine::fsync(Request *req){
    io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if(!sqe) return false;
    
    // Envia Fsync dos metadados para SQ
    io_uring_prep_fsync(sqe, req->fd, IORING_FSYNC_DATASYNC);

    io_uring_sqe_set_data(sqe, req);
    return true;
}

bool IOEngine::fallocate(int fd, int mode, uint64_t offset, uint64_t size){
    io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if (!sqe) return false;

    io_uring_prep_fallocate(sqe, fd, mode, offset, size);

    io_uring_sqe_set_data(sqe, nullptr); 
    
    return true;
}

bool IOEngine::ftruncate(Request* req, uint64_t length){
    io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if (!sqe) return false;

    // Prepara a operação assíncrona de truncagem no anel
    io_uring_prep_ftruncate(sqe, req->fd, length);

    // Como é uma chamada de fechamento, podemos passar nullptr ou um ponteiro de controle
    io_uring_sqe_set_data(sqe, req); 
    
    return true;
}

int IOEngine::submit(){
    // Faz submit das SQ do io-uring
    int ret = io_uring_submit(&ring);
    if (ret < 0) {
        LOG_ERROR("IOEngine::submit - Erro crítico ao submeter syscalls ao Kernel: " << strerror(-ret));
    }
    return ret;
}

uint32_t IOEngine::get_cqe(Request **completed_reqs, uint32_t max_count){

    unsigned head;
    uint32_t count = 0;
    uint32_t count_out = 0;
    io_uring_cqe *cqe;

    // Passa por todas CQEs disponíveis
    io_uring_for_each_cqe(&ring, head, cqe){
        if(count >= max_count) break; // Limite de CQEs por lote

        Request* req = static_cast<Request*>(io_uring_cqe_get_data(cqe));

        if(!req){
            if(cqe->res < 0) LOG_ERROR("Erro silencioso no fallocate background: " << cqe->res);
            count_out++;
            continue;
        } //###else LOG_DEBUG("get_cqe: cqe da (requisição " << req->id << ") de offset " << req->logical_offset << " recebida");

        req->cqe_res = cqe->res;
        
        // cqe->res tem os bytes processados (se > 0) ou -errno (se < 0)
        if(cqe->res < 0 && cqe->res != -ETIME){
            LOG_ERROR("get_cqe: Ocorreu um erro no processamento do arquivo FD " << req->fd 
                      << " | Código io_uring: " << cqe->res 
                      << " | Motivo: " << strerror(-cqe->res));
        }

        // Conta quantas CQEs foram recebidas e guarda um ponteiro para cada uma no vetor de requisições
        completed_reqs[count++] = req;
    }

    // Se conseguiu ler alguma CQE, avança na quantidade que já foi lida
    if((count + count_out) > 0){
        io_uring_cq_advance(&ring, count+count_out);
    }

    //###if(count) LOG_DEBUG("get_cqe: retornando " << count << " requsições para ser tratadas");
    return count;
}

uint32_t IOEngine::get_sq_space(){
    // Retorna o espaço restante da SQ
    return io_uring_sq_space_left(&ring);
}

IOEngine::~IOEngine() {
    LOG_DEBUG("IOEngine: Desligando io_uring e limpando recursos...");
    // Encerra o io-uring
    io_uring_queue_exit(&ring);
}