#include <rise/worker/worker.hpp>
#include <rise/request/request.hpp>
#include <rise/buffer/buffer.hpp>
#include <rise/queue/jobqueue.hpp>
#include <rise/io/io_engine.hpp>
#include <rise/fuse/filesystem.hpp>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <immintrin.h>

#ifdef ENABLE_DEBUG
    #define LOG_DEBUG(msg) std::cerr << "[DEBUG] " << msg << "\n"
    #define LOG_ERROR(msg) std::cerr << "[ERROR] " << msg << "\n"
#else
    #define LOG_DEBUG(msg) ((void)0)
    #define LOG_ERROR(msg) ((void)0)
#endif

Worker::Worker(JobQueue *jobQueue, IOQueue *ioQueue, BufferQueue *bufferQueue, RequestQueue *requestQueue, IOEngine *ioEngine, int id):
    jobQueue(jobQueue), 
    ioQueue(ioQueue), 
    bufferQueue(bufferQueue), 
    requestQueue(requestQueue),
    ioEngine(ioEngine),
    compressor(bufferQueue->get_size()),
    id(id){
    
    LOG_DEBUG("Worker: Inicializado e pronto para processar jobs de I/O.");
}

void Worker::run(){
    Request *req;
    uint32_t idle = 0;

    LOG_DEBUG("Worker: Iniciando loop de processamento de tarefas (run)...");

    // Loop principal enquanto flag de encerramento for false e enquanto tiver requisições de compressão/descompressão pendentes
    while(!shutdown.load(std::memory_order_relaxed) || jobQueue->get_count()){

        if(jobQueue->pop(req)){
            idle = 0;
            LOG_DEBUG("worker: iniciando processamento da (requisição " << req->id << ") de offset " << req->logical_offset);
            if(req->type == WRITE){
                // Salva buffer alocado previamente
                Buffer *old = req->buffer;
                Buffer *origin_buf = 0;
                origin_buf = req->origin_buf;

                while(!bufferQueue->pop(req->buffer)) wait.sleep(idle++);
                LOG_DEBUG("buffer: Peguei o buffer " << req->buffer->id << " para colocar dados comprimidos da (requisição " << req->id << ") de offset " << req->logical_offset);

                // Executa a compressão
                uint64_t bytes = 0;
                try {
                    bytes = compressor.compress((char*)req->user_buffer, (char*)req->buffer->data, req->logical_size);
                } catch (const std::exception& e) {
                    LOG_ERROR("Worker [WRITE]: Excecao critica na compressao do arquivo FD " << req->fd << " | Motivo: " << e.what());
                }

                // Se deu certo
                if(bytes){

                    // Alinha memoria preenchendo com 0s até o limite superior multiplo de 4096 (para O_DIRECT)
                    size_t aligned_size = (bytes + 4095) & ~4095;
                    if(aligned_size > bytes) std::memset(req->buffer->data + bytes, 0, aligned_size - bytes);

                    uint64_t current_tail = req->physical_tail_offset->load(std::memory_order_relaxed);
                    uint64_t final_aligned_offset;

                    // Tenta reservar um espaço para escrever
                    while(true){
                        final_aligned_offset = (current_tail + 4095) & ~4095;
                        
                        if(req->physical_tail_offset->compare_exchange_weak(
                            current_tail,
                            final_aligned_offset + aligned_size,
                            std::memory_order_acq_rel,
                            std::memory_order_relaxed
                        )) break;
                    }

                    req->physical_offset = final_aligned_offset;
                    req->compressed_size = bytes; // tamanho real (o ioEngine recalcula o aligned size)

                    // Envia para fila de I/O 
                    idle = 0;
                    while(!ioQueue->push(req)) wait.sleep(idle++);
                    LOG_DEBUG("worker: (requisição " << req->id << ") foi enviada para ioQueue com offset " << req->logical_offset);
                    //###if(idle) LOG_DEBUG("ioQueue: Esperou " << idle << " iterações no worker");

                // Caso a compressão tenha dado errado
                } else {
                    LOG_DEBUG("worker: Erro durante a compressão no worker");
                    throw std::runtime_error("ERRO: A compreesão deu errado");
                }

                // Se tinha um buffer na memoria, devolve para fila de buffer
                if(old){
                    while(!bufferQueue->push(old));
                    LOG_DEBUG("worker: Devolvendo buffer " << old->id << " que estava marcado como old na (requisição " << req->id << ") de offset " << req->logical_offset);
                }
                if(origin_buf){
                    if(origin_buf->refs.fetch_sub(1, std::memory_order_relaxed) == 1){
                        LOG_DEBUG("worker: Ultimo pedaco da escrita finalizado. Devolvendo buffer original " << origin_buf->id << " que estava na (requisição " << req->id << ") de offset " << req->logical_offset);
                        LOG_DEBUG("buffer: Devolvi o buffer " << origin_buf->id);
                        while(!bufferQueue->push(origin_buf));
                    } //###else LOG_DEBUG("worker: Referencias do buffer " << origin_buf->id << " decrementado em 1 na (requisição " << req->id << ") de offset " << req->logical_offset);
                }

            } else if(req->type == READ){
                
                Buffer *dscmp;
                Buffer *origin_buf = req->origin_buf;

                // Pega um buffer para descomprimir
                while(!bufferQueue->pop(dscmp)) wait.sleep(idle++);
                LOG_DEBUG("buffer: Peguei o buffer " << dscmp->id << " para descompressao da (requisição " << req->id << ") que tem offset " << req->logical_offset);

                // Resgata informações da chunk
                while(req->ctx->file_meta->lock.test_and_set(std::memory_order_acquire)) _mm_pause();
                uint32_t chunk_comp_size = req->ctx->file_meta->chunk_index[req->chunk_index].compressed_size;
                uint32_t chunk_log_size  = req->ctx->file_meta->chunk_index[req->chunk_index].logical_size;
                req->ctx->file_meta->lock.clear(std::memory_order_release);

                // Tenta descomprimir
                int64_t bytes = -1;
                try {
                    bytes = compressor.decompress(
                        (char*)req->buffer->data,
                        (char*)dscmp->data,
                        chunk_comp_size,
                        chunk_log_size
                    );
                    uint32_t bs = req->ctx->file_meta->block_size;
                    if(chunk_log_size < bs) memset(dscmp->data + chunk_log_size, 0, bs - chunk_log_size);
                } catch (const std::exception& e) {
                    LOG_ERROR("Worker [READ]: Excecao critica na descompressao do arquivo FD " << req->fd << " | Motivo: " << e.what());
                }
                // Se deu errado
                if(bytes < 0 || bytes != chunk_log_size){
                    LOG_DEBUG("worker: Erro durante a descompressão no worker");
                    throw std::runtime_error("ERRO: A descompressão deu errado");

                // Se deu certo
                } else {
                    LOG_DEBUG("worker: Decompressão bem sucedida para buffer " << dscmp->id << " da (requisição " << req->id << ") de offset " << req->logical_offset);
                    if(req->is_rmw){
                        // Aplica as modificações no buffer
                        std::memcpy(dscmp->data + req->logical_offset, req->user_buffer, req->logical_size);

                        // Deleta o buffer se não tiver mais nenhuma requisição usando
                        if(origin_buf->refs.fetch_sub(1, std::memory_order_relaxed) == 1){
                            LOG_DEBUG("buffer: Ultimo request finalizado. Devolvendo buffer original " << origin_buf->id << " da requisção " << req->id << " de offset " << req->logical_offset);
                            LOG_DEBUG("buffer: Devolvi o buffer " << origin_buf->id);
                            while(!bufferQueue->push(origin_buf));
                        }

                        // Calcula o novo tamanho lógico (porque a escrita do usuário pode ter expandido o final do bloco)
                        uint32_t new_logical_size = std::max(
                            (uint32_t)chunk_log_size, 
                            (uint32_t)(req->logical_offset + req->logical_size)
                        );

                        // Compacta o bloco de novo em cima do buffer velho (req->buffer)
                        uint64_t comp_bytes = 0;
                        try {
                            comp_bytes = compressor.compress((char*)dscmp->data, (char*)req->buffer->data, new_logical_size);
                        } catch (const std::exception& e) {
                            LOG_ERROR("Worker [RMW]: Excecao na recompressao do arquivo FD " << req->fd << " | Motivo: " << e.what());
                        }

                        if(comp_bytes > 0){
                            LOG_DEBUG("worker: Compactação bem sucedida para buffer " << req->buffer->id << " da (requisição " << req->id << ") de offset " << req->logical_offset);
                            
                            // Prepara para O_DIRECT (Alinha o buffer com 0s no final)
                            size_t aligned_size = (comp_bytes + 4095) & ~4095;
                            if(aligned_size > comp_bytes) std::memset(req->buffer->data + comp_bytes, 0, aligned_size - comp_bytes);

                            // Reserva o offset físico alinhado
                            uint64_t current_tail = req->physical_tail_offset->load(std::memory_order_relaxed);
                            uint64_t final_aligned_offset;
                            while(true){
                                final_aligned_offset = (current_tail + 4095) & ~4095;
                                
                                if(req->physical_tail_offset->compare_exchange_weak(
                                    current_tail,
                                    final_aligned_offset + aligned_size,
                                    std::memory_order_acq_rel,
                                    std::memory_order_relaxed
                                )) break;
                            }

                            // Modifica a requisição para virar pacote de WRITE e envia pra ioQueue
                            req->type = WRITE;
                            req->physical_offset = final_aligned_offset;
                            req->compressed_size = comp_bytes;
                            req->logical_size = new_logical_size;
                            
                            // Devolve o buffer novo de descompressão (o user_buffer é mantido seguro)
                            LOG_DEBUG("worker: Devolvendo buffer temporário de RMW " << dscmp->id << " da (requisição " << req->id << ") de offset " << req->logical_offset);
                            LOG_DEBUG("buffer: Devolvi o buffer " << origin_buf->id);
                            while(!bufferQueue->push(dscmp));

                            idle = 0;
                            while(!ioQueue->push(req)) wait.sleep(idle++);
                            LOG_DEBUG("worker: Enviando (requisição " << req->id << ") de offset " << req->logical_offset << " para ioQueue");
                            
                        } else {
                            LOG_DEBUG("worker: Erro durante a re-compressão no worker");
                            throw std::runtime_error("ERRO: A re-compressão falhou");
                        }

                    // --- CACHE MISS NORMAL (FUSE_READ) ---
                    } else {
                        
                        // Devolve buffer da requisição pois os dados comprimidos não são mais necessários
                        LOG_DEBUG("worker: Devolvendo buffer " << req->buffer->id << " de dados comprimidos da (requisição " << req->id << ") de leitura do offset " << req->logical_offset);
                        while(!bufferQueue->push(req->buffer));
                        
                        req->readCache->buf = dscmp;

                        // Incrementa leitores do cache, coloca o cache como CACHED e notifica
                        req->readCache->reading.fetch_add(1, std::memory_order_relaxed);
                        req->readCache->state.store(CacheState::CACHED, std::memory_order_release);
                        req->readCache->state.notify_all();
                        LOG_DEBUG("woker: Descompressão finalizada, retornando...");
                    }
                }
            }
            
            idle = 0;
            continue;
        }
        
        // Suspende worker (exceto se estiver em processo de encerramento)

        if(idle > 10000){
            idle = 0;
            if(!shutdown.load(std::memory_order_relaxed)){
                LOG_DEBUG("worker: Worker " << id << " dormiu");
                jobQueue->suspend(shutdown);
                LOG_DEBUG("worker: Worker " << id << " acordou");
            }
        }
        if(idle++) wait.sleep(idle);
    }
    
    LOG_DEBUG("Worker: Loop principal encerrado com sucesso.");
}

void Worker::stop(){
    LOG_DEBUG("Worker: Recebeu sinal de parada (stop). Desligando motor de processamento...");
    // Envia sinal de encerramento e acorda workers
    shutdown.store(true, std::memory_order_relaxed);
    jobQueue->notify();
}