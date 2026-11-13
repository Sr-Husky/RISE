#include <rise/scheduler/scheduler.hpp>
#include <rise/fuse/filesystem.hpp>
#include <rise/io/io_engine.hpp>
#include <linux/time_types.h>
#include <algorithm>
#include <iostream>
#include <cstring>
#include <fcntl.h>
#include <immintrin.h>

#ifdef ENABLE_DEBUG
    #define LOG_DEBUG(msg) std::cerr << "[DEBUG] " << msg << "\n"
    #define LOG_ERROR(msg) std::cerr << "[ERROR] " << msg << "\n"
#else
    #define LOG_DEBUG(msg) ((void)0)
    #define LOG_ERROR(msg) ((void)0)
#endif

Scheduler::Scheduler(JobQueue *jobQueue, IOQueue *ioQueue, BufferQueue *bufferQueue, RequestQueue *requestQueue, Metadata *metadata, IOEngine *ioEngine):
jobQueue(jobQueue), ioQueue(ioQueue), bufferQueue(bufferQueue), requestQueue(requestQueue), metadata(metadata), ioEngine(ioEngine), pending(0){

    LOG_DEBUG("Scheduler: Inicializado com sucesso.");
}

void Scheduler::run(){
    LOG_DEBUG("Scheduler: Iniciando loop principal de execucao...");

    Request *req;
    Request *cqes[32] = {};
    bool pending_fsync = false;
    uint32_t idle = 0;

    // Loop principal enquanto flag de encerramento for falsa, metadados estiverem modificados ou tiver requisições de write pendentes
    while(!shutdown.load(std::memory_order_relaxed) || pending > 0 || !pending_job_retries.empty()){

        // Verifica se tem jobs que deram errado
        for(size_t k=0; k<pending_job_retries.size();){
            if(jobQueue->push(pending_job_retries[k])){
                pending_job_retries[k] = pending_job_retries.back();
                pending_job_retries.pop_back();
            }
            else k++;
        }

        // Pega CQEs para tratar
        uint32_t cqe_count = ioEngine->get_cqe(cqes, 32), expected_size;
        for(int x = 0; x<cqe_count; x++){
            LOG_DEBUG("scheduler: (requisição " << cqes[x]->id << ") de offset " << cqes[x]->logical_offset << " recebida para tratar CQE");
            switch(cqes[x]->type){
                case WRITE:
                    idle = 0;
                    // Calcula tamanhos esperados de de acordo com limites superiores (por causa do O_DIRECT)
                    expected_size = (cqes[x]->compressed_size + 4095) & ~4095;

                    // Se o tamanho estiver correto
                    if(cqes[x]->cqe_res == expected_size){

                        while(cqes[x]->file_lock_ptr->test_and_set(std::memory_order_acquire)) _mm_pause();
                        
                        LOG_DEBUG("scheduler: atualizando metatadados da (requisição " << cqes[x]->id << ") de offset " << cqes[x]->logical_offset);
                        // Atualiza metadados
                        int64_t cs = (int64_t)cqes[x]->compressed_size - (int64_t)((*cqes[x]->file_chunks_ptr)[cqes[x]->chunk_index].compressed_size);
                        cqes[x]->total_compressed_size->fetch_add(cs, std::memory_order_relaxed);
                        
                        Chunk *chunk = &cqes[x]->ctx->file_meta->chunk_index[cqes[x]->chunk_index];
                        chunk->physical_offset = cqes[x]->physical_offset;
                        chunk->compressed_size = cqes[x]->compressed_size;
                        chunk->logical_size = cqes[x]->logical_size;
                        chunk->busy = false;

                        cqes[x]->file_lock_ptr->clear(std::memory_order_release);

                        // Acorda worker se estiver dormindo
                        wakeup_worker->store(true, std::memory_order_release);
                        wakeup_worker->notify_one();

                        // Submissão de chamadas e liberação de memória
                        OpenContext* ctx = cqes[x]->ctx;
                        
                        // Faz o Fallocate do RMW
                        if(cqes[x]->old_compressed_size > 0){
                            uint64_t aligned_old_size = (cqes[x]->old_compressed_size + 4095) & ~4095;
                            if(aligned_old_size > 0){
                                LOG_DEBUG("scheduler: fazendo fallocate da (requisição " << cqes[x]->id << ") de offset " << cqes[x]->logical_offset);
                                uint32_t idle_falloc = 0;
                                if(::fallocate(cqes[x]->fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, cqes[x]->old_physical_offset, aligned_old_size) < 0)
                                    LOG_ERROR("scheduler: punch falhou: " << strerror(errno));
                            }
                        }

                        LOG_DEBUG("scheduler: Escrita da (requisição " << cqes[x]->id << ") no offset " << cqes[x]->logical_offset << " finalizada!");
                        LOG_DEBUG("scheduler: pending_write deve ser decrementado para requsição " << cqes[x]->id << " do offset " << cqes[x]->logical_offset);
                        LOG_DEBUG("scheduler: Devolvendo buffer " << cqes[x]->buffer->id << " da (requisição " << cqes[x]->id << ") do offset " << cqes[x]->logical_offset << " apos o sucesso da CQE");
                        
                        LOG_DEBUG("buffer: Devolvi o buffer " << cqes[x]->buffer->id);
                        while(!bufferQueue->push(cqes[x]->buffer));
                        while(!requestQueue->push(cqes[x]));
                        LOG_DEBUG("scheduler: (requisição " << cqes[x]->id << ") devolvida");
                        
                        int a = ctx->pending_writes.fetch_sub(1, std::memory_order_seq_cst);
                        LOG_DEBUG("scheduler: pending_write decrementado no valor " << 1 << ", peding write agora é " << a-1 << " no arquivo de FD" << ctx->fd);

                    // Se o tamanho estiver errado
                    } else {
                        LOG_ERROR("Scheduler [WRITE]: Erro na gravacao do arquivo FD " << cqes[x]->fd 
                                  << " | Esperado: " << expected_size 
                                  << " | Retorno cqe_res: " << cqes[x]->cqe_res 
                                  << " | Tentando novamente...");

                        throw std::runtime_error("ERRO: O tamanho estava errado no WRITE do scheduler");
                    }

                    pending--;
                    break;
                
                case READ:
                    // Leitura para fazer RMW
                    if(cqes[x]->is_rmw){
                        LOG_DEBUG("scheduler: resultado da requsição de leitura " << cqes[x]->id << " de offset " << cqes[x]->logical_offset << " recebida para RMW!");
                        // Calcula o tamanho alinhado que o IOEngine pediu pro disco
                        uint32_t aligned_expected = (cqes[x]->compressed_size + 4095) & ~4095;

                        // Verifica se a leitura leu a quantidade exata esperada
                        if(cqes[x]->cqe_res == static_cast<ssize_t>(aligned_expected)){
                            uint32_t idle = 0;
                            LOG_DEBUG("scheduler: enviando (requisição " << cqes[x]->id << ") de offset " << cqes[x]->logical_offset << " para jobQueue");
                            if(!jobQueue->push(cqes[x])) pending_job_retries.push_back(cqes[x]);
                            
                        } else {
                            // Falha: Pode ser erro do io_uring (< 0) ou EOF inesperado
                            LOG_ERROR("Scheduler [READ RMW]: Erro ao ler o bloco antigo no disco | Lidos: " << cqes[x]->cqe_res 
                                    << " / Esperados: " << aligned_expected
                                    << " | Motivo: " << (cqes[x]->cqe_res < 0 ? strerror(-cqes[x]->cqe_res) : "EOF Inesperado"));
                            
                            throw std::runtime_error("ERRO: O tamanho estava errado no READ do scheduler");
                        }
                    // Leitura de READ normal para cache
                    } else {
                        LOG_DEBUG("scheduler: resultado da requsição de leitura " << cqes[x]->id << " de offset " << cqes[x]->logical_offset << " recebida para cache!");
                        cqes[x]->readCache->state.store(BUSY_, std::memory_order_release); 
                        cqes[x]->readCache->state.notify_all();
                    }

                    pending--;
                    break;
            }
        }

        // Lê requisições de I/O desde que tenha espaço no ring de SQ
        for(int x=ioEngine->get_sq_space(); x>0; x--){
            if(!ioQueue->pop(req)) break;
            idle = 0;

            // Para requisições de WRITE
            if(req->type == WRITE){
                LOG_DEBUG("scheduler: (requisição " << req->id << ") recebida para write no offset " << req->logical_offset);
                
                ioEngine->write(req);
                pending++;

            // Para requisições de READ
            } else {
                
                if(ioEngine->read(req));//###LOG_DEBUG("scheduler: Requisição de leitura enviada");
                pending++;
            }
        }

        if(pending){
            ioEngine->submit();
            continue;
        }
            
        // Se o scheduler estiver completamente ocioso
        if(pending == 0 && pending_job_retries.empty() && idle >= 2000) {

            // Se o scheduler estiver sendo encerrado, saida do loop principal
            if(shutdown.load(std::memory_order_relaxed)) break;

            // Suspende o scheduler
            ioQueue->suspend(shutdown);

            LOG_DEBUG("IOQueue: Schedueler acordou");
            
            idle = 0;
        } else wait.sleep(idle++);
        
    }

    LOG_DEBUG("Scheduler: Loop principal finalizado.");
}

void Scheduler::stop(){
    LOG_DEBUG("Scheduler: Sinal de parada (stop) recebido. Notificando filas...");
    // Envia sinal de encerramento e arcoda o scheduler
    shutdown.store(true, std::memory_order_relaxed);
    ioQueue->notify();
}

void Scheduler::set_wake(std::atomic<bool> *a){
    wakeup_worker = a;
}