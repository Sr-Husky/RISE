#pragma once

#include <liburing.h>
#include <cstdint>
#include <unordered_map>
#include <linux/time_types.h>

struct Request;

class IOEngine{

    public:
        IOEngine();
        ~IOEngine();
        bool write(Request*);                               // Envia requisições de write alinhadas com io_uring
        bool read(Request*);                                // Envia requisições de read alinhadas com io_uring
        bool fsync(Request*);                               // Envia requisições de fsync para metadados
        bool fallocate(int, int, uint64_t, uint64_t);       // Envia requisições de fallocate para desalocar memoria
        bool ftruncate(Request*, uint64_t);                 // Envia requisições de ftruncate para truncar
        int submit();                                       // Submete SQEs
        uint32_t get_cqe(Request**, uint32_t);              // Coleta CQEs em lote
        uint32_t get_sq_space();                            // Retorna o espaço atual na fila SQ
    
    private:
        io_uring ring;

};