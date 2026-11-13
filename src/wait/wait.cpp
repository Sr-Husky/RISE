#include <rise/wait/wait.hpp>
#include <cstdint>
#include <immintrin.h>
#include <thread>

void Wait::sleep(uint32_t idle){
    if(idle < 10000) return;
    if(idle < 60000){
        _mm_pause();
        return;
    }
    std::this_thread::yield();
}