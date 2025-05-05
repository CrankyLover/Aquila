#include "Timing.h"
#include <iostream>

std::chrono::high_resolution_clock::time_point  start;
//std::mutex time_mutex={};

void startTiming() {
//    time_mutex.lock();
//    if (start.time_since_epoch().count() == 0) {
//        start = std::chrono::high_resolution_clock::now();
//    }
//    time_mutex.unlock();
    start = std::chrono::high_resolution_clock::now();
}

void stopTiming() {
    std::chrono::high_resolution_clock::time_point  end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000.0;
    std::cout << duration << std::endl;
}

double stopTimingAndGive() {
    return  std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start).count() / 1000.0;
}

void stopTimingAndWrite(int flag) {
    
}