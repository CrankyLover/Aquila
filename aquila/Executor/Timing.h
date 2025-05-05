#include "Executor.h"
#include <cstdarg>

#include <chrono>

extern std::chrono::high_resolution_clock::time_point  start;

void startTiming();
void stopTiming();
void stopTimingAndWrite(int flag);
double stopTimingAndGive();