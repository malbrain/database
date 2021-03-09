#ifndef _TIMESTAMPS_H_
#define _TIMESTAMPS_H_

#include <inttypes.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <memory.h>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <winbase.h>
#include <process.h>
#include <intrin.h>
#include <time.h>

#define	 aligned_malloc _aligned_malloc
#else
#include <x86intrin.h>
#include <pthread.h>
#include <sched.h>
#include <time.h>
#endif

#ifndef _WIN32
#ifdef apple
#include <libkern/OSAtomic.h>
#define pausex() OSMemoryBarrier()
#else
#define pausex() sched_yield()
#endif
#else
#define pausex() YieldProcessor()
#endif

#if !defined(ALIGN) && !defined(ATOMIC) && !defined(CLOCK) && !defined(RDTSC)
#define RDTSC
#endif

#if defined(_WIN32)
#define clock_gettime(mode, spec) (timespec_get(spec, TIME_UTC))
#endif

typedef union {
  struct {
	uint64_t low;
	uint64_t hi;	
  };

  struct {
	uint64_t base;
	time_t tod[1];
  };

#ifdef _WIN32
  uint64_t bitsX2[2];
#else
  __int128 bits[1];
#endif
} TsEpoch;

uint64_t rdtscUnits;

bool pausey(int loops);
bool tsGo;

typedef enum {
  TSAvail = 0,  // initial unassigned TS slot
  TSIdle,       // assigned, nothing pending
  TSGen         // request for next TS
} TSCmd;

typedef union {
#ifndef _WIN32
  __int128 tsBits[1];
#else
  uint64_t tsBits[2];
#endif
  struct {
    union {
        struct {
            uint8_t tsCmd;
            uint8_t tsLatch[1];
        };
        uint32_t tsFiller[1];
    };
    uint32_t tsSeqCnt;
    time_t tsEpoch;
  };
  uint64_t lowHi[2];
#ifdef ALIGN
  uint8_t filler[64];
#endif
} Timestamp;

//  API

void timestampInstall(Timestamp *dest, Timestamp *src, char lock, char unlock);
int64_t timestampCmp(Timestamp * ts1, Timestamp * ts2, char lock, char unlock);
uint64_t timestampInit(Timestamp *tsArray, int tsMaxClients);
uint16_t timestampClnt(Timestamp *tsArray, int tsMaxClients);
void timestampQuit(Timestamp *tsArray, uint16_t idx);
void timestampNext(Timestamp *tsArray, uint16_t idx);
void timestampCAX(Timestamp *dest, Timestamp *src, int chk, int lock, int unlock);

//	intrinsic atomic machine code

uint64_t atomicINC64(volatile uint64_t *dest);
#ifdef _WIN32
bool atomicCAS128(volatile uint64_t *what, uint64_t *comp, uint64_t *repl);
#else
bool atomicCAS128(volatile __int128 *what, __int128 *comp, __int128 *repl);
#endif
#endif
