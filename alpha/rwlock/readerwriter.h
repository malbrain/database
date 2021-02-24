// reader-writer Phase-Fair FIFO lock -- type 1

#ifndef _RWLOCK_H_
#define _RWLOCK_H_

#include "../mutex/mutex.h"

#ifndef RWTYPE
#define RWTYPE 1
#endif

#if RWTYPE == 1

typedef union {
  struct {
	volatile uint16_t writer[1];
	volatile uint16_t reader[1];
  };
  uint32_t bits[1];
} Counter;
	
typedef struct {
  Counter requests[1];
  Counter completions[1];
} RWLock;
#endif

#define RDINCR 0x10000

// reader-writer mutex lock (Neither FIFO nor Fair) -- type 2

#if RWTYPE == 2

typedef struct {
  MyMutex xcl[1];
  MyMutex wrt[1];
  uint16_t readers[1];
} RWLock;
#endif

// reader-writer Phase Fair/FIFO lock -- type 3

#if RWTYPE == 3

typedef volatile struct {
	uint16_t rin[1];
	uint16_t rout[1];
	uint16_t ticket[1];
	uint16_t serving[1];
} RWLock;

#define PHID 0x1
#define PRES 0x2
#define MASK 0x3
#define RINC 0x4
#endif

void readLock(RWLock* lock);
void readUnlock(RWLock* lock);
void writeLock(RWLock* lock);
void writeUnlock(RWLock* lock);
void initLock(RWLock* lock);
#endif