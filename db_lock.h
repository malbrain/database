// set default LOCKTYPE:

#if !defined(LOCKTYPE1) && !defined(LOCKTYPE2) && !defined(LOCKTYPE3) && !defined(LOCKTYPE4)
#define LOCKTYPE2 x
#endif

#ifdef LOCKTYPE1

// reader-writer Phase-Fair FIFO lock -- type 1

typedef volatile union {
  struct {
	uint16_t writer[1];
	uint16_t reader[1];
  };
  uint32_t bits[1];
} Counter;
	
typedef struct {
  Counter requests[1];
  Counter completions[1];
} RWLock;

#define RDINCR 0x10000

#endif

#ifdef LOCKTYPE2

// reader-writer mutex lock (Neither FIFO nor Fair) -- type 2

#ifdef FUTEX
//	Mutex based reader-writer lock

typedef enum {
	FREE = 0,
	LOCKED,
	CONTESTED
} MutexState;

typedef struct {
	volatile MutexState state[1];
} Mutex;
#else
typedef volatile struct {
	char lock[1];
} Mutex;
#endif
void mutex_lock(Mutex* mutex);
void mutex_unlock(Mutex* mutex);

typedef struct {
  Mutex xcl[1];
  Mutex wrt[1];
  uint16_t readers[1];
} RWLock;
#endif

#ifdef LOCKTYPE3

// reader-writer Phase Fair/FIFO lock -- type 3

enum {
	QueRd = 1,	// reader queue
	QueWr = 2	// writer queue
} RWQueue;

typedef volatile union {
	struct {
	  uint16_t rin[1];
	  uint16_t rout[1];
	  uint16_t serving[1];
	  uint16_t ticket[1];
	};
	uint32_t rw[2];
} RWLock;

#define PHID 0x1
#define PRES 0x2
#define MASK 0x3
#define RINC 0x4
#endif

#ifdef LOCKTYPE4

#ifdef _WIN32
#include <windows.h>
#else
#include <pthread.h>
#endif

typedef struct {
#ifndef _WIN32
	pthread_rwlock_t lock[1];
#else
	SRWLOCK srw[1];
#endif
} RWLock;

#endif

//	lock api

void initLock (RWLock *lock);
void writeLock (RWLock *lock);
void writeUnlock (RWLock *lock);
void readLock (RWLock *lock);
void readUnlock (RWLock *lock);

