//	Latch Manager

#define _GNU_SOURCE
#include <linux/futex.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <memory.h>
#include <limits.h>

#define SYS_futex 202

int sys_futex(void *addr1, int op, int val1, struct timespec *timeout, void *addr2, int val3)
{
	return syscall(SYS_futex, addr1, op, val1, timeout, addr2, val3);
}

#include "rwfutex.h"
uint64_t FutexCnt[1];

#define pause() asm volatile("pause\n": : : "memory")

int lock_spin (int *cnt) {
volatile int idx;

	if (!*cnt)
	  *cnt = 8;
	else if (*cnt < 1024 * 1024)
	  *cnt += *cnt / 4;

	if (*cnt < 1024 )
	  for (idx = 0; idx < *cnt; idx++)
		pause();
	else
		return 1;

	return 0;
}

void mutex_lock(Mutex *mutex) {
MutexState nxt =  LOCKED;
uint32_t spinCount = 0;

  while (__sync_val_compare_and_swap(mutex->state, FREE, nxt) != FREE)
	while (*mutex->state != FREE)
	  if (lock_spin (&spinCount)) {
		if (*mutex->state == LOCKED)
    	  if (__sync_val_compare_and_swap(mutex->state, LOCKED, CONTESTED) == FREE)
			break;

  		__sync_fetch_and_add(FutexCnt, 1);
		sys_futex((void *)mutex->state, FUTEX_WAIT, CONTESTED, NULL, NULL, 0);
		nxt = CONTESTED;
		break;
	  }
}

void mutex_unlock(Mutex* mutex) {
	if (__sync_fetch_and_sub(mutex->state, 1) == CONTESTED)  {
   		*mutex->state = FREE;
 		sys_futex( (void *)mutex->state, FUTEX_WAKE, 1, NULL, NULL, 0);
   }
}

//  a phase fair reader/writer lock implementation

void WriteLock (RWLock *lock)
{
uint32_t spinCount = 0;
uint16_t w, r, tix;
uint32_t prev;

	tix = __sync_fetch_and_add (lock->ticket, 1);

	// wait for our ticket to come up in serving

	while( 1 ) {
		prev = lock->rw[1];
		if( tix == (uint16_t)prev )
		  break;

		// add ourselves to the waiting for write ticket queue

  		__sync_fetch_and_add(FutexCnt, 1);
		sys_futex( (void *)&lock->rw[1], FUTEX_WAIT_BITSET, prev, NULL, NULL, QueWr );
	}

	// wait for existing readers to drain while allowing new readers queue

	w = PRES | (tix & PHID);
	r = __sync_fetch_and_add (lock->rin, w);

	while( 1 ) {
		prev = lock->rw[0];
		if( r == (uint16_t)(prev >> 16))
		  break;

		// we're the only writer waiting on the readers ticket number

  		__sync_fetch_and_add(FutexCnt, 1);
		sys_futex( (void *)&lock->rw[0], FUTEX_WAIT_BITSET, prev, NULL, NULL, QueWr );
	}
}

void WriteUnlock (RWLock *lock)
{
	// clear writer waiting and phase bit
	//	and advance writer ticket

	__sync_fetch_and_and (lock->rin, ~MASK);
	lock->serving[0]++;

	if( (*lock->rin & ~MASK) != (*lock->rout & ~MASK) )
	  if( sys_futex( (void *)&lock->rw[0], FUTEX_WAKE_BITSET, INT_MAX, NULL, NULL, QueRd ) )
		return;

	//  is writer waiting (holding a ticket)?

	if( *lock->ticket == *lock->serving )
		return;

	//	are rest of writers waiting for this writer to clear?
	//	(have to wake all of them so ticket holder can proceed.)

	sys_futex( (void *)&lock->rw[1], FUTEX_WAKE_BITSET, INT_MAX, NULL, NULL, QueWr );
}

void ReadLock (RWLock *lock)
{
uint32_t spinCount = 0;
uint32_t prev;
uint16_t w;

	w = __sync_fetch_and_add (lock->rin, RINC) & MASK;

	if( w )
	  while( 1 ) {
		prev = lock->rw[0];
		if( w != (prev & MASK))
		  break;
  		__sync_fetch_and_add(FutexCnt, 1);
		sys_futex( (void *)&lock->rw[0], FUTEX_WAIT_BITSET, prev, NULL, NULL, QueRd );
	  }
}

void ReadUnlock (RWLock *lock)
{
	__sync_fetch_and_add (lock->rout, RINC);

	// is a writer waiting for this reader to finish?

	if( *lock->rin & PRES )
	  sys_futex( (void *)&lock->rw[0], FUTEX_WAKE_BITSET, 1, NULL, NULL, QueWr );

	// is a writer waiting for reader cycle to finish?

	else if( *lock->ticket != *lock->serving )
	  sys_futex( (void *)&lock->rw[1], FUTEX_WAKE_BITSET, INT_MAX, NULL, NULL, QueWr );
}


//	mutex based reader-writer lock

void WriteLock2 (RWLock2 *lock)
{
	mutex_lock(lock->xcl);
	mutex_lock(lock->wrt);
	mutex_unlock(lock->xcl);
}

void WriteUnlock2 (RWLock2 *lock)
{
	mutex_unlock(lock->wrt);
}

void ReadLock2 (RWLock2 *lock)
{
	mutex_lock(lock->xcl);

	if( !__sync_fetch_and_add (lock->readers, 1) )
		mutex_lock(lock->wrt);

	mutex_unlock(lock->xcl);
}

void ReadUnlock2 (RWLock2 *lock)
{
	if( !__sync_sub_and_fetch (lock->readers, 1) )
		mutex_unlock(lock->wrt);
}

//	lite weight futex Latch Manager

//	wait until write lock mode is clear
//	and add 1 to the share count

void futex_readlock(FutexLock *lock)
{
uint32_t waited = 0;
FutexLock prev[1];

  while( 1 ) {
	// increment reader count

	*prev->longs = __sync_fetch_and_add (lock->longs, SHARE);

	//  see if exclusive request is not already granted
	//	 or if it is reader phase

	if( waited || !lock->wrt )
	  if( !lock->xlock )
		return;

	//	wait for writer to release lock

	 __sync_fetch_and_sub (lock->longs, SHARE);
	 __sync_fetch_and_or (lock->longs, READ);

	if( lock->xlock && !prev->share )
	  sys_futex( (void *)lock->longs, FUTEX_WAKE_BITSET, INT_MAX, NULL, NULL, QueWr );

	prev->read = 1;
	waited = 1;

	__sync_fetch_and_add(FutexCnt, 1);
	sys_futex( (void *)lock->longs, FUTEX_WAIT_BITSET, *prev->longs, NULL, NULL, QueRd );
  }
}

//	wait for other read and write locks to release

void futex_writelock(FutexLock *lock)
{
uint32_t slept = 0, ours = 0;
FutexLock temp[1];

  ours = 0;

  while( 1 ) {
	*temp->longs = *lock->longs;

	if (!ours)
	  ours = ~__sync_fetch_and_or(lock->longs, XCL) & XCL;

	if( ours )			// did we set XCL bit?
	  if( !temp->share )	{	// any active readers?
	    if( slept )
		  __sync_fetch_and_sub(lock->shorts, WRT);
		return;
	  }

	if( !slept ) {
	  temp->shorts[0] = __sync_fetch_and_add(lock->shorts, WRT) + WRT;
	  slept = 1;
	}

	if (ours && !lock->share)
		continue;

	__sync_fetch_and_add(FutexCnt, 1);
	sys_futex ((void *)lock->longs, FUTEX_WAIT_BITSET, *temp->longs, NULL, NULL, QueWr);
  }
}

//	try to obtain write lock

//	return 1 if obtained,
//		0 otherwise

int futex_writetry(FutexLock *lock)
{
FutexLock prev[1];

	*prev->longs = __sync_fetch_and_or(lock->longs, XCL);

	//	take write access if all bits are clear

	if( !prev->xlock )
	  if( !prev->share )
		return 1;
	  else
		__sync_fetch_and_and(lock->longs, ~XCL);

	return 0;
}

//	clear write mode
//	wake up sleeping readers

void futex_releasewrite(FutexLock *lock)
{
FutexLock prev[1];

	*prev->longs = __sync_fetch_and_and(lock->longs, ~(XCL | READ));

	//	alternate read/write phases

	//	are readers waiting?

	if( prev->read ) {
	  if( sys_futex( (void *)lock->longs, FUTEX_WAKE_BITSET, INT_MAX, NULL, NULL, QueRd ) )
		return;
	}

	if( lock->wrt )
	  sys_futex( (void *)lock->longs, FUTEX_WAKE_BITSET, INT_MAX, NULL, NULL, QueWr );
}

//	decrement reader count
//	wake up sleeping writers

void futex_releaseread(FutexLock *lock)
{
FutexLock prev[1];

	*prev->longs = __sync_sub_and_fetch(lock->longs, SHARE);

	//	alternate read/write phases

	if( prev->wrt || prev->xlock ) {
	  if( !prev->share )
		sys_futex( (void *)lock->longs, FUTEX_WAKE_BITSET, INT_MAX, NULL, NULL, QueWr );
	  return;
	}
}

#ifdef STANDALONE
#include <stdio.h>

#include <time.h>
#include <sys/resource.h>

double getCpuTime(int type)
{
struct rusage used[1];
struct timeval tv[1];

	switch( type ) {
	case 0:
		gettimeofday(tv, NULL);
		return (double)tv->tv_sec + (double)tv->tv_usec / 1000000;

	case 1:
		getrusage(RUSAGE_SELF, used);
		return (double)used->ru_utime.tv_sec + (double)used->ru_utime.tv_usec / 1000000;

	case 2:
		getrusage(RUSAGE_SELF, used);
		return (double)used->ru_stime.tv_sec + (double)used->ru_stime.tv_usec / 1000000;
	}

	return 0;
}

#include <pthread.h>

unsigned char Array[256] __attribute__((aligned(64)));
pthread_rwlock_t syslock[1] = {PTHREAD_RWLOCK_INITIALIZER};
FutexLock futexlock[1];
RWLock rwlock[1];
RWLock2 rwlock2[1];

enum {
	systemType,
	futexType,
	RWType,
	RWType2
} LockType;

typedef struct {
	int threadNo;
	int loops;
	int type;
} Arg;

void work (int usecs, int shuffle) {
volatile int cnt = usecs * 300;
int first, idx;

	while(shuffle && usecs--) {
	  first = Array[0];
	  for (idx = 0; idx < 255; idx++)
		Array[idx] = Array[idx + 1];

	  Array[255] = first;
	}

	while (cnt--)
#ifdef unix
		__sync_fetch_and_add(&usecs, 1);
#else
		InterlockedIncrement(&usecs);
#endif
}

void *launch(void *info) {
Arg *arg = (Arg *)info;
int idx;

	for( idx = 0; idx < arg->loops; idx++ ) {
		if (arg->type == systemType)
		  pthread_rwlock_rdlock(syslock), work(1, 0), pthread_rwlock_unlock(syslock);
		else if (arg->type == futexType)
		  futex_readlock(futexlock), work(1, 0), futex_releaseread(futexlock);
		else if (arg->type == RWType)
		  ReadLock(rwlock), work(1, 0), ReadUnlock(rwlock);
		else if (arg->type == RWType2)
		  ReadLock2(rwlock2), work(1, 0), ReadUnlock2(rwlock2);
		else
		  work(1, 0);

		if( (idx & 511) == 0)
		  if (arg->type == systemType)
		    pthread_rwlock_wrlock(syslock), work(10, 1), pthread_rwlock_unlock(syslock);
		  else if (arg->type == futexType)
			futex_writelock(futexlock), work(10, 1), futex_releasewrite(futexlock);
		  else if (arg->type == RWType)
			WriteLock(rwlock), work(10, 1), WriteUnlock(rwlock);
		  else if (arg->type == RWType2)
			WriteLock2(rwlock2), work(10, 1), WriteUnlock2(rwlock2);
		  else
			work(10, 0);
#ifdef DEBUG
	  if (arg->type >= 0)
		if (!(idx % 100000))
			fprintf(stderr, "Thread %d loop %d\n", arg->threadNo, idx);
#endif
	}

#ifdef DEBUG
	fprintf(stderr, "Thread %d finished\n", arg->threadNo);
#endif
	return NULL;
}

int main (int argc, char **argv)
{
double start, elapsed, overhead[3];
int idx, threadCnt, lockType;
pthread_t thread_id[1];
pthread_t *threads;
Arg *args, base[1];

	for (idx = 0; idx < 256; idx++)
		Array[idx] = idx;

	if (argc < 2) {
		fprintf(stderr, "Usage: %s #thrds type\n", argv[0]); 
		printf("sizeof SystemLatch: %d\n", (int)sizeof(syslock));
		printf("sizeof FutexLock: %d\n", (int)sizeof(FutexLock));
		printf("sizeof RWLock: %d\n", (int)sizeof(RWLock));
		printf("sizeof RWLock2: %d\n", (int)sizeof(RWLock2));
		printf("sizeof Mutex: %d\n", (int)sizeof(Mutex));
		exit(1);
	}

	//	calculate non-lock timing

	base->loops = 1000000;
	base->threadNo = 0;
	base->type = -1;

	start = getCpuTime(0);
	launch(base);

	overhead[0] = getCpuTime(0) - start;
	overhead[1] = getCpuTime(1);
	overhead[2] = getCpuTime(2);

	threadCnt = atoi(argv[1]);
	lockType = atoi(argv[2]);

	args = calloc(threadCnt, sizeof(Arg));

	threads = malloc (threadCnt * sizeof(pthread_t));

	for (idx = 0; idx < threadCnt; idx++) {
	  args[idx].loops = 1000000/threadCnt;
	  args[idx].type = lockType;
	  args[idx].threadNo = idx;

	  if (pthread_create(threads + idx, NULL, launch, args + idx))
		fprintf(stderr, "Unable to create thread %d, errno = %d\n", idx, errno);
	}

	// 	wait for termination

	for( idx = 0; idx < threadCnt; idx++ )
		pthread_join (threads[idx], NULL);

	for( idx = 0; idx < 256; idx++)
	  if (Array[idx] != (unsigned char)(Array[(idx+1) % 256] - 1))
		fprintf (stderr, "Array out of order\n");

	elapsed = getCpuTime(0) - start;
	elapsed -= overhead[0];
	if (elapsed < 0)
		elapsed = 0;
	fprintf(stderr, " real %.3fus\n", elapsed);
	elapsed = getCpuTime(1);
	elapsed -= overhead[1];
	if (elapsed < 0)
		elapsed = 0;
	fprintf(stderr, " user %.3fus\n", elapsed);
	elapsed = getCpuTime(2);
	elapsed -= overhead[2];
	if (elapsed < 0)
		elapsed = 0;
	fprintf(stderr, " sys  %.3fus\n", elapsed);
	fprintf(stderr, " futex waits: %lld\n", FutexCnt[0]);
}
#endif
