//  reader/writer lock implementations
//	by Karl Malbrain, malbrain@cal.berkeley.edu
//	20 JUN 2016

#define _XOPEN_SOURCE 700
#include <stdlib.h>
#include <stdint.h>
#include <limits.h>
#include <memory.h>
#include <errno.h>
#include <time.h>

#include "db_lock.h"

#ifndef _WIN32
#include <sched.h>
#else
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <process.h>
#endif

#ifdef FUTEX
#include <linux/futex.h>
#define SYS_futex 202

int sys_futex(void *addr1, int op, int val1, struct timespec *timeout, void *addr2, int val3)
{
	return syscall(SYS_futex, addr1, op, val1, timeout, addr2, val3);
}
#endif

#ifndef _WIN32
#ifdef apple
#include <libkern/OSAtomic.h>
#define pause() OSMemoryBarrier()
#else
#define pause() __asm __volatile("pause\n": : : "memory")
#endif

void lock_sleep (int cnt) {
struct timespec ts[1];

	ts->tv_sec = 0;
	ts->tv_nsec = cnt;
	nanosleep(ts, NULL);
}

int lock_spin (uint32_t *cnt) {
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
#else

void lock_sleep (int ticks) {
LARGE_INTEGER start[1], freq[1], next[1];
double conv, interval;
uint32_t idx;

	QueryPerformanceFrequency(freq);
	QueryPerformanceCounter(next);
	conv = (double)freq->QuadPart / 100000000.; 

	for (idx = 0; idx < ticks; idx += (uint32_t)interval) {
		*start = *next;
		Sleep(0);
		QueryPerformanceCounter(next);
		interval = ((next->QuadPart - start->QuadPart) / conv);
	}
}

int lock_spin (uint32_t *cnt) {
volatile uint32_t idx;

	if (!*cnt)
	  *cnt = 8;

	if (*cnt < 1024 * 1024)
	  *cnt += *cnt / 4;

	if (*cnt < 1024 )
	  for (idx = 0; idx < *cnt; idx++)
		YieldProcessor();
 	else
 		return 1;

	return 0;
}
#endif

//	mutex implementation

#ifndef FUTEX
#ifndef _WIN32
void mutex_lock(KMMutex* mutex) {
uint32_t spinCount = 0;

  while (__sync_fetch_and_or(mutex->lock, 1) & 1)
	while (*mutex->lock)
	  if (lock_spin (&spinCount))
		lock_sleep(spinCount);
}

void mutex_unlock(KMMutex* mutex) {
	//asm volatile ("" ::: "memory");
	*mutex->lock = 0;
}
#else
void mutex_lock(KMMutex* mutex) {
uint32_t spinCount = 0;

  while (_InterlockedOr8(mutex->lock, 1) & 1)
	while (*mutex->lock & 1)
	  if (lock_spin(&spinCount))
		lock_sleep(spinCount);
}

void mutex_unlock(KMMutex* mutex) {
	*mutex->lock = 0;
}
#endif
#else
void mutex_lock(KMMutex *mutex) {
MutexState nxt =  LOCKED;
uint32_t spinCount = 0;

  while (__sync_val_compare_and_swap(mutex->state, FREE, nxt) != FREE)
	while (*mutex->state != FREE)
	  if (lock_spin (&spinCount)) {
		if (*mutex->state == LOCKED)
    	  if (__sync_val_compare_and_swap(mutex->state, LOCKED, CONTESTED) == FREE)
			break;

		sys_futex((void *)mutex->state, FUTEX_WAIT, CONTESTED, NULL, NULL, 0);
		nxt = CONTESTED;
		break;
	  }
}

void mutex_unlock(KMMutex* mutex) {
	if (__sync_fetch_and_sub(mutex->state, 1) == CONTESTED)  {
   		*mutex->state = FREE;
 		sys_futex( (void *)mutex->state, FUTEX_WAKE, 1, NULL, NULL, 0);
   }
}
#endif

#ifdef LOCKTYPE1

void initLock(RWLock *latch) {
}

#ifdef FUTEX

//  a simple phase fair reader/writer lock implementation

void writeLock(RWLock *lock)
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

		sys_futex( (void *)&lock->rw[0], FUTEX_WAIT_BITSET, prev, NULL, NULL, QueWr );
	}
}

void writeUnlock (RWLock *lock)
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

void readLock (RWLock *lock)
{
uint32_t prev;
uint16_t w;

	w = __sync_fetch_and_add (lock->rin, RINC) & MASK;

	if( w )
	  while( 1 ) {
		prev = lock->rw[0];
		if( w != (prev & MASK))
		  break;
		sys_futex( (void *)&lock->rw[0], FUTEX_WAIT_BITSET, prev, NULL, NULL, QueRd );
	  }
}

void readUnlock (RWLock *lock)
{
	__sync_fetch_and_add (lock->rout, RINC);

	// is a writer waiting for this reader to finish?

	if( *lock->rin & PRES )
	  sys_futex( (void *)&lock->rw[0], FUTEX_WAKE_BITSET, 1, NULL, NULL, QueWr );

	// is a writer waiting for reader cycle to finish?

	else if( *lock->ticket != *lock->serving )
	  sys_futex( (void *)&lock->rw[1], FUTEX_WAKE_BITSET, INT_MAX, NULL, NULL, QueWr );
}

#else

//  a simple phase fair reader/writer lock implementation

void writeLock (RWLock *lock)
{
Counter prev[1], next[1];
uint32_t spinCount = 0;

	do {
	  *prev->bits = *lock->requests->bits;
	  *next->bits = *prev->bits;
	  next->writer[0]++;
# ifndef _WIN32
	} while (!__sync_bool_compare_and_swap(lock->requests->bits, *prev->bits, *next->bits));
# else
	} while (_InterlockedCompareExchange(lock->requests->bits, *next->bits, *prev->bits) != *prev->bits);
# endif

	while (lock->completions->bits[0] != prev->bits[0])
	  if (lock_spin(&spinCount))
		lock_sleep(spinCount);
}

void writeUnlock (RWLock *lock)
{
# ifndef _WIN32
	__sync_fetch_and_add (lock->completions->writer, 1);
# else
	_InterlockedExchangeAdd16(lock->completions->writer, 1);
# endif
}

void readLock (RWLock *lock)
{
uint32_t spinCount = 0;
Counter prev[1];

# ifndef _WIN32
	*prev->bits = __sync_fetch_and_add (lock->requests->bits, RDINCR);
# else
	*prev->bits =_InterlockedExchangeAdd(lock->requests->bits, RDINCR);
# endif
	
	while (*lock->completions->writer != *prev->writer)
	  if (lock_spin(&spinCount))
		lock_sleep(spinCount);
}

void readUnlock (RWLock *lock)
{
# ifndef _WIN32
	__sync_fetch_and_add (lock->completions->reader, 1);
# else
	_InterlockedExchangeAdd16(lock->completions->reader, 1);
# endif
}
#endif
#endif

#ifdef LOCKTYPE2

void initLock(RWLock *latch) {
}

//	mutex based reader-writer lock

void writeLock (RWLock *lock)
{
	mutex_lock(lock->xcl);
	mutex_lock(lock->wrt);
	mutex_unlock(lock->xcl);
}

void writeUnlock (RWLock *lock)
{
	mutex_unlock(lock->wrt);
}

void readLock (RWLock *lock)
{
	mutex_lock(lock->xcl);
#ifndef _WIN32
	if( !__sync_fetch_and_add (lock->readers, 1) )
#else
	if( !(_InterlockedIncrement16 (lock->readers)-1) )
#endif
		mutex_lock(lock->wrt);

	mutex_unlock(lock->xcl);
}

void readUnlock (RWLock *lock)
{
#ifndef _WIN32
	if( !__sync_sub_and_fetch (lock->readers, 1) )
#else
	if( !_InterlockedDecrement16 (lock->readers) )
#endif
		mutex_unlock(lock->wrt);
}
#endif

#ifdef LOCKTYPE3

void initLock(RWLock *latch) {
}

#ifndef FUTEX
void writeLock (RWLock *lock)
{
uint32_t spinCount = 0;
uint16_t w, r, tix;

#ifndef _WIN32
	tix = __sync_fetch_and_add (lock->ticket, 1);
#else
	tix = _InterlockedExchangeAdd16 (lock->ticket, 1);
#endif
	// wait for our ticket to come up

	while( tix != lock->serving[0] )
	  if (lock_spin(&spinCount))
	    lock_sleep (spinCount);

	//	add the writer present bit and tix phase

	spinCount = 0;
	w = PRES | (tix & PHID);
#ifndef _WIN32
	r = __sync_fetch_and_add (lock->rin, w);
#else
	r = _InterlockedExchangeAdd16 (lock->rin, w);
#endif

	while( r != *lock->rout )
	  if (lock_spin(&spinCount))
		lock_sleep (spinCount);
}

void writeUnlock (RWLock *lock)
{
#ifndef _WIN32
	__sync_fetch_and_and (lock->rin, ~MASK);
#else
	_InterlockedAnd16 (lock->rin, ~MASK);
#endif
	lock->serving[0]++;
}

void readLock (RWLock *lock)
{
uint32_t spinCount = 0;
uint16_t w;

#ifndef _WIN32
	w = __sync_fetch_and_add (lock->rin, RINC) & MASK;
#else
	w = _InterlockedExchangeAdd16 (lock->rin, RINC) & MASK;
#endif
	if( w )
	  while( w == (*lock->rin & MASK) )
	   if (lock_spin(&spinCount))
		lock_sleep (spinCount);
}

void readUnlock (RWLock *lock)
{
#ifndef _WIN32
	__sync_fetch_and_add (lock->rout, RINC);
#else
	_InterlockedExchangeAdd16 (lock->rout, RINC);
#endif
}
#endif
#endif

#ifdef LOCKTYPE4

//	Latch Manager -- pthreads locks

void initLock(RWLock *latch) {
#ifndef _WIN32
pthread_rwlockattr_t rwattr[1];

	pthread_rwlockattr_init (rwattr);
	pthread_rwlockattr_setpshared (rwattr, PTHREAD_PROCESS_SHARED);

	pthread_rwlock_init (latch->lock, rwattr);
	pthread_rwlockattr_destroy (rwattr);
#else
	InitializeSRWLock (latch->srw);
#endif
}

void readLock(RWLock *latch)
{
#ifndef _WIN32
	pthread_rwlock_rdlock (latch->lock);
#else
	AcquireSRWLockShared (latch->srw);
#endif
}

void readUnlock(RWLock *latch)
{
#ifndef _WIN32
	pthread_rwlock_unlock (latch->lock);
#else
	ReleaseSRWLockShared (latch->srw);
#endif
}

//	wait for other read and write latches to relinquish

void writeLock(RWLock *latch)
{
#ifndef _WIN32
	pthread_rwlock_wrlock (latch->lock);
#else
	AcquireSRWLockExclusive (latch->srw);
#endif
}

void writeUnlock(RWLock *latch)
{
#ifndef _WIN32
	pthread_rwlock_unlock (latch->lock);
#else
	ReleaseSRWLockExclusive (latch->srw);
#endif
}
#endif
