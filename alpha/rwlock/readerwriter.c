//  a phase fair reader/writer lock implementation, version 2
//	by Karl Malbrain, malbrain@cal.berkeley.edu
//	20 JUN 2016

#include <stdlib.h>
#include <stdint.h>
#include <memory.h>
#include <errno.h>

#include "readerwriter.h"

#ifndef _WIN32
#include <sched.h>
#else
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <process.h>
#include <intrin.h>
#endif

int NanoCnt[1];
int lock_spin(uint32_t* cnt);
void lock_sleep(int cnt);

#ifdef NEEDMUTEX

#ifndef _WIN32
#define pause() asm volatile("pause\n": : : "memory")

void lock_sleep (int cnt) {
struct timespec ts[1];

	ts->tv_sec = 0;
	ts->tv_nsec = cnt;
	nanosleep(ts, NULL);
	__sync_fetch_and_add(NanoCnt, 1);
}

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
#else

void lock_sleep (int ticks) {
LARGE_INTEGER start[1], freq[1], next[1];
double conv, interval;
int idx;

	QueryPerformanceFrequency(freq);
	QueryPerformanceCounter(next);
	conv = (double)freq->QuadPart / 1000000000; 

	for (idx = 0; idx < ticks; idx += (int)interval) {
		*start = *next;
		Sleep(0);
		QueryPerformanceCounter(next);
		interval = (next->QuadPart - start->QuadPart) / conv;
	}

	_InterlockedIncrement(NanoCnt);
}

int lock_spin (uint32_t *cnt) {
uint32_t idx;

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

#ifndef _WIN32

void mutex_lock(KMMutex* mutex) {
uint32_t spinCount = 0;
uint32_t prev;

  while (__sync_lock_test_and_set(mutex->lock, 1))
	while (*mutex->lock)
	  if (lock_spin (&spinCount))
		lock_sleep(spinCount);
}

void mutex_unlock(KMMutex* mutex) {
#ifdef _WIN32
	MemoryBarrier();
#else
	__sync_synchronize();
#endif
	*mutex->lock = 0;
}
#else
void mutex_lock(KMMutex* mutex) {
uint32_t spinCount = 0;

  while (_InterlockedOr8(mutex->lock, 1))
	while (*mutex->lock)
	  if (lock_spin(&spinCount))
		lock_sleep(spinCount);
}

void mutex_unlock(KMMutex* mutex) {
#ifdef _WIN32
	MemoryBarrier();
#else
	__sync_synchronize();
#endif
	*mutex->lock = 0;
}
#endif
#endif

//	simple Phase-Fair FIFO rwlock

#if RWTYPE == 1

void initLock(RWLock* lock) {
	memset(lock, 0, sizeof(*lock));
}

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
#ifdef _WIN32
	MemoryBarrier();
#else
	__sync_synchronize();
#endif
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
	*prev->bits = _InterlockedExchangeAdd(lock->requests->bits, RDINCR);
# endif
	
	while (*lock->completions->writer != *prev->writer)
	  if (lock_spin(&spinCount))
		lock_sleep(spinCount);
}

void readUnlock (RWLock *lock)
{
#ifdef _WIN32
	MemoryBarrier();
#else
	__sync_synchronize();
#endif
# ifndef _WIN32
	__sync_fetch_and_add (lock->completions->reader, 1);
# else
	_InterlockedExchangeAdd16(lock->completions->reader, 1);
# endif
}
#endif

//	reader/writer lock implementation
//	mutex based

#if RWTYPE == 2

void initLock(RWLock* lock) {
	memset(lock, 0, sizeof(*lock));
}

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

	if (lock->readers[0]++ == 0)
			mutex_lock(lock->wrt);

	mutex_unlock(lock->xcl);
}

void readUnlock (RWLock *lock)
{
	mutex_lock(lock->xcl);

	if(--lock->readers[0] == 0)
		mutex_unlock(lock->wrt);

	mutex_unlock(lock->xcl);
}
#endif

#if RWTYPE == 3

void initLock(RWLock* lock) {
	memset((void *)lock, 0, sizeof(*lock));
}

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
#ifndef  _WIN32
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
#ifdef _WIN32
	MemoryBarrier();
#else
	__sync_synchronize();
#endif
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
#ifdef _WIN32
	MemoryBarrier();
#else
	__sync_synchronize();
#endif
#ifndef _WIN32
	__sync_fetch_and_add (lock->rout, RINC);
#else
	_InterlockedExchangeAdd16 (lock->rout, RINC);
#endif
}
#endif

#ifdef STANDALONE
#include <stdio.h>

#ifdef _WIN32
double getCpuTime(int type)
{
FILETIME crtime[1];
FILETIME xittime[1];
FILETIME systime[1];
FILETIME usrtime[1];
SYSTEMTIME timeconv[1];
double ans = 0;

	memset (timeconv, 0, sizeof(SYSTEMTIME));

	switch( type ) {
	case 0:
		GetSystemTimeAsFileTime (xittime);
		FileTimeToSystemTime (xittime, timeconv);
		ans = (double)timeconv->wDayOfWeek * 3600 * 24;
		break;
	case 1:
		GetProcessTimes (GetCurrentProcess(), crtime, xittime, systime, usrtime);
		FileTimeToSystemTime (usrtime, timeconv);
		break;
	case 2:
		GetProcessTimes (GetCurrentProcess(), crtime, xittime, systime, usrtime);
		FileTimeToSystemTime (systime, timeconv);
		break;
	}

	ans += (double)timeconv->wHour * 3600;
	ans += (double)timeconv->wMinute * 60;
	ans += (double)timeconv->wSecond;
	ans += (double)timeconv->wMilliseconds / 1000;
	return ans;
}
#else
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
#endif

#ifndef _WIN32
unsigned char Array[256] __attribute__((aligned(64)));
#include <pthread.h>
pthread_rwlock_t lock0[1] = {PTHREAD_RWLOCK_INITIALIZER};
#else
__declspec(align(64)) unsigned char Array[256];
SRWLOCK lock0[1] = {SRWLOCK_INIT};
#endif

RWLock lock[1];

typedef struct {
	int threadCnt;
	int threadNo;
	int loops;
	int type;
	int work;
} Arg;

void work (int validate, int usecs, int shuffle) {
volatile int cnt = usecs * 300;
int first, idx;

  if (validate) {
	while(shuffle && usecs--) {
	  first = Array[0];
	  for (idx = 0; idx < 255; idx++)
		Array[idx] = Array[idx + 1];

	  Array[255] = first;
	}

	while (cnt--)
#ifndef _WIN32
		__sync_fetch_and_add(&usecs, 1);
#else
		_InterlockedIncrement(&usecs);
#endif
  }
}

#ifdef _WIN32
UINT __stdcall launch(void *vals) {
#else
void *launch(void *vals) {
#endif
Arg *arg = (Arg *)vals;
int idx;

	for( idx = 0; idx < arg->loops; idx++ ) {
#if RWTYPE == 0
#ifndef _WIN32
		pthread_rwlock_rdlock(lock), work(arg->work, 1, 0), pthread_rwlock_unlock(lock);
#else
		AcquireSRWLockShared(lock), work(arg->work, 1, 0), ReleaseSRWLockShared(lock);
#endif
#elif RWTYPE > 0
		readLock(lock), work(arg->work, 1, 0), readUnlock(lock);
		work(arg->work, 1,0);
#endif
	  if( (idx & 511) == 0)
#if RWTYPE == 0
#ifndef _WIN32
		  pthread_rwlock_wrlock(lock0), work(arg->work, 10, 1), pthread_rwlock_unlock(lock0);
#else
		  AcquireSRWLockExclusive(lock0), work(arg->work, 10, 1), ReleaseSRWLockExclusive(lock0);
#endif
#if RWTYPE > 0
		  writeLock(lock), work(arg->work, 10, 1), writeUnlock(lock);
#else
		  work(arg->work, 10,1);
#endif
#ifdef DEBUG
	  if (arg->type >= 0)
	   if (!(idx % 100000))
		fprintf(stderr, "Thread %d loop %d\n", arg->threadNo, idx);
#endif
	}

#ifdef DEBUG
	if (arg->type >= 0)
		fprintf(stderr, "Thread %d finished\n", arg->threadNo);
#endif
#ifndef _WIN32
	return NULL;
#else
	return 0;
#endif
}

int main (int argc, char **argv)
{
double start[3], elapsed, overhead[2][3];
int threadCnt, idx, phase;
Arg *args, base[1];

#ifndef WIN32
pthread_t *threads;
#else
DWORD thread_id[1];
HANDLE *threads;
#endif

	if (argc < 2) {
		fprintf(stderr, "Usage: %s #thrds lockType\n", argv[0]); 
		printf("sizeof RWLock: %d\n", (int)sizeof(lock));

		threadCnt = 1;
		LockType = 1;
	}
	else
	{
		threadCnt = atoi(argv[1]);
		LockType = atoi(argv[2]);
	}

	//	calculate non-lock timing

  for(phase = 0; phase < 2; phase++) {
	base->loops = 1000000 / threadCnt;;
	base->threadCnt = 1;
	base->threadNo = 0;
	base->type = -1;
	base->work = phase;

	start[0] = getCpuTime(0);
	start[1] = getCpuTime(1);
	start[2] = getCpuTime(2);

	launch(base);

	overhead[phase][0] = getCpuTime(0) - start[0];
	overhead[phase][1] = getCpuTime(1) - start[1];
	overhead[phase][2] = getCpuTime(2) - start[2];
  }

#ifndef _WIN32
	threads = malloc(threadCnt * sizeof(pthread_t));
#else
	threads = GlobalAlloc(GMEM_FIXED | GMEM_ZEROINIT, threadCnt * sizeof(HANDLE));
#endif
	args = calloc(threadCnt, sizeof(Arg));

  for(phase = 0; phase < 2; phase++) {
	start[0] = getCpuTime(0);
	start[1] = getCpuTime(1);
	start[2] = getCpuTime(2);

	for (idx = 0; idx < 256; idx++)
		Array[idx] = idx;

 	for (idx = 0; idx < threadCnt; idx++) {
	  args[idx].loops = 1000000 / threadCnt;
	  args[idx].threadCnt = threadCnt;
	  args[idx].threadNo = idx;
	  args[idx].type = LockType;
	  args[idx].work = phase;

#ifdef _WIN32
	  do threads[idx] = (HANDLE)_beginthreadex(NULL, 131072, launch, (void *)(args + idx), 0, NULL);
	  while ((int64_t)threads[idx] == -1 && (SwitchToThread(), 1));
#else
	  if (pthread_create(threads + idx, NULL, launch, (void *)(args + idx)))
		fprintf(stderr, "Unable to create thread %d, errno = %d\n", idx, errno);
#endif
	}

	// 	wait for termination

#ifndef _WIN32
	for (idx = 0; idx < threadCnt; idx++)
		pthread_join (threads[idx], NULL);
#else
	for (idx = 0; idx < threadCnt; idx++) {
		WaitForSingleObject (threads[idx], INFINITE);
		CloseHandle(threads[idx]);
	}
#endif

	if (phase)
	 for( idx = 0; idx < 256; idx++)
	  if (Array[idx] != (unsigned char)(Array[(idx+1) % 256] - 1))
		fprintf (stderr, "Array out of order\n");

	if (!phase)
		fprintf(stderr, "\nrwlock time/lock: \n");
	else
		fprintf(stderr, "\nrwlock moderate load: \n");

	elapsed = getCpuTime(0) - start[0];
	elapsed -= overhead[phase][0];

	if (elapsed < 0)
		elapsed = 0;

	fprintf(stderr, " real %.3fus\n", elapsed);

	elapsed = getCpuTime(1) - start[1];
	elapsed -= overhead[phase][1];

	if (elapsed < 0)
		elapsed = 0;

	fprintf(stderr, " user %.3fus\n", elapsed);

	elapsed = getCpuTime(2) - start[2];
	elapsed -= overhead[phase][2];

	if (elapsed < 0)
		elapsed = 0;

	fprintf(stderr, " sys  %.3fus\n", elapsed);
	fprintf(stderr, " nanosleeps %d\n", NanoCnt[0]);
  }
}
#endif
#endif