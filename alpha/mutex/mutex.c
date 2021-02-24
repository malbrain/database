//  Mutex, CAS, MCS and ticket lock implmentation

//  please report bugs located to the program author,
//  malbrain@cal.berkeley.edu

#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <time.h>

#include "mutex.h"

bool MutexAtomicCAS8(volatile char *dest, uint8_t value, uint8_t comp) {
#ifdef _WIN32
    return _InterlockedCompareExchange8 (dest, value, comp) == comp;
#else
    return __sync_bool_compare_and_swap (dest, comp, value);
#endif
}

#ifdef FUTEX
#include <linux/futex.h>
#include <limits.h>

#define SYS_futex 202
int sys_futex(void *addr1, int op, int val1, struct timespec *timeout, void *addr2, int val3)
{
	return syscall(SYS_futex, addr1, op, val1, timeout, addr2, val3);
}

#endif

#ifdef FUTEX
uint64_t FutexCnt[1];
#endif

int NanoCnt[1];

#ifndef _WIN32

void lock_sleep (int cnt) {
struct timespec ts[1];

	ts->tv_sec = 0;
	ts->tv_nsec = cnt;
	nanosleep(ts, NULL);
	__sync_fetch_and_add(NanoCnt, 1);
}

int lock_spin (uint32_t *cnt) {
int idx;

	if (!*cnt)
	  *cnt = 8;
	else if (*cnt < 8192)
	  *cnt += *cnt / 8;

	if (*cnt < 1024 )
	  for (idx = 0; idx < *cnt; idx++)
		pause();
	else
		return 1;

	return 0;
}

void mcs_lock (MCSMutex **lock, MCSMutex *qnode) {
uint32_t spinCount = 0;
MCSMutex *predecessor;

	qnode->next = NULL;
	*qnode->lock = 0;
#ifdef FUTEX
	qnode->futex = 0;
#endif

	predecessor = __sync_lock_test_and_set (lock, qnode);

	// MCS lock is idle

	if (!predecessor)
	  return;

	// turn on lock bit

	*qnode->lock = 1;
	predecessor->next = qnode;

	// wait for lock bit to go off

	while (*qnode->lock) {
	  if (lock_spin (&spinCount)) {
#ifndef FUTEX
		lock_sleep(spinCount);
#else
		qnode->futex = 1;
		sys_futex ((void *)qnode->bits, FUTEX_WAIT, 0x10001, NULL, NULL, 0);
  		__sync_fetch_and_add(FutexCnt, 1);
#endif
	  }
	}
}

void mcs_unlock (MCSMutex **lock, MCSMutex *qnode) {
uint32_t spinCount = 0;
MCSMutex *next;

	//	is there no next queue entry?

	if (!qnode->next) {
		if (__sync_bool_compare_and_swap (lock, qnode, 0LL))
			return;

		//  wait for next queue entry to install itself

		while (!qnode->next)
		  if (lock_spin (&spinCount))
		    lock_sleep (spinCount);
	}

	// turn off lock bit and wake up next queue entry

#ifdef _WIN32
	MemoryBarrier();
#else
	__sync_synchronize();
#endif
	next = (MCSMutex *)qnode->next;
	*next->lock = 0;

#ifdef FUTEX
	if (next->futex)
 		sys_futex( (void *)next->bits, FUTEX_WAKE, 1, NULL, NULL, 0);
#endif
}

void KM_lock(KMMutex *mutex) {
MutexState c, nxt =  LOCKED;
uint32_t spinCount = 0;

  while (!__sync_bool_compare_and_swap(mutex->state, FREE, nxt))
	while ((c = *mutex->state) != FREE)
	  if (lock_spin (&spinCount))
#ifndef FUTEX
		lock_sleep (spinCount);
#else
		{
		  if (c == LOCKED)
    		if (__sync_val_compare_and_swap(mutex->state, LOCKED, CONTESTED) == FREE)
			  break;

  		  __sync_fetch_and_add(FutexCnt, 1);
		  sys_futex((void *)mutex->state, FUTEX_WAIT, CONTESTED, NULL, NULL, 0);
		  nxt = CONTESTED;
		  break;
		}
#endif
}

void KM_unlock(KMMutex* mutex) {
#ifdef FUTEX
	if (__sync_fetch_and_sub(mutex->state, 1) == CONTESTED)  {
   		*mutex->state = FREE;
 		sys_futex( (void *)mutex->state, FUTEX_WAKE, 1, NULL, NULL, 0);
	}
#else
#ifdef _WIN32
	MemoryBarrier();
#else
	__sync_synchronize();
#endif
	*mutex->state = 0;
#endif
}

void CAS_lock(CASMutex* cas) {
uint32_t spinCount = 0;

  while (!MutexAtomicCAS8(cas->state, 1, 0))
	while (*cas->state & 1)
	  if (lock_spin(&spinCount))
		lock_sleep(spinCount);
}

void CAS_unlock(CASMutex* cas) {
#ifdef _WIN32
	MemoryBarrier();
#else
	__sync_synchronize();
#endif
	*cas->state = 0;
}

void ticket_lock(TicketMutex* ticket) {
uint32_t spinCount = 0;
uint16_t ours;

	ours = __sync_fetch_and_add(ticket->next, 1);

	while (ours != ticket->serving[0])
	  if (lock_spin (&spinCount))
		lock_sleep (spinCount);
}

void ticket_unlock(TicketMutex* ticket) {
#ifdef _WIN32
	MemoryBarrier();
#else
	__sync_synchronize();
#endif
	ticket->serving[0]++;
}

#else

void lock_sleep (int ticks) {
LARGE_INTEGER start[1], freq[1], next[1];
int idx, interval;
double conv;

	QueryPerformanceFrequency(freq);
	QueryPerformanceCounter(next);
	conv = (double)freq->QuadPart / 1000000000; 

	for (idx = 0; idx < ticks; idx += interval) {
		*start = *next;
		Sleep(0);
		QueryPerformanceCounter(next);
		interval = (int)((next->QuadPart - start->QuadPart) / conv);
	}

	_InterlockedIncrement(NanoCnt);
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

void mcs_lock (MCSMutex **lock, MCSMutex *qnode) {
MCSMutex *predecessor;

	qnode->next = NULL;

#	ifdef _WIN64
		predecessor = _InterlockedExchangePointer(lock, qnode);
#	else
		predecessor = (MCS *)_InterlockedExchange(lock, qnode);
#	endif

	// MCS lock is idle

	if (!predecessor)
	  return;

	// link qnode onto predecessor's wait chain

	predecessor->next = qnode;

	// wait for predecessor to signal us

	WaitForSingleObject(qnode->wait, INFINITE);
}

void mcs_unlock (MCSMutex **lock, MCSMutex *qnode) {
uint32_t spinCount = 0;
MCSMutex *next;

	//	is there no next queue entry?

	if (!qnode->next) {
#	  ifdef _WIN64
		if (_InterlockedCompareExchangePointer (lock, NULL, qnode) == qnode)
			return;
#	  else
		if ((MCSMutex *)_InterlockedCompareExchange (lock, NULL, qnode) == qnode)
			return;
#	  endif

		//  wait for next queue entry to install itself

		while (!qnode->next)
		  lock_spin (&spinCount);
	}

	// wake up next entry in wait chain

	next = (MCSMutex *)qnode->next;
	SetEvent(next->wait);
}

void KM_lock(MyMutex* mutex) {
uint32_t spinCount = 0;

  while (_InterlockedOr8(mutex->state, 1) & 1)
	while (*mutex->state & 1)
	  if (lock_spin(&spinCount))
		lock_sleep(spinCount);
}

void KM_unlock(MyMutex* mutex) {
	MemoryBarrier();
	*mutex->state = 0;
}

void CAS_lock(CASMutex* cas) {
uint32_t spinCount = 0;

  while (!MutexAtomicCAS8(cas->state, 1, 0))
	while (*cas->state & 1)
	  if (lock_spin(&spinCount))
		lock_sleep(spinCount);
}

void CAS_unlock(CASMutex* cas) {
	MemoryBarrier();
	*cas->state = 0;
}

#endif

#if HAVEMUTEX == 2

void ticket_lock(Ticket* ticket) {
uint32_t spinCount = 0;
uint16_t ours;

	ours = (uint16_t)_InterlockedIncrement16(ticket->next) - 1;

	while( ours != ticket->serving[0] )
	  if (lock_spin(&spinCount))
		lock_sleep(spinCount);
}

void ticket_unlock(Ticket* ticket) {
	_InterlockedIncrement16(ticket->serving);
}
#endif

#if HAVEMUTEX == 2
#endif

#ifdef STANDALONE
#ifdef unix
pthread_mutex_t sysmutex[1] = {PTHREAD_MUTEX_INITIALIZER};
unsigned char Array[256] __attribute__((aligned(64)));
Ticket ticket[1] __attribute__((aligned(64)));
KMMutex mutex[1] __attribute__((aligned(64)));
CAS cas[1] __attribute__((aligned(64)));
uint64_t FAA64[1]  __attribute__((aligned(64)));
uint32_t FAA32[1] __attribute__((aligned(64)));
uint16_t FAA16[1] __attribute__((aligned(64)));

#else
__declspec(align(64)) unsigned char Array[256];
__declspec(align(64)) Ticket ticket[1];
__declspec(align(64)) KMMutex mutex[1];
__declspec(align(64)) CAS cas[1];

__declspec(align(64)) uint64_t FAA64[1];
__declspec(align(64)) uint32_t FAA32[1];
__declspec(align(64)) uint16_t FAA16[1];

CRITICAL_SECTION sysmutex[1];
#endif

int ThreadCnt = 4;
MCS *mcs[1];

typedef enum {
	SystemType,
	MutexType,
	TicketType,
	MCSType,
	CASType,
	FAA64Type,
	FAA32Type,
	FAA16Type
} LockType;

#ifndef unix
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

typedef struct {
	LockType type;
	int idx;
} Param;

#ifdef unix
void *testit (void *arg) {
#else
UINT __stdcall testit (void *arg) {
#endif
Param *param = (Param *)arg;
uint64_t threadno = param->idx;
LockType type = param->type;
int idx, first, loop;
MCS qnode[1];

#ifdef _WIN32
#define faa_time(BITS, FCN) _InterlockedIncrement##FCN(FAA##BITS)


	qnode->wait = CreateEvent(NULL, FALSE, FALSE, NULL);
#else
#define faa_time(BITS,FCN) __sync_fetch_and_add(FAA##BITS, 1ULL) 
#endif
#ifdef DEBUG
fprintf(stderr, "thread %lld type %d\n", threadno, type);
#endif

  for (loop = 0; loop < 100000000/ThreadCnt; loop++) {
#ifdef DEBUG2
	  if (!(loop % (1000000 / ThreadCnt)))
		fprintf(stderr, "thread %lld loop %d\n", threadno, loop);
#endif
	  if (type == MutexType)
		mutex_lock(mutex);
	  else if (type == CASType)
		CAS_lock(cas);
	  else if (type == TicketType)
		ticket_lock(ticket);
	  else if (type == SystemType)
		system_lock(sysmutex);
	  else if (type == MCSType)
		  mcs_lock(mcs, qnode);
	  else if (type == FAA64Type)
		  faa_time(64,64);
	  else if (type == FAA32Type)
		  faa_time(32,);
	  else if (type == FAA16Type)
		  faa_time(16,16);

	  if (type > CASType)
		  continue;
#ifdef VALIDATE
	  first = Array[0];

	  for (idx = 0; idx < 255; idx++)
		Array[idx] = Array[idx + 1];

	  Array[255] = first;
#endif
	  if (type == MutexType)
		mutex_unlock(mutex);
	  else if (type == CASType)
		CAS_unlock(cas);
	  else if (type == TicketType)
		ticket_unlock(ticket);
	  else if (type == SystemType)
		system_unlock(sysmutex);
	  else if (type == MCSType)
		mcs_unlock(mcs, qnode);
	}

#ifdef _WIN32
	CloseHandle(qnode->wait);
#endif
#ifdef DEBUG
	fprintf(stderr, "thread %lld exiting\n", threadno);
#endif
	return 0;
}

int main (int argc, char **argv)
{
double start, elapsed;
LockType type = 0;
int idx;
#ifdef unix
pthread_t *threads;
#else
HANDLE *threads;
#endif

#ifdef _WIN32
	InitializeCriticalSection(sysmutex);
#endif
	start = getCpuTime(0);

	for (idx = 0; idx < 256; idx++)
		Array[idx] = idx;

	if (argc == 1) {
		fprintf(stderr, "Usage: %s #threads #type ...\n", argv[0]);
		fprintf(stderr, "0: System Type %zu bytes\n", sizeof(sysmutex));
		fprintf(stderr, "1: KMMutex Type %zu bytes\n", sizeof(KMMutex));
		fprintf(stderr, "2: Ticket Type %zu bytes\n", sizeof(Ticket));
		fprintf(stderr, "3: MCS Type %zu bytes\n", sizeof(MCS));
		fprintf(stderr, "4: CAS Type %zu bytes\n", sizeof(CAS));
		fprintf(stderr, "5: FAA64 type %zu bytes\n", sizeof(FAA64));
		fprintf(stderr, "6: FAA32 type %zu bytes\n", sizeof(FAA32));
		fprintf(stderr, "7: FAA16 type %zu bytes\n", sizeof(FAA16));
	}

	if (argc > 1)
		ThreadCnt = atoi(argv[1]);

#ifdef unix
	threads = malloc (ThreadCnt * sizeof(pthread_t));
#else
	threads = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, ThreadCnt * sizeof(HANDLE));
#endif

	for (idx = 0; idx < ThreadCnt; idx++) {
	  Param *param = calloc(0, sizeof(Param));

	  if (idx < argc - 2)
		type = atoi(argv[idx + 2]);

	  param->type = type;
	  param->idx = idx;
#ifdef unix
	  if( pthread_create (threads + idx, NULL, testit, (void *)param) )
		fprintf(stderr, "Unable to create thread %d, errno = %d\n", idx, (int)errno);
#else
	  do threads[idx] = (HANDLE)_beginthreadex(NULL, 131072, testit, (void *)param, 0, NULL);
	  while ((int64_t)threads[idx] == -1 && (SwitchToThread(), 1));
#endif
	}

	// 	wait for termination

#ifdef unix
	for( idx = 0; idx < ThreadCnt; idx++ )
		pthread_join (threads[idx], NULL);
#else
	for( idx = 0; idx < ThreadCnt; idx++ ) {
		WaitForSingleObject (threads[idx], INFINITE);
		CloseHandle(threads[idx]);
	}
#endif
#ifdef VALIDATE
	for( idx = 0; idx < 256; idx++)
	  if (Array[idx] != (unsigned char)(Array[(idx+1) % 256] - 1))
		fprintf (stderr, "Array out of order\n");
#endif
	elapsed = getCpuTime(0) - start;
	fprintf(stderr, " real %.0fns\n", elapsed * 10);
	elapsed = getCpuTime(1);
	fprintf(stderr, " user %.0fns\n", elapsed * 10);
	elapsed = getCpuTime(2);
	fprintf(stderr, " sys  %.0fns\n", elapsed * 10);
#ifdef FUTEX
	fprintf(stderr, " futex waits: %lld\n", FutexCnt[0]);
#endif
	fprintf(stderr, " nanosleeps %d\n", NanoCnt[0]);
}
#endif
