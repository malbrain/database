//	standalone driver file for Timestamps
//	specify /D CLOCK or /D RDTSC on the compile (VS19)
#include "timestamps.h"
#include <stdlib.h>
#include <stdio.h>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <direct.h>
#include <process.h>
#include <windows.h>
#endif

double getCpuTime(int type);

extern uint64_t rdtscEpochs;
extern bool tsGo;

typedef struct {
  int idx;
  int count;
} TsArgs;

Timestamp *tsVector;
int maxTS = 8;

#ifndef _WIN32
void *clientGo(void *arg) {
#else
unsigned __stdcall clientGo(void *arg) {
#endif
TsArgs *args = arg;
uint64_t idx, dups = 0, count = 0, skipped = 0;;
uint16_t slot = timestampClnt(tsVector, maxTS);
Timestamp prev[1] = {0, 0};

printf("Begin client %d\n", args->idx);

for (idx = 0; idx < args->count; idx++) {
  timestampNext(tsVector, slot);

  if (timestampCmp(tsVector + slot, prev) < 0)
    count++;
  else if (timestampCmp(tsVector + slot, prev) == 0)
    dups++;
  else
    skipped++;

  prev->tsBits[0] = tsVector[slot].tsBits[0];
  prev->tsBits[1] = tsVector[slot].tsBits[1];
#ifdef _DEBUG
  if (idx &&    idx % 10000000 == 0) fprintf(stderr, "%d:%.9d ", args->idx, (int)idx);
#endif  // DEBUG
  }

  printf("client %d count = %" PRIu64 " Out of Order = %" PRIu64 " dups = %" PRIu64 "\n", args->idx, count, skipped, dups);
#ifndef _WIN32
  return NULL;
#else
  return 0;
#endif
}

#ifndef _WIN32
int main(int argc, char **argv) {
  pthread_t *threads;
  int err;
#else
int _cdecl main(int argc, char **argv) {
  HANDLE *threads;
#endif
  printf("size of Timestamp = %d, TsEpoch = %d pointer = %d\n", (int)sizeof(Timestamp), (int)sizeof(TsEpoch), (int)sizeof(argv));
  TsArgs *baseArgs, *args;
  uint64_t sum = 0, first = 0, nxt = 0;
  int idx;
  double startx1 = getCpuTime(0);
  double startx2 = getCpuTime(1);
  double startx3 = getCpuTime(2);
  double elapsed;

  struct timespec spec[1];
  int64_t start, end;

  clock_gettime(CLOCK_REALTIME, spec);
  start = spec->tv_nsec + spec->tv_sec * 1000000000;

  for (idx = 0; idx < 1000000; idx++) {
    time(&spec->tv_sec);
  }

  clock_gettime(CLOCK_REALTIME, spec);
  end = spec->tv_nsec + spec->tv_sec * 1000000000;

  printf("time() timing = %" PRIu64 "ns\n\n", (end - start) / idx);

  clock_gettime(CLOCK_REALTIME, spec);
  first = spec->tv_nsec + spec->tv_sec * 1000000000;

  do {
    clock_gettime(CLOCK_REALTIME, spec);
    nxt = spec->tv_nsec + spec->tv_sec * 1000000000;
  } while (first == nxt);

  clock_gettime(CLOCK_REALTIME, spec);
  start = spec->tv_nsec + spec->tv_sec * 1000000000;

  for (idx = 0; idx < 1000000; idx++) {
    clock_gettime(CLOCK_REALTIME, spec);
  }

  clock_gettime(CLOCK_REALTIME, spec);
  end = spec->tv_nsec + spec->tv_sec * 1000000000;

  printf("clock_gettime() timing = %" PRIu64 "ns, resolution = %" PRIu64 "\n",
         (end - start) / idx, nxt - first);

#ifdef RDTSC
  first = __rdtsc() + __rdtsc() + __rdtsc() + __rdtsc();
  first /= 4;

  do
    nxt = __rdtsc();
  while (first == nxt);

  clock_gettime(CLOCK_REALTIME, spec);
  start = spec->tv_nsec + spec->tv_sec * 1000000000;

  for (sum = idx = 0; idx < 1000000; idx++) {
    sum += __rdtsc();
  }

  clock_gettime(CLOCK_REALTIME, spec);
  end = spec->tv_nsec + spec->tv_sec * 1000000000;

  printf("__rdtsc() timing = %" PRIu64 "ns, first units = %" PRIu64
         " avg units = %d\n",
         (end - start) / idx, nxt - first, (int)(__rdtsc() - first) / idx);

  rdtscUnits = nxt - first;
  putchar('\n');
#endif

  if (argc > 1) maxTS = atoi(argv[1]) + 1;

  baseArgs = malloc(maxTS * sizeof(TsArgs));
#ifdef ALIGN
  tsVector = (Timestamp *)aligned_malloc(maxTS, sizeof(Timestamp));
  memset(tsVector, 0, maxTS * sizeof(Timestamp));
#else
  tsVector = (Timestamp *)calloc(maxTS, sizeof(Timestamp));
#endif
  timestampInit(tsVector, maxTS);

#ifndef _WIN32
  threads = malloc(maxTS * sizeof(pthread_t));
#else
  threads = GlobalAlloc(GMEM_FIXED | GMEM_ZEROINIT, maxTS * sizeof(HANDLE));
#endif
  //	fire off threads

  idx = 0;
  startx1 = getCpuTime(0);
  startx2 = getCpuTime(1);
  startx3 = getCpuTime(2);

  if (maxTS <= 2) {
    args = baseArgs;
    args->count = atoi(argv[2]);
    args->idx = 0;
    clientGo(args);
  } else  do {
    args = baseArgs + idx;
    args->count = atoi(argv[2]);
    args->idx = idx;
#ifndef _WIN32
	if( idx )
      if ((err = pthread_create(threads + idx, NULL, clientGo, args)))
        fprintf(stderr, "Error creating thread %d\n", err);
#else
    if( idx )
      while (((int64_t)(threads[idx] = (HANDLE)_beginthreadex(NULL, 65536, clientGo, args, 0, NULL)) < 0LL))
        fprintf(stderr, "Error creating thread errno = %d\n", errno);
#endif
	if( idx )
		printf("thread %d launched for %d timestamps\n", idx, atoi(argv[2]));

  } while (++idx < maxTS);

  // 	wait for termination

  if (idx) {
#ifndef _WIN32
    for (idx = 1; idx < maxTS; idx++) pthread_join(threads[idx], NULL);
#else
    WaitForMultipleObjects(maxTS - 1, threads + 1, TRUE, INFINITE);
    for (idx = 1; idx < maxTS; idx++) CloseHandle(threads[idx]);
#endif
  }
#ifdef ATOMIC
    printf("Atomic Incr\n");
#endif
#ifdef ALIGN
    printf("Atomic Aligned 64\n");
#endif
#ifdef RDTSC
    printf("\nTSC COUNT: New  Epochs = %" PRIu64 "\n", rdtscEpochs);
#endif
#ifdef CLOCK
    printf("Hi Res Timer\n");
#endif
    elapsed = getCpuTime(0) - startx1;
    printf(" real %dm%.3fs\n", (int)(elapsed / 60), elapsed - (int)(elapsed / 60) * 60);

    elapsed = getCpuTime(1) - startx2;
    printf (" user %dm%.3fs\n", (int)(elapsed / 60), elapsed - (int)(elapsed / 60) * 60);

    elapsed = getCpuTime(2) - startx3;
    printf(" sys  %dm%.3fs\n", (int)(elapsed / 60), elapsed - (int)(elapsed / 60) * 60);

    return 0;
}

#ifndef _WIN32
#include <stdlib.h>
#include <sys/resource.h>
#include <sys/time.h>

double getCpuTime(int type) {
  struct rusage used[1];
  struct timeval tv[1];

  switch (type) {
    case 0:
      gettimeofday(tv, NULL);
      return (double)tv->tv_sec + (double)tv->tv_usec / 1000000;

    case 1:
      getrusage(RUSAGE_SELF, used);
      return (double)used->ru_utime.tv_sec +
             (double)used->ru_utime.tv_usec / 1000000;

    case 2:
      getrusage(RUSAGE_SELF, used);
      return (double)used->ru_stime.tv_sec +
             (double)used->ru_stime.tv_usec / 1000000;
  }

  return 0;
}

#else

#define WIN32_LEAN_AND_MEAN
#include <process.h>
#include <windows.h>

double getCpuTime(int type) {
  FILETIME crtime[1];
  FILETIME xittime[1];
  FILETIME systime[1];
  FILETIME usrtime[1];
  SYSTEMTIME timeconv[1];
  double ans = 0;

  memset(timeconv, 0, sizeof(SYSTEMTIME));

  switch (type) {
    case 0:
      GetSystemTimeAsFileTime(xittime);
      FileTimeToSystemTime(xittime, timeconv);
      ans = (double)timeconv->wDayOfWeek * 3600 * 24;
      break;
    case 1:
      GetProcessTimes(GetCurrentProcess(), crtime, xittime, systime, usrtime);
      FileTimeToSystemTime(usrtime, timeconv);
      break;
    case 2:
      GetProcessTimes(GetCurrentProcess(), crtime, xittime, systime, usrtime);
      FileTimeToSystemTime(systime, timeconv);
      break;
  }

  ans += (double)timeconv->wHour * 3600;
  ans += (double)timeconv->wMinute * 60;
  ans += (double)timeconv->wSecond;
  ans += (double)timeconv->wMilliseconds / 1000;
  return ans;
}

#endif
