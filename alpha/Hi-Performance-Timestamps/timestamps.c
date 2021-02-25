//  Hi Performance timestamp generator.
//  Multi-process/multi-threaded clients
//  a mmap common data structure shared by 
//  processes

//  Clients are given one of the client array slots of timestamps.
//  Client Requests for the next timestamp are made from and delivered into the assigned
//  client array slot.

//  The some server flavors use slot 0 to store the last timestamp assigned as the basis for the next request.

#include "timestamps.h"
#include <stdio.h>
#include <inttypes.h>
#include <stdint.h>

//  store the initialization values

uint64_t rdtscEpochs = 0;
extern bool debug;

void timestampLock(uint8_t *latch) {
#ifndef _WIN32
  while (__sync_fetch_and_or(latch, 1) & 1) {
#else
  while (_InterlockedOr8((volatile int8_t *)latch, 1) & 1) {
#endif
    do
#ifndef _WIN32
      pause();
#else
      YieldProcessor();
#endif
    while (*latch & 1);
  }
}

void timestampUnlock(volatile uint8_t *latch) {
#ifdef _WIN32
  MemoryBarrier();
#else
  __sync_synchronize();
#endif
  *latch = 0;
}

#ifdef _WIN32
__declspec(align(16)) volatile TsEpoch rdtscEpoch[1];
#else
volatile TsEpoch rdtscEpoch[1] __attribute__((__aligned__(16)));
#endif

//	atomic install checked 64 bit value

static bool atomicCAS64(volatile uint64_t *dest, uint64_t *comp, uint64_t *value) {
#ifdef _WIN32
  return _InterlockedCompareExchange64(dest, *value, *comp) == *comp;
}
#else
  return __atomic_compare_exchange(dest, comp, value, false, __ATOMIC_RELEASE, __ATOMIC_RELAXED );
}
#endif

//	atomic install checked 8 bit value

static bool atomicCAS8(volatile uint8_t *dest, uint8_t *comp,
                        uint8_t *value) {
#ifdef _WIN32
  return _InterlockedCompareExchange8(dest, *value, *comp) == *comp;
}
#else
  return __atomic_compare_exchange(dest, comp, value, false, __ATOMIC_RELEASE,
                                   __ATOMIC_RELAXED);
}
#endif

//	atomic install checked 16 bit value

static bool atomicCAS16(volatile uint16_t *dest, uint16_t *comp, uint16_t *value) {
#ifdef _WIN32
  return _InterlockedCompareExchange16(dest, *value, *comp) == *comp;
}
#else
  return __atomic_compare_exchange(dest, comp, value, false, __ATOMIC_RELEASE,
                                   __ATOMIC_RELAXED);
}
#endif

//	atomic 64 bit increment

uint64_t atomicINC64(volatile uint64_t *dest) {
#ifdef _WIN32
  return _InterlockedIncrement64(dest);
#else
  return __sync_add_and_fetch(dest, 1);
#endif
}

//	atomic install checked TsEpoch value

bool atomicCASEpoch(volatile TsEpoch *what, TsEpoch *comp, TsEpoch *repl) {
#ifdef _WIN32
  return atomicCAS128(what->bitsX2, comp->bitsX2, repl->bitsX2);
#else
  return atomicCAS128(what->bits, comp->bits, repl->bits);
#endif
}

//  atomic install checked 128 bit value

#ifdef _WIN32
bool atomicCAS128(volatile uint64_t * what, uint64_t * comp, uint64_t * repl) {
  return _InterlockedCompareExchange128(what, repl[1], repl[0], comp);
}
#else
bool atomicCAS128(volatile __int128 *what, __int128 *comp, __int128 *repl) {
  return __atomic_compare_exchange(what, comp, repl, false,
                                   __ATOMIC_RELEASE, __ATOMIC_RELAXED);
}
#endif

//  routine to wait

bool pausey(int loops) {
  if (loops < 20) return tsGo;

  pausex();
  return tsGo;
}

//	tsMaxClients is the number of client slots plus one for slot zero
// for flavor ALIGN, place tsBase on 64 byte alignment

uint64_t timestampInit(Timestamp *tsBase, int tsMaxClients) {
#ifdef RDTSC
struct timespec spec[1];
uint64_t sum = 0, first = 0, nxt = 0;
int64_t start, end;
int idx;

  clock_gettime(CLOCK_REALTIME, spec);
  rdtscEpoch->tod[0] = spec->tv_sec;
  rdtscEpoch->base = __rdtsc() - (1000000000ULL - spec->tv_nsec);

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

  if(debug)
    printf("__rdtsc() timing = %" PRIu64 "ns, first units = %" PRIu64
         " avg units = %d\n",
         (end - start) / idx, nxt - first, (int)(__rdtsc() - first) / idx);

  if(!(rdtscUnits = nxt - first))
    rdtscUnits = 1;

  return rdtscUnits;
#endif
}

//  Client request for tsBase slot

uint16_t timestampClnt(Timestamp *tsBase, int maxClient) {
  uint8_t tsAvail[1] = {TSAvail};
  uint8_t tsIdle[1] = {TSIdle};
  int idx = 0;

  while (++idx < maxClient)
	if( tsBase[idx].tsCmd == TSAvail )
	  if (atomicCAS8(&tsBase[idx].tsCmd, tsAvail, tsIdle)) 
		  return idx;

  return 0;
}

//	release tsBase slot

void timestampQuit(Timestamp *tsBase, uint16_t idx) {

  tsBase[idx].tsCmd = TSAvail;
}

//  request next timestamp

void timestampNext(Timestamp *tsBase, uint16_t idx) {
  Timestamp prev[1];

  prev[0].lowHi[0] = tsBase[idx].lowHi[0];
  prev[0].lowHi[1] = tsBase[idx].lowHi[1];
#ifdef CLOCK
  struct timespec spec[1];
  do {
    clock_gettime(CLOCK_REALTIME, spec);

    tsBase[idx].tsEpoch = spec->tv_sec;
    tsBase[idx].tsSeqCnt = spec->tv_nsec;
    tsBase[idx].tsIdx = idx;

  } while (timestampCmp(prev, tsBase + idx) == 0);

  return;
#endif
#ifdef RDTSC
#ifdef _WIN32
  __declspec(align(16)) TsEpoch newEpoch[1];
  __declspec(align(16)) TsEpoch oldEpoch[1];
#else
  TsEpoch newEpoch[1] __attribute__((__aligned__(16)));
  TsEpoch oldEpoch[1] __attribute__((__aligned__(16)));
#endif
#if defined(WSL) || defined(_WIN32)
  uint64_t maxRange = 1000000000;
  struct timespec spec[1];
  uint64_t ts, range, units;
  bool once = true;
  time_t tod[1];

  do {
    do {
      ts = __rdtsc();
      *tod = *(volatile time_t *)rdtscEpoch->tod;
      range = ts - rdtscEpoch->base;
      units = range / rdtscUnits;

      if (range <= rdtscUnits) units = 1, printf("range underflow\n");

      // Skip down to assign Timestamp from current Epoch
      // guard against shredded load

      if (*tod != *(volatile time_t *)rdtscEpoch->tod) continue;

      if (units < maxRange) break;

      if (once) {
        atomicINC64(&rdtscEpochs);
        once = false;
      }

      oldEpoch->base = ts - range;
      oldEpoch->tod[0] = tod[0];

      clock_gettime(CLOCK_REALTIME, spec);

      // same epoch?

      if (spec->tv_sec == *tod)
        maxRange = 2000000000;
      else
        maxRange = 3000000000;

      newEpoch->base = ts - (maxRange - spec->tv_nsec);
      newEpoch->tod[0] = spec->tv_sec;

      //  Release new Epoch via atomicCAS128

      atomicCASEpoch(rdtscEpoch, oldEpoch, newEpoch);

    } while (true);

#elif defined(__linux__)
  uint32_t maxRange = 1000000000;
  uint64_t ts, range, units;
  time_t tod[1];
  bool once = true;

  do {
    do {
      ts = __rdtsc();
      *tod = rdtscEpoch->tod[0];
      range = ts - rdtscEpoch->base;
      units = range / rdtscUnits;

      if (time(NULL) == *tod)
        if (*tod == *(volatile time_t *)rdtscEpoch->tod && (units < maxRange))
          break;

      if (once) {
        atomicINC64(&rdtscEpochs);
        once = false;
      }

      oldEpoch->tod[0] = tod[0];
      oldEpoch->base = ts - range;

      newEpoch->base = ts;
      newEpoch->tod[0] = time(NULL);

      //  Release new Epoch via atomicCAS128

      atomicCAS128(rdtscEpoch, oldEpoch, newEpoch);
    } while (true);
#endif
    // emit assigned Timestamp.

    tsBase[idx].tsEpoch = tod[0];
    tsBase[idx].tsSeqCnt = (uint32_t)units;
  } while (timestampCmp(prev, tsBase + idx, 0, 0) <= 0);
  return;
#endif
#if defined(ATOMIC) || defined(ALIGN)
  tsBase[idx].lowHi[0] = atomicINC64(tsBase->lowHi);
#endif
}

//	install new timestamp if > (or <) existing value

void timestampCAX(Timestamp *dest, Timestamp *src, int chk, int lock, int unlock) {
  Timestamp cmp[1];

    switch (lock | 0x20) {
    case 'l':
      timestampLock(dest->tsLatch);
      break;
    case 'r':
      timestampLock(src->tsLatch);
      break;
    case 'b':
      timestampLock(dest->tsLatch);
      timestampLock(src->tsLatch);
      break;
  }
  do {
    cmp->lowHi[0] = dest->lowHi[0] & ~1ULL;
    cmp->lowHi[1] = dest->lowHi[1];

    if (chk < 0 ) {
      if( cmp->lowHi[1] > src->lowHi[1]) break;
      if (cmp->lowHi[1] == src->lowHi[1] && cmp->lowHi[0] > src->lowHi[0])
        break;
    }

    if (chk > 0 ) {
        if( cmp->lowHi[1] < src->lowHi[1]) break;
        if( cmp->lowHi[1] == src->lowHi[1] &&  cmp->lowHi[0] < src->lowHi[0]) break;
    }

#ifdef _WIN32
  } while (!_InterlockedCompareExchange128 (dest->tsBits, src->tsBits[1], src->tsBits[0], cmp->tsBits));
#else
  } while (!__atomic_compare_exchange(dest->tsBits, cmp->tsBits, src->tsBits, false,
                                      __ATOMIC_SEQ_CST, __ATOMIC_ACQUIRE));
#endif
  switch (unlock | 0x20) {
    case 'l':
      timestampUnlock(dest->tsLatch);
      break;
    case 'r':
      timestampUnlock(src->tsLatch);
      break;
    case 'b':
      timestampUnlock(dest->tsLatch);
      timestampUnlock(src->tsLatch);
      break;
  }
}

  //	install a timestamp value 

void timestampInstall(Timestamp *dest, Timestamp *src, char lock, char unlock) {
    switch (lock | 0x20) { 
    case 'd':
      timestampLock(dest->tsLatch);
      break;
    case 's':
      timestampLock(src->tsLatch);
      break;
    case 'b':
      timestampLock(dest->tsLatch);
      timestampLock(src->tsLatch);
      break;
    }
#ifdef _WIN32
  dest->tsBits[1] = src->tsBits[1];
  dest->tsBits[0] = src->tsBits[0];
#else
  dest->tsBits[0] = src->tsBits[0];
#endif
  switch (unlock | 0x20) {
    case 'd':
      timestampUnlock(dest->tsLatch);
      break;
    case 's':
      timestampUnlock(src->tsLatch);
      break;
    case 'b':
      timestampUnlock(dest->tsLatch);
      timestampUnlock(src->tsLatch);
      break;
  }
}

// compare timestamps

int64_t timestampCmp(Timestamp *ts1, Timestamp *ts2, char lock, char unlock) {
  int64_t comp;

  switch (lock | 0x20) {
    case 'l':
      timestampLock(ts1->tsLatch);
      break;

    case 'r':
      timestampLock(ts2->tsLatch);
      break;

    case 'b':
      timestampLock(ts1->tsLatch);
      timestampLock(ts2->tsLatch);
      break;
  }
  comp = ts2->lowHi[1] - ts1->lowHi[1];

  if(!comp)
    comp = ts2->lowHi[0] - ts1->lowHi[0];

  switch (unlock | 0x20) {
    case 'l':
        timestampUnlock(ts1->tsLatch);
        break;

    case 'r':
        timestampUnlock(ts2->tsLatch);
        break;

    case 'b':
        timestampUnlock(ts1->tsLatch);
        timestampUnlock(ts2->tsLatch);
        break;
    }
    return comp;
}
