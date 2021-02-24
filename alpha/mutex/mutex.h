#ifndef _MUTEX_H_
#define _MUTEX_H_

#include <stdint.h>
#include <stdbool.h>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <winbase.h>
#include <process.h>
#else
#include <pthread.h>
#include <sched.h>
#endif

#ifndef _WIN32
#ifdef apple
#include <libkern/OSAtomic.h>
#define pause() OSMemoryBarrier()
#else
#define pause() sched_yield()
#endif
#endif

#ifndef HAVEMUTEX
#define KM_lock mutex_lock
#define KM_unlock mutex_unlock
#define KMMutex MyMutex
#endif

#if HAVEMUTEX == 1
#define TINY_lock mutex_lock
#define TINY_unlock mutex_unlock
#define TINYMutex MyMutex
#endif

#if HAVEMUTEX == 2
#define Ticket_lock mutex_lock
#define Ticket_unlock mutex_unlock
#define TicketMutex MyMutex
#endif

#if HAVEMUTEX == 3
#define CAS_lock mutex_lock
#define CAS_unlock mutex_unlock
#define CASMutex MyMutex
#endif

#if HAVEMUTEX == 4
#define MCS_lock mutex_lock
#define MCS_unlock mutex_unlock
#define MCSMutex MyMutex
#endif

typedef enum {
	FREE = 0,
	LOCKED,
	CONTESTED
} MutexState;

typedef struct {
	volatile char state[1];
} CASMutex;

typedef struct {
	volatile char lock[1];
	volatile char state[1];
} KMMutex;

typedef struct {
	volatile char state[1];
} TINYMutex;

typedef struct {
	volatile uint16_t serving[1];
	volatile uint16_t next[1];
} TicketMutex;

typedef struct {
  volatile void * volatile next;
#ifdef _WIN32
  HANDLE wait;
#else
#ifdef FUTEX
  union {
	struct {
	  volatile uint16_t lock[1];
	  uint16_t futex;
	};
	uint32_t bits[1];
  };
#else
  volatile char lock[1];
#endif
#endif
} MCSMutex;

#endif

#ifndef HAVEMUTEX
void KM_lock(KMMutex* mutex);
void KM_unlock(KMMutex* mutex);
#endif

#if HAVEMUTEX == 1
#endif

#if HAVEMUTEX == 2
void Ticket_lock(Ticket_Mutex* mutex);
void Ticket_unlock(TicketMutex* mutex);
#endif

#if HAVEMUTEX == 3
void CAS_lock(CASMutex* mutex);
void CAS_unlock(CASMutex* mutex);
#endif

#if HAVEMUTEX == 4
void MCS_lock(MCSMutex* mutex);
void MCS_unlock(MCSMutex* mutex);
#endif
