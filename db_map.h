#pragma once

/**
 *	map allocations
 */

uint64_t allocMap(DbMap *map, uint32_t size);
uint64_t allocBlk(DbMap *map, uint32_t size, bool zeroit);
uint64_t allocObj(DbMap *map, DbAddr *free, DbAddr *tail, int type, uint32_t size, bool zeroit);
uint64_t allocObjId(DbMap *map, DbAddr *free, DbAddr *tail);

void *fetchIdSlot (DbMap *map, ObjId objId);
void *getObj(DbMap *map, DbAddr addr); 
void freeBlk(DbMap *map, DbAddr addr);
void freeId(DbMap *map, ObjId objId);

uint64_t getFreeFrame(DbMap *map);
uint64_t allocFrame(DbMap *map);

/**
 * spin latches
 */

void lockAddr(volatile uint64_t* bits);
void unlockAddr(volatile uint64_t* bits);
void lockLatch(volatile char* latch);
void unlockLatch(volatile char* latch);
void waitNonZero(volatile char *zero);
void waitNonZero32(volatile uint32_t *zero);
void waitNonZero64(volatile uint64_t *zero);
void waitZero(volatile char *zero);
void waitZero32(volatile uint32_t *zero);
void waitZero64(volatile uint64_t *zero);
void art_yield(void);

/**
 * atomic integer ops
 */

void kill_slot(volatile char* latch);

uint64_t atomicAdd64(volatile uint64_t *value, int64_t amt);
uint32_t atomicAdd32(volatile uint32_t *value, int32_t amt);
uint64_t atomicOr64(volatile uint64_t *value, uint64_t amt);
uint32_t atomicOr32(volatile uint32_t *value, uint32_t amt);
uint64_t atomicExchange(uint64_t *target, uint64_t value);
uint64_t compareAndSwap(uint64_t* target, uint64_t compare_val, uint64_t swap_val);
char atomicExchange8(volatile char *target, char value);
char atomicAnd8(volatile char *value, char mask);
char atomicOr8(volatile char *value, char mask);

int readSegZero(DbMap *map, DbArena *segZero);

void closeMap(DbMap *map);
void deleteMap(char *path);
bool fileExists(char *path);
void yield(void);

#ifdef _WIN32
void lockArena (HANDLE hndl, char *path);
void unlockArena (HANDLE hndl, char *path);
#else
void lockArena (int hndl, char *path);
void unlockArena (int hndl, char *path);
#endif
