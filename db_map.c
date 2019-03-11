#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <intrin.h>
#else
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/file.h>
#include <errno.h>
#include <sched.h>

#ifdef apple
#include <libkern/OSAtomic.h>
#define pause() OSMemoryBarrier()
#else
#define pause() __asm __volatile("pause\n": : : "memory")
#endif
#endif

#include <inttypes.h>

#ifndef PRIx64
#define PRIx64 I64x
#endif

#include "db.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_map.h"

extern char *hndlPath;

void yield() {
#ifndef _WIN32
			pause();
#else
			YieldProcessor();
#endif
}

//	add next name to path

uint32_t addPath(char *path, uint32_t pathLen, char *name, uint32_t nameLen,  uint64_t ver) {
uint32_t len = 0, off = 12;
char buff[12];

	if (pathLen)
	  if( path[pathLen - 1] != '/')
		path[pathLen] = '.', len = 1;

	path += pathLen;

	if (pathLen + nameLen + len < MAX_path) {
		memcpy(path + len, name, nameLen);
		len += nameLen;
	}

	if (ver) {
	  do buff[--off] = ver % 10 + '0';
	  while (ver /= 10);

	  if (pathLen + len + 14 - off < MAX_path) {
	  	path[len++] = '-';
		memcpy (path + len, buff + off, 12 - off);
		len += 12 - off;
	  }
	} 

	path[len] = 0;
	return len;
}

//  assemble filename path

void getPath(DbMap *map, char *name, uint32_t nameLen, uint64_t ver) {
uint32_t len = 12, off = 0, prev;

	if (map->parent && map->parent->parent)
		len += prev = map->parent->pathLen, len++;
	else if (hndlPath)
		len += prev = (uint32_t)strlen(hndlPath), len++;
	else
		prev = 0;

	map->arenaPath = db_malloc(nameLen + len + 1, false);

	// 	don't propagate Catalog paths

	if (map->parent && map->parent->parent)
		memcpy (map->arenaPath, map->parent->arenaPath, prev);
	else if (hndlPath) {
		memcpy (map->arenaPath, hndlPath, prev);
		map->arenaPath[prev++] = '/';
	}

	off = prev;

	map->pathLen = off + addPath(map->arenaPath, off, name, nameLen, ver);
}

#ifdef _WIN32
HANDLE openPath(char *path, bool create) {
HANDLE hndl;
DWORD dispMode = create ? OPEN_ALWAYS : OPEN_EXISTING;
#else
int openPath(char *path, bool create) {
int hndl, flags = O_RDWR;
#endif

#ifdef _WIN32
	hndl = CreateFile(path, GENERIC_READ | GENERIC_WRITE | DELETE, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, dispMode, FILE_ATTRIBUTE_NORMAL |  FILE_FLAG_RANDOM_ACCESS, NULL);

	if (create && hndl == INVALID_HANDLE_VALUE) {
		fprintf(stderr, "Unable to create/open %s, error = %d\n", path, (int)GetLastError());
		return NULL;
	}

	return hndl;
#else

	if  (create)
	flags |= O_CREAT;

	hndl = open (path, flags, 0664);

	if (create && hndl == -1) {
		fprintf (stderr, "Unable to open/create %s, error = %d", path, errno);
		return -1;
	}

	return hndl;
#endif
}

void waitZero(volatile uint8_t *zero) {
	while (*zero)
#ifndef _WIN32
			pause();
#else
			YieldProcessor();
#endif
}

void waitZero32(volatile uint32_t *zero) {
	while (*zero)
#ifndef _WIN32
			pause();
#else
			YieldProcessor();
#endif
}

void waitZero64(volatile uint64_t *zero) {
	while (*zero)
#ifndef _WIN32
			pause();
#else
			YieldProcessor();
#endif
}

void waitNonZero(volatile uint8_t *zero) {
	while (!*zero)
#ifndef _WIN32
			pause();
#else
			YieldProcessor();
#endif
}

void waitNonZero32(volatile uint32_t *zero) {
	while (!*zero)
#ifndef _WIN32
			pause();
#else
			YieldProcessor();
#endif
}

void waitNonZero64(volatile uint64_t *zero) {
	while (!*zero)
#ifndef _WIN32
			pause();
#else
			YieldProcessor();
#endif
}

void lockLatchGrp(volatile uint8_t *latch, uint8_t bitNo) {
	uint8_t mask = 1 << (bitNo % 8);

#ifndef _WIN32
	while (__sync_fetch_and_or(latch + bitNo / 8, mask) & mask) {
#else
	while (_InterlockedOr8(latch + bitNo / 8, mask) & mask) {
#endif
		do
#ifndef _WIN32
			pause();
#else
			YieldProcessor();
#endif
		while (latch[bitNo / 8] & mask);
	}
 }

void unlockLatchGrp(volatile uint8_t *latch, uint8_t bitNo) {
	uint8_t mask = 1 << (bitNo % 8);

#ifndef _WIN32
	__sync_fetch_and_and(latch + bitNo / 8, (uint8_t)~mask);
#else
	_InterlockedAnd8(latch + bitNo / 8, (uint8_t)~mask);
#endif
}

void lockAddr(volatile uint64_t* bits) {
#ifndef _WIN32
	while (__sync_fetch_and_or(bits, ADDR_MUTEX_SET) & ADDR_MUTEX_SET) {
#else
	while (_InterlockedOr64((volatile int64_t *)bits, ADDR_MUTEX_SET) & ADDR_MUTEX_SET) {
#endif
		do
#ifndef _WIN32
			pause();
#else
			YieldProcessor();
#endif
		while (*bits & ADDR_MUTEX_SET);
	}
}

void unlockAddr(volatile uint64_t* bits) {
	*bits = *bits & ~ADDR_MUTEX_SET;
}

uint8_t atomicAnd8(volatile uint8_t *value, uint8_t mask) {
#ifndef _WIN32
	return __sync_fetch_and_and(value, mask);
#else
	return _InterlockedAnd8( value, mask);
#endif
}

//	atomic install 8 bit value

bool atomicCAS8(uint8_t *dest, uint8_t comp, uint8_t newValue) {
#ifdef _WIN32
	return _InterlockedCompareExchange8 (dest, newValue, comp) == comp;
#else
	return __sync_bool_compare_and_swap (dest, comp, newValue);
#endif
}

//	atomic install 16 bit value

bool atomicCAS16(uint16_t *dest, uint16_t comp, uint16_t value) {
#ifdef _WIN32
	return _InterlockedCompareExchange16 (dest, value, comp) == comp;
#else
	return __sync_bool_compare_and_swap (dest, comp, value);
#endif
}

//	atomic install 32 bit value

bool atomicCAS32(uint32_t *dest, uint32_t comp, uint32_t value) {
#ifdef _WIN32
	return _InterlockedCompareExchange (dest, value, comp) == comp;
#else
	return __sync_bool_compare_and_swap (dest, comp, value);
#endif
}

//	atomic install 64 bit value

bool atomicCAS64(uint64_t *dest, uint64_t comp, uint64_t value) {
#ifdef _WIN32
	return _InterlockedCompareExchange64 (dest, value, comp) == comp;
#else
	return __sync_bool_compare_and_swap (dest, comp, value);
#endif
}

uint8_t atomicOr8(volatile uint8_t *value, uint8_t mask) {
#ifndef _WIN32
	return __sync_fetch_and_or(value, mask);
#else
	return _InterlockedOr8( value, mask);
#endif
}

uint64_t atomicAdd64(volatile uint64_t *value, int64_t amt) {
#ifndef _WIN32
	return __sync_add_and_fetch(value, amt);
#else
	return _InterlockedExchangeAdd64( value, amt) + amt;
#endif
}

uint32_t atomicAdd32(volatile uint32_t *value, int32_t amt) {
#ifndef _WIN32
	return __sync_add_and_fetch(value, amt);
#else
	return _InterlockedExchangeAdd( (volatile long *)value, amt) + amt;
#endif
}

uint16_t atomicAdd16(volatile uint16_t *value, int16_t amt) {
#ifndef _WIN32
	return __sync_add_and_fetch(value, amt);
#else
	return _InterlockedExchangeAdd16( (volatile short *)value, amt) + amt;
#endif
}

uint64_t atomicOr64(volatile uint64_t *value, uint64_t amt) {
#ifndef _WIN32
	return __sync_fetch_and_or (value, amt);
#else
	return _InterlockedOr64( value, amt);
#endif
}

uint32_t atomicOr32(volatile uint32_t *value, uint32_t amt) {
#ifndef _WIN32
	return __sync_fetch_and_or(value, amt);
#else
	return _InterlockedOr( (volatile long *)value, amt);
#endif
}

void *mapMemory (DbMap *map, uint64_t offset, uint64_t size, uint32_t segNo) {
void *mem;

#ifndef _WIN32
int flags = MAP_SHARED;

	if( map->hndl < 0 ) {
		flags |= MAP_ANON;
		offset = 0;
	}

	mem = mmap(NULL, size, PROT_READ | PROT_WRITE, flags, map->hndl, offset);

	if (mem == MAP_FAILED) {
		fprintf (stderr, "Unable to mmap %s, offset = %" PRIx64 ", size = %" PRIx64 ", error = %d", map->arenaPath, offset, size, errno);
		return NULL;
	}
#else
	if (map->hndl == INVALID_HANDLE_VALUE)
		return VirtualAlloc(NULL, size, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE);

	if (!(map->maphndl[segNo] = CreateFileMapping(map->hndl, NULL, PAGE_READWRITE, (DWORD)((offset + size) >> 32), (DWORD)(offset + size), NULL))) {
		fprintf (stderr, "Unable to CreateFileMapping %s, size = %" PRIx64 ", segment = %d error = %d\n", map->arenaPath, offset + size, segNo, (int)GetLastError());
		return NULL;
	}

	mem = MapViewOfFile(map->maphndl[segNo], FILE_MAP_WRITE, (DWORD)(offset >> 32), (DWORD)offset, size);

	if (!mem) {
		fprintf (stderr, "Unable to MapViewOfFile %s, offset = %" PRIx64 ", size = %" PRIx64 ", error = %d\n", map->arenaPath, offset, size, (int)GetLastError());
		return NULL;
	}
#endif

	return mem;
}

void unmapSeg (DbMap *map, uint32_t segNo) {
#ifndef _WIN32
	munmap(map->base[segNo], map->arena->segs[segNo].size);
#else
	if (map->arenaDef && !map->arenaDef->params[OnDisk].boolVal) {
		VirtualFree(map->base[segNo], 0, MEM_RELEASE);
		return;
	}

	UnmapViewOfFile(map->base[segNo]);
	CloseHandle(map->maphndl[segNo]);
#endif
}

uint64_t compareAndSwap(uint64_t* target, uint64_t compareVal, uint64_t swapVal) {
#ifndef _WIN32
	return __sync_val_compare_and_swap(target, compareVal, swapVal);
#else
	return _InterlockedCompareExchange64((volatile __int64*)target, swapVal, compareVal);
#endif
}

uint64_t atomicExchange(uint64_t *target, uint64_t swapVal) {
#ifndef _WIN32
	return __sync_lock_test_and_set(target, swapVal);
#else
	return _InterlockedExchange64((volatile int64_t*)target, swapVal);
#endif
}

uint8_t atomicExchange8(volatile uint8_t *target, uint8_t swapVal) {
#ifndef _WIN32
	return __sync_lock_test_and_set(target, swapVal);
#else
	return _InterlockedExchange8(target, swapVal);
#endif
}

#ifdef _WIN32
void lockArena (HANDLE hndl, char *path) {
OVERLAPPED ovl[1];

	memset (ovl, 0, sizeof(ovl));
	ovl->OffsetHigh = 0x80000000;

	if (LockFileEx (hndl, LOCKFILE_EXCLUSIVE_LOCK, 0, sizeof(DbArena), 0, ovl))
		return;

	fprintf (stderr, "Unable to lock %s, error = %d", path, (int)GetLastError());
	exit(1);
}
#else
void lockArena (int hndl, char *path) {

	if (!flock(hndl, LOCK_EX))
		return;

	fprintf (stderr, "Unable to lock %s, error = %d", path, errno);
	exit(1);
}
#endif

#ifdef _WIN32
void unlockArena (HANDLE hndl, char *path) {
OVERLAPPED ovl[1];

	memset (ovl, 0, sizeof(ovl));
	ovl->OffsetHigh = 0x80000000;

	if (UnlockFileEx (hndl, 0, sizeof(DbArena), 0, ovl))
		return;

	fprintf (stderr, "Unable to unlock %s, error = %d", path, (int)GetLastError());
	exit(1);
}
#else
void unlockArena (int hndl, char *path) {
	if (!flock(hndl, LOCK_UN))
		return;

	fprintf (stderr, "Unable to unlock %s, error = %d", path, errno);
	exit(1);
}
#endif

bool fileExists(char *path) {
#ifdef _WIN32
	int attr = GetFileAttributes(path);

	if( attr == 0xffffffff)
		return false;

	if (attr & FILE_ATTRIBUTE_DIRECTORY)
		return false;

	return true;
#else
	return !access(path, F_OK);
#endif
}

//	close a map

void closeMap(DbMap *map) {
bool killIt = *map->arena->mutex & KILL_BIT;
#ifdef _WIN32
FILE_DISPOSITION_INFO dispInfo[1];
#endif

	while (map->numSeg)
		unmapSeg(map, --map->numSeg);

	if (map->parent)
		atomicAdd32(map->parent->openCnt, -1);

	if (killIt) {
#ifdef _WIN32
		memset (dispInfo, 0, sizeof(dispInfo));
		dispInfo->DeleteFile = true;
		SetFileInformationByHandle (map->hndl, FileDispositionInfo, dispInfo, sizeof(dispInfo));
#else
		unlink(map->arenaPath);
#endif
	}

#ifdef _WIN32
	CloseHandle(map->hndl);
#else
	if( map->hndl < 0 )
		close(map->hndl);
#endif

	db_free(map);
}

//	delete a map by name

void deleteMap(char *path) {
DbMap map[1];

#ifdef _WIN32
	HANDLE hndl = openPath (path, false);

	if (hndl == INVALID_HANDLE_VALUE)
		return;

	lockArena(hndl, path);
#else
	int hndl = openPath(path, false);
	
	if (hndl < 0)
		return;
#endif
	memset (map, 0, sizeof(DbMap));
	map->hndl = hndl;

	if ((map->base[0] = mapMemory(map, 0, sizeof(DbArena), 0))) {
		map->arena = (DbArena *)map->base[0];
		atomicOr8((volatile uint8_t *)map->arena->mutex, KILL_BIT);
		unmapSeg (map, 0);
	}

	unlockArena(hndl, path);
#ifdef _WIN32
	CloseHandle(hndl);
	DeleteFile (path);
#else
	close(hndl);
	unlink(path);
#endif
}

//	open arena file and read segment zero

int readSegZero(DbMap *map, DbArena *segZero) {
#ifdef _WIN32
DWORD amt;
#else
int amt;
#endif

#ifdef _WIN32
	map->hndl = openPath (map->arenaPath, true);

	if (map->hndl == INVALID_HANDLE_VALUE)
		return -1;

	lockArena(map->hndl, map->arenaPath);

	if (!ReadFile(map->hndl, segZero, sizeof(DbArena), &amt, NULL)) {
		unlockArena(map->hndl, map->arenaPath);
		CloseHandle(map->hndl);
		return -1;
	}
#else
	map->hndl = openPath (map->arenaPath, true);

	if (map->hndl == -1)
		return -1;

	lockArena(map->hndl, map->arenaPath);

#ifdef DEBUG
	fprintf(stderr, "lockArena %s\n", map->arenaPath);
#endif
	// read first part of segment zero if it exists

	amt = pread(map->hndl, segZero, sizeof(DbArena), 0LL);

	if (amt < 0)
		unlockArena(map->hndl, map->arenaPath);
#endif

	return amt;
}

