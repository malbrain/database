#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
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
			Yield();
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

	if (map->parent)
		len += prev = map->parent->pathLen, len++;
	else if (hndlPath)
		len += prev = strlen(hndlPath), len++;
	else
		prev = 0;

	map->arenaPath = db_malloc(len + 1, false);

	if (map->parent)
		memcpy (map->arenaPath, map->parent->arenaPath, prev);
	else if (hndlPath) {
		memcpy (map->arenaPath, hndlPath, prev);
		map->arenaPath[prev++] = '/';
	}

	off = prev;

	map->pathLen = off + addPath(map->arenaPath, off, name, nameLen, ver);
}

#ifdef _WIN32
HANDLE openPath(char *path) {
HANDLE hndl;
#else
int openPath(char *path) {
int hndl, flags;
#endif

#ifdef _WIN32
	hndl = CreateFile(path, GENERIC_READ | GENERIC_WRITE | DELETE, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL |  FILE_FLAG_RANDOM_ACCESS, NULL);

	if (hndl == INVALID_HANDLE_VALUE) {
		fprintf(stderr, "Unable to create/open %s, error = %d\n", path, (int)GetLastError());
		return NULL;
	}

	return hndl;
#else

	flags = O_RDWR | O_CREAT;

	hndl = open (path, flags, 0664);

	if (hndl == -1) {
		fprintf (stderr, "Unable to open/create %s, error = %d", path, errno);
		return -1;
	}

	return hndl;
#endif
}

void waitZero(volatile char *zero) {
	while (*zero)
#ifndef _WIN32
			pause();
#else
			Yield();
#endif
}

void waitZero32(volatile int32_t *zero) {
	while (*zero)
#ifndef _WIN32
			pause();
#else
			Yield();
#endif
}

void waitZero64(volatile int64_t *zero) {
	while (*zero)
#ifndef _WIN32
			pause();
#else
			Yield();
#endif
}

void waitNonZero(volatile char *zero) {
	while (!*zero)
#ifndef _WIN32
			pause();
#else
			Yield();
#endif
}

void waitNonZero32(volatile int32_t *zero) {
	while (!*zero)
#ifndef _WIN32
			pause();
#else
			Yield();
#endif
}

void waitNonZero64(volatile int64_t *zero) {
	while (!*zero)
#ifndef _WIN32
			pause();
#else
			Yield();
#endif
}

void lockLatch(volatile char* latch) {
#ifndef _WIN32
	while (__sync_fetch_and_or(latch, MUTEX_BIT) & MUTEX_BIT) {
#else
	while (_InterlockedOr8(latch, MUTEX_BIT) & MUTEX_BIT) {
#endif
		do
#ifndef _WIN32
			pause();
#else
			Yield();
#endif
		while (*latch & MUTEX_BIT);
	}
}

void unlockLatch(volatile char* latch) {
	*latch &= ~MUTEX_BIT;
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
			Yield();
#endif
		while (*bits & ADDR_MUTEX_SET);
	}
}

void unlockAddr(volatile uint64_t* bits) {
	*bits = *bits & ~ADDR_MUTEX_SET;
}

int8_t atomicOr8(volatile char *value, char amt) {
#ifndef _WIN32
	return __sync_fetch_and_or(value, amt);
#else
	return _InterlockedOr8( value, amt);
#endif
}

int64_t atomicAdd64(volatile int64_t *value, int64_t amt) {
#ifndef _WIN32
	return __sync_add_and_fetch(value, amt);
#else
	return _InterlockedAdd64( value, amt);
#endif
}

int32_t atomicAdd32(volatile int32_t *value, int32_t amt) {
#ifndef _WIN32
	return __sync_add_and_fetch(value, amt);
#else
	return _InterlockedAdd( (volatile long *)value, amt);
#endif
}

int64_t atomicOr64(volatile int64_t *value, int64_t amt) {
#ifndef _WIN32
	return __sync_fetch_and_or (value, amt);
#else
	return _InterlockedOr64( value, amt);
#endif
}

int32_t atomicOr32(volatile int32_t *value, int32_t amt) {
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
		flags |= MAP_ANONYMOUS;
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

	mem = MapViewOfFile(map->maphndl[segNo], FILE_MAP_WRITE, offset >> 32, offset, size);

	if (!mem) {
		fprintf (stderr, "Unable to MapViewOfFile %s, offset = %" PRIx64 ", size = %" PRIx64 ", error = %d\n", map->arenaPath, offset, size, (int)GetLastError());
		return NULL;
	}
#endif

	return mem;
}

void unmapSeg (DbMap *map, uint32_t segNo) {
char *base = segNo ? map->base[segNo] : 0ULL;

#ifndef _WIN32
	munmap(base, map->arena->segs[segNo].size);
#else
	if (!map->arenaDef->params[OnDisk].boolVal) {
		VirtualFree(base, 0, MEM_RELEASE);
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
	return _InterlockedExchange64((volatile __int64*)target, swapVal);
#endif
}


#ifdef _WIN32
void lockArena (DbMap *map) {
OVERLAPPED ovl[1];

	memset (ovl, 0, sizeof(ovl));
	ovl->OffsetHigh = 0x80000000;

	if (LockFileEx (map->hndl, LOCKFILE_EXCLUSIVE_LOCK, 0, sizeof(DbArena), 0, ovl))
		return;

	fprintf (stderr, "Unable to lock %s, error = %d", map->arenaPath, (int)GetLastError());
	exit(1);
}
#else
void lockArena (DbMap *map) {

	if (!flock(map->hndl, LOCK_EX))
		return;

	fprintf (stderr, "Unable to lock %s, error = %d", map->arenaPath, errno);
	exit(1);
}
#endif

#ifdef _WIN32
void unlockArena (DbMap *map) {
OVERLAPPED ovl[1];

	memset (ovl, 0, sizeof(ovl));
	ovl->OffsetHigh = 0x80000000;

	if (UnlockFileEx (map->hndl, 0, sizeof(DbArena), 0, ovl))
		return;

	fprintf (stderr, "Unable to unlock %s, error = %d", map->arenaPath, (int)GetLastError());
	exit(1);
}
#else
void unlockArena (DbMap *map) {
	if (!flock(map->hndl, LOCK_UN))
		return;

	fprintf (stderr, "Unable to unlock %s, error = %d", map->arenaPath, errno);
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
#ifdef _WIN32
FILE_DISPOSITION_INFO dispInfo[1];
#endif

	while (map->numSeg)
		unmapSeg(map, --map->numSeg);

	if (map->parent)
		atomicAdd32(map->parent->openCnt, -1);

	if (*map->arenaDef->dead) {
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

//	delete a map

void deleteMap(char *path) {
#ifdef _WIN32
	DeleteFile (path);
#else
	unlink(path);
#endif
}


