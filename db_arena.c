#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <errno.h>
#endif

#include "db.h"
#include "db_map.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_frame.h"

bool mapSeg (DbMap *map, uint32_t currSeg);
void mapZero(DbMap *map, uint64_t size);
void mapAll (DbMap *map);

extern DbMap memMap[1];
DbAddr openMaps[1];

//	open map given database red/black rbEntry

DbMap *arenaRbMap(DbMap *parent, RedBlack *rbEntry) {
ArenaDef *arenaDef = (ArenaDef *)(rbEntry + 1);
void **childMap;
DbMap *map;

	if (*arenaDef->dead & KILL_BIT)
		return NULL;

	//	see if we've opened this child
	//	map in this process already

	childMap = arrayElement(memMap, openMaps, arenaDef->mapIdx, sizeof(void *));

	if ((map = (DbMap *)*childMap))
		waitNonZero(map->arena->type);
	else {
		map = openMap(parent, rbkey(rbEntry), rbEntry->keyLen, arenaDef, &rbEntry->addr);
		atomicAdd32(parent->openCnt, 1);
		*childMap = map;
	}

	return map;
}

//  open/create an Object database/store/index arena file
//	call with writeLock(parent->childMaps->lock)

DbMap *openMap(DbMap *parent, char *name, uint32_t nameLen, ArenaDef *arenaDef, DbAddr *rbAddr) {
DbArena *segZero = NULL;
RedBlack *rbEntry;
DataBase *db;
DbMap *map;

#ifdef _WIN32
DWORD amt = 0;
#else
int32_t amt = 0;
#endif

	map = db_malloc(sizeof(DbMap) + arenaDef->localSize, true);

	if ((map->parent = parent))
		map->db = parent->db;
	else
		map->db = map;

	if (!arenaDef->params[OnDisk].boolVal) {
#ifdef _WIN32
		map->hndl = INVALID_HANDLE_VALUE;
#else
		map->hndl = -1;
#endif
		return initArena(map, arenaDef, name, nameLen, rbAddr);
	}

	getPath(map, name, nameLen, arenaDef->ver);

	//	open the onDisk arena file

#ifdef _WIN32
	map->hndl = openPath (map->arenaPath);

	if (map->hndl == INVALID_HANDLE_VALUE) {
		db_free(map->arenaPath);
		db_free(map);
		return NULL;
	}

	lockArena(map);

	segZero = VirtualAlloc(NULL, sizeof(DbArena), MEM_COMMIT, PAGE_READWRITE);

	if (!ReadFile(map->hndl, segZero, sizeof(DbArena), &amt, NULL)) {
		fprintf (stderr, "Unable to read %lld bytes from %s, error = %d\n", sizeof(DbArena), map->arenaPath, errno);
		VirtualFree(segZero, 0, MEM_RELEASE);
		CloseHandle(map->hndl);
		db_free(map->arenaPath);
		db_free(map);
		return NULL;
	}
#else
	map->hndl = openPath (map->arenaPath);

	if (map->hndl == -1) {
		db_free(map->arenaPath);
		db_free(map);
		return NULL;
	}

	lockArena(map);

#ifdef DEBUG
	fprintf(stderr, "lockArena %s\n", map->arenaPath);
#endif
	// read first part of segment zero if it exists

	segZero = valloc(sizeof(DbArena));

	amt = pread(map->hndl, segZero, sizeof(DbArena), 0LL);

	if (amt < 0) {
		fprintf (stderr, "Unable to read %d bytes from %s, error = %d\n", (int)sizeof(DbArena), map->arenaPath, errno);
		db_free(map->arenaPath);
		unlockArena(map);
		close(map->hndl);
		free(segZero);
		db_free(map);
		return NULL;
	}
#endif
	//	did we create the arena?

	if (amt < sizeof(DbArena)) {
		if ((map = initArena(map, arenaDef, name, nameLen, rbAddr)))
			unlockArena(map);
#ifdef _WIN32
		VirtualFree(segZero, 0, MEM_RELEASE);
#else
		free(segZero);
#endif
		return map;
	}

	//  verify the arena shape

	if (segZero->baseSize != arenaDef->baseSize) {
		fprintf (stderr, "Arena baseSize:%d doesn't match:%d file: %s\n", segZero->baseSize, arenaDef->baseSize, map->arenaPath);
		unlockArena(map);
#ifdef _WIN32
		CloseHandle(map->hndl);
#else
		close(map->hndl);
#endif
		free(segZero);
		db_free(map);
		return NULL;
	}

	if (segZero->objSize != arenaDef->objSize) {
		fprintf (stderr, "Arena objSize:%d doesn't match:%d file: %s\n", segZero->objSize, arenaDef->objSize, map->arenaPath);
		unlockArena(map);
#ifdef _WIN32
		CloseHandle(map->hndl);
#else
		close(map->hndl);
#endif
		free(segZero);
		db_free(map);
		return NULL;
	}

	//  since segment zero exists,
	//	initialize the map
	//	and map seg zero

	initLock(map->childMaps->lock);
	assert(segZero->segs->size > 0);

	mapZero(map, segZero->segs->size);
#ifdef _WIN32
	VirtualFree(segZero, 0, MEM_RELEASE);
#else
	free(segZero);
#endif

	rbEntry = getObj(map->db, *map->arena->redblack);
	map->arenaDef = (ArenaDef *)(rbEntry + 1);
	unlockArena(map);

	// wait for initialization to finish

	waitNonZero(map->arena->type);
	return map;
}

//	finish creating new arena
//	call with arena locked

DbMap *initArena (DbMap *map, ArenaDef *arenaDef, char *name, uint32_t nameLen, DbAddr *rbAddr) {
uint64_t initSize = arenaDef->params[InitSize].intVal;
uint32_t segOffset;
uint32_t bits;

	segOffset = sizeof(DbArena) + arenaDef->baseSize;
	segOffset += 7;
	segOffset &= -8;

	if (initSize < segOffset)
		initSize = segOffset;

	if (initSize < MIN_segsize)
		initSize = MIN_segsize;

	initSize += 65535;
	initSize &= -65536;

#ifdef DEBUG
	if (map->parent)
		fprintf(stderr, "InitMap %s at %llu bytes\n", map->arenaPath, initSize);
#endif
#ifdef _WIN32
	_BitScanReverse((unsigned long *)&bits, initSize - 1);
	bits++;
#else
	bits = 32 - (__builtin_clz (initSize - 1));
#endif
	//  create initial segment on unix, windows will automatically do it

	initSize = 1ULL << bits;

#ifndef _WIN32
	if (map->hndl != -1)
	  if (ftruncate(map->hndl, initSize)) {
		fprintf (stderr, "Unable to initialize file %s, error = %d\n", map->arenaPath, errno);
		close(map->hndl);
		db_free(map);
		return NULL;
	  }
#endif

	//  initialize new arena segment zero

	assert(initSize > 0);

	mapZero(map, initSize);
	map->arena->segs[map->arena->currSeg].nextObject.offset = segOffset >> 3;
	map->arena->baseSize = arenaDef->baseSize;
	map->arena->objSize = arenaDef->objSize;
	map->arena->segs->size = initSize;
	map->arena->delTs = 1;

	//	are we creating a catalog or database?

	if (!map->parent) {
		RedBlack *rbEntry = rbNew(map, name, nameLen, sizeof(ArenaDef));
		map->arenaDef = (ArenaDef *)(rbEntry + 1);
		memcpy(map->arenaDef, arenaDef, sizeof(ArenaDef));
		map->arena->redblack->bits = rbEntry->addr.bits;
		initLock(map->arenaDef->idList->lock);
	} else {
		map->arena->redblack->bits = rbAddr->bits;
		map->arenaDef = arenaDef;
	}

	return map;
}

//  initialize arena segment zero

void mapZero(DbMap *map, uint64_t size) {

	assert(size > 0);

	map->arena = mapMemory (map, 0, size, 0);
	map->base[0] = (char *)map->arena;
	map->numSeg = 1;

	mapAll(map);
}

//  extend arena into new segment
//  return FALSE if out of memory

bool newSeg(DbMap *map, uint32_t minSize) {
uint64_t size = map->arena->segs[map->arena->currSeg].size;
uint64_t off = map->arena->segs[map->arena->currSeg].off;
uint32_t nextSeg = map->arena->currSeg + 1;
uint64_t nextSize;

	off += size;
	nextSize = off * 2;

	while (nextSize - off < minSize)
	 	if (nextSize - off <= MAX_segsize)
			nextSize += nextSize;
		else
			fprintf(stderr, "newSeg segment overrun: %d\n", minSize), exit(1);

	if (nextSize - off > MAX_segsize)
		nextSize = off - MAX_segsize;

#ifdef _WIN32
	assert(__popcnt64(nextSize) == 1);
#else
	assert(__builtin_popcountll(nextSize) == 1);
#endif

	map->arena->segs[nextSeg].off = off;
	map->arena->segs[nextSeg].size = nextSize - off;
	map->arena->segs[nextSeg].nextId.seg = nextSeg;
	map->arena->segs[nextSeg].nextObject.segment = nextSeg;
	map->arena->segs[nextSeg].nextObject.offset = nextSeg ? 0 : 1;

	//  extend the disk file, windows does this automatically

#ifndef _WIN32
	if (map->hndl != -1)
	  if (ftruncate(map->hndl, nextSize)) {
		fprintf (stderr, "Unable to extend file %s to %ULL, error = %d\n", map->arenaPath, nextSize, errno);
		return false;
	  }
#endif

	if (!mapSeg(map, nextSeg))
		return false;

	map->arena->currSeg = nextSeg;
	map->numSeg = nextSeg + 1;
	return true;
}

//  allocate an object from frame list
//  return 0 if out of memory.

uint64_t allocObj(DbMap* map, DbAddr *free, DbAddr *tail, int type, uint32_t size, bool zeroit ) {
uint32_t bits, amt;
DbAddr slot;

	size += 7;
	size &= -8;

	if (type < 0) {
#ifdef _WIN32
		_BitScanReverse((unsigned long *)&bits, size - 1);
		bits++;
#else
		bits = 32 - (__builtin_clz (size - 1));
#endif
		amt = size;
		type = bits * 2;
		size = 1 << bits;

		// implement half-bit sizing

		if (bits > 4 && amt <= 3 * size / 4)
			size -= size / 4;
		else
			type++;

		free += type;

		if (tail)
			tail += type;
	} else
		amt = size;

	lockLatch(free->latch);

	while (!(slot.bits = getNodeFromFrame(map, free))) {
	  if (!getNodeWait(map, free, tail))
		if (!initObjFrame(map, free, type, size)) {
			unlockLatch(free->latch);
			return 0;
		}
	}

	unlockLatch(free->latch);

	if (zeroit)
		memset (getObj(map, slot), 0, amt);

	*slot.latch = type;
	return slot.bits;
}

void freeBlk(DbMap *map, DbAddr addr) {
	addSlotToFrame(map, &map->arena->freeBlk[addr.type], NULL, addr.bits);
}

void freeId(DbMap *map, ObjId objId) {
	addSlotToFrame(map, &map->arena->freeBlk[ObjIdType], NULL, objId.bits);
}

uint64_t allocBlk(DbMap *map, uint32_t size, bool zeroit) {
	return allocObj(map, map->arena->freeBlk, NULL, -1, size, zeroit);
}

void mapAll (DbMap *map) {
	lockLatch(map->mapMutex);

	while (map->numSeg <= map->arena->currSeg)
		if (mapSeg (map, map->numSeg))
			map->numSeg++;
		else
			fprintf(stderr, "Unable to map segment %d on map %s\n", map->numSeg, map->arenaPath), exit(1);

	unlockLatch(map->mapMutex);
}

void* getObj(DbMap *map, DbAddr slot) {
	if (!slot.addr)
		fprintf (stderr, "Invalid zero DbAddr: %s\n", map->arenaPath), exit(1);

	//  catch up segment mappings

	if (slot.segment >= map->numSeg)
		mapAll(map);

	return map->base[slot.segment] + slot.offset * 8ULL;
}

//  allocate raw space in the current segment
//  or return 0 if out of memory.

uint64_t allocMap(DbMap *map, uint32_t size) {
uint64_t max, addr;

	lockLatch(map->arena->mutex);

	max = map->arena->segs[map->arena->currSeg].size
		  - map->arena->segs[map->arena->objSeg].nextId.index * map->arena->objSize;

	size += 7;
	size &= -8;

	// see if existing segment has space
	// otherwise allocate a new segment.

	if (map->arena->segs[map->arena->currSeg].nextObject.offset * 8ULL + size > max) {
		if (!newSeg(map, size)) {
			unlockLatch (map->arena->mutex);
			return 0;
		}
	}

	addr = map->arena->segs[map->arena->currSeg].nextObject.bits;
	map->arena->segs[map->arena->currSeg].nextObject.offset += size >> 3;
	unlockLatch(map->arena->mutex);
	return addr;
}

bool mapSeg (DbMap *map, uint32_t currSeg) {
uint64_t size = map->arena->segs[currSeg].size;
uint64_t off = map->arena->segs[currSeg].off;

	assert(size > 0);

	if ((map->base[currSeg] = mapMemory (map, off, size, currSeg)))
		return true;

	return false;
}

//	return pointer to Obj slot

void *fetchIdSlot (DbMap *map, ObjId objId) {
	if (!objId.index) {
		fprintf (stderr, "Invalid zero document index: %s\n", map->arenaPath);
		exit(1);
	}

	return map->base[objId.seg] + map->arena->segs[objId.seg].size - objId.index * map->arena->objSize;
}

//
// allocate next available object id
//

uint64_t allocObjId(DbMap *map, DbAddr *free, DbAddr *tail, uint16_t idx) {
ObjId objId;

	if (free)
		free += ObjIdType;

	if (tail)
		tail += ObjIdType;

	lockLatch(free->latch);

	// see if there is a free object in the free queue
	// otherwise create a new frame of new objects

	while (!(objId.bits = getNodeFromFrame(map, free))) {
		if (!getNodeWait(map, free, tail))
			if (!initObjIdFrame(map, free)) {
				unlockLatch(free->latch);
				return 0;
			}
	}

	objId.idx = idx;
	unlockLatch(free->latch);
	return objId.bits;
}
