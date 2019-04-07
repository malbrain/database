#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#ifndef apple
#define _XOPEN_SOURCE 600
#include <malloc.h>
#endif
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#endif

#include <inttypes.h>

#include "db.h"
#include "db_arena.h"
#include "db_map.h"
#include "db_object.h"
#include "db_frame.h"
#include "db_redblack.h"
#include "db_skiplist.h"

bool mapSeg (DbMap *map, uint32_t currSeg);
void mapZero(DbMap *map, uint64_t size);
void mapAll (DbMap *map);

uint64_t totalMemoryReq[1];

extern DbMap memMap[1];

//	open map given database red/black rbEntry

DbMap *arenaRbMap(DbMap *parent, RedBlack *rbEntry) {
ArenaDef *arenaDef = (ArenaDef *)(rbEntry + 1);
DbMap *map;

	if (*arenaDef->dead & KILL_BIT)
		return NULL;

	map = openMap(parent, rbkey(rbEntry), rbEntry->keyLen, arenaDef, rbEntry);
	return map;
}

//  open/create an Object database/store/index arena file

DbMap *openMap(DbMap *parent, char *name, uint32_t nameLen, ArenaDef *arenaDef, RedBlack *rbEntry) {
DbMap *map = NULL, **childMap = NULL;
DbArena *segZero = NULL;
SkipEntry *entry;
int amt;

	//	see if we've opened this
	//	map in this process already

	if (parent) {
	  lockLatch(parent->childMaps->latch);
	  entry = skipAdd(memMap, parent->childMaps, arenaDef->id);
	  childMap = (DbMap **)entry->val;

	  if ((map = *childMap)) {
		waitNonZero(map->type);
		atomicAdd32(parent->openCnt, 1);
		unlockLatch(parent->childMaps->latch);
		return map;
	  }
	}

	map = db_malloc(sizeof(DbMap) + arenaDef->mapXtra, true);

	if ((map->parent = parent)) {
		*childMap = map;
		unlockLatch(parent->childMaps->latch);
	}

	//	are we opening a database?
	//	a database is its own db

	if (parent && parent->parent)
		map->db = parent->db;
	else
		map->db = map;

	if (!arenaDef->params[OnDisk].boolVal) {
#ifdef _WIN32
		map->hndl = INVALID_HANDLE_VALUE;
#else
		map->hndl = -1;
#endif
		return initArena(map, arenaDef, name, nameLen, rbEntry);
	}

	getPath(map, name, nameLen, arenaDef->nxtVer);

	//	open the onDisk arena file
	//	and read segZero

#ifdef _WIN32
	segZero = VirtualAlloc(NULL, sizeof(DbArena), MEM_COMMIT, PAGE_READWRITE);
#else
	segZero = valloc(sizeof(DbArena));
#endif
	amt = readSegZero(map, segZero);

	if (amt < 0) {
		fprintf (stderr, "Unable to read %d bytes from %s, error = %d\n", (int)sizeof(DbArena), map->arenaPath, errno);
#ifdef _WIN32
		VirtualFree(segZero, 0, MEM_RELEASE);
#else
		free(segZero);
#endif
		db_free(map->arenaPath);
		db_free(map);
		return NULL;
	}

	//	did we create the arena file?

	if (amt < sizeof(DbArena)) {
		if ((map = initArena(map, arenaDef, name, nameLen, rbEntry)))
			unlockArena(map->hndl, map->arenaPath);
#ifdef _WIN32
		VirtualFree(segZero, 0, MEM_RELEASE);
#else
		free(segZero);
#endif
		return map;
	}

	//  since segment zero exists,
	//	initialize the map
	//	and map seg zero

	assert(segZero->segs->size > 0);
	mapZero(map, segZero->segs->size);
#ifdef _WIN32
	VirtualFree(segZero, 0, MEM_RELEASE);
#else
	free(segZero);
#endif

	if (map->arena->rbAddr->addr)
		rbEntry = getObj(map, *map->arena->rbAddr);

	map->arenaDef = (ArenaDef *)(rbEntry + 1);
	map->objSize = map->arena->objSize;
	map->rbEntry = rbEntry;

	map->type[0] = map->arenaDef->arenaType;
	unlockArena(map->hndl, map->arenaPath);

	// wait for initialization to finish

	waitNonZero(map->arena->type);
	return map;
}

//	finish creating new arena
//	call with arena locked

DbMap *initArena (DbMap *map, ArenaDef *arenaDef, char *name, uint32_t nameLen, RedBlack *rbEntry) {
uint64_t initSize = arenaDef->params[InitSize].intVal;
uint32_t segOffset;
uint32_t bits;

	//	compute seg zero size multiple of cache line size

	segOffset = sizeof(DbArena) + arenaDef->baseSize;
	segOffset += 63;
	segOffset &= -64;

	if (initSize < segOffset)
		initSize = segOffset;

	if (initSize < MIN_segsize)
		initSize = MIN_segsize;

	initSize += 65535;
	initSize &= -65536;

#ifdef DEBUG
	if (map->arenaPath)
		fprintf(stderr, "InitMap %s at %llu bytes\n", map->arenaPath, initSize);
#endif
#ifdef _WIN32
	_BitScanReverse((unsigned long *)&bits, (unsigned long)initSize - 1);
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
	map->arena->segs->nextObject.offset = segOffset >> 4;
	map->arena->baseSize = arenaDef->baseSize;
	map->arena->objSize = arenaDef->objSize;
	map->arena->segs->size = initSize;
	map->arena->delTs = 1;

	//	are we a catalog or database?

	if (!rbEntry || !map->parent->parent) {
		rbEntry = rbNew(map, name, nameLen, sizeof(ArenaDef));
		map->arenaDef = (ArenaDef *)(rbEntry + 1);
		map->arena->rbAddr->bits = rbEntry->addr.bits;
		memcpy(map->arenaDef, arenaDef, sizeof(ArenaDef));
	} else
		map->arenaDef = arenaDef;

	map->objSize = map->arena->objSize;
	map->rbEntry = rbEntry;

	map->type[0] = arenaDef->arenaType;
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
//  return FALSE if out of memor from acceptance of all parties

bool newSeg(DbMap *map, uint32_t minSize) {
uint64_t size = map->arena->segs[map->arena->currSeg].size;
uint64_t off = map->arena->segs[map->arena->currSeg].off;
uint32_t nextSeg = map->arena->currSeg + 1;
uint64_t nextSize;

	nextSize = size * 2;	// double current size
	off += size;

	while (nextSize < minSize)
	 	if (nextSize < MAX_segsize)
			nextSize += nextSize;
		else
			fprintf(stderr, "newSeg segment overrun: %d\n", minSize), exit(1);

	if (nextSize > MAX_segsize)
		nextSize = MAX_segsize;

#ifdef _WIN32
	assert(__popcnt64(nextSize) == 1);
#else
	assert(__builtin_popcountll(nextSize) == 1);
#endif

	map->arena->segs[nextSeg].off = off;
	map->arena->segs[nextSeg].size = nextSize;
	map->arena->segs[nextSeg].nextObject.segment = nextSeg;
	map->arena->segs[nextSeg].nextId.seg = nextSeg;

	//  extend the disk file, windows does this automatically

#ifndef _WIN32
	if (map->hndl != -1)
	  if (ftruncate(map->hndl, nextSize + off)) {
		fprintf (stderr, "Unable to extend file %s to %" PRIu64 ", error = %d\n", map->arenaPath, nextSize + off, errno);
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

uint64_t allocObj(DbMap* map, DbAddr *free, DbAddr *wait, int type, uint32_t size, bool zeroit ) {
uint32_t bits, amt;
DbAddr slot;

	size += 15;
	size &= -16;

	if (!size)
		size = 16;

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

		if (wait)
			wait += type;
	} else
		amt = size;

	lockLatch(free->latch);

	while (!(slot.bits = getNodeFromFrame(map, free) & ADDR_BITS)) {
	  if (!getNodeWait(map, free, wait))
		if (!initObjFrame(map, free, type, size)) {
			unlockLatch(free->latch);
			return 0;
		}
	}

	unlockLatch(free->latch);

	if (zeroit)
		memset (getObj(map, slot), 0, amt);

	*slot.latch = type << BYTE_SHIFT;
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
	if (!slot.addr) {
		fprintf (stderr, "Invalid zero DbAddr: %s\n", map->arenaPath);
		exit(1);
	}

	//  catch up segment mappings

	if (slot.segment >= map->numSeg)
		mapAll(map);

	return map->base[slot.segment] + slot.offset * 16ULL;
}

//  allocate raw space in the current segment
//  or return 0 if out of memory.

uint64_t allocMap(DbMap *map, uint32_t size) {
uint64_t max, addr, off;

	lockLatch(map->arena->mutex);

	max = map->arena->segs[map->arena->currSeg].size
		  - map->arena->segs[map->arena->objSeg].nextId.idx * map->objSize;

	// round request up to cache line size

	size += 63;
	size &= -64;

#ifdef DEBUG
	atomicAdd64 (totalMemoryReq, size);
#endif

	// see if existing segment has space
	// otherwise allocate a new segment.

	off = map->arena->segs[map->arena->currSeg].nextObject.offset;

	if (off * 16ULL + size > max) {
		if (!newSeg(map, size)) {
			unlockLatch (map->arena->mutex);
			return 0;
		}
	}

	addr = map->arena->segs[map->arena->currSeg].nextObject.bits;
	map->arena->segs[map->arena->currSeg].nextObject.offset += size >> 4;
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
	if (!objId.idx) {
		fprintf (stderr, "Invalid zero document index: %s\n", map->arenaPath);
		exit(1);
	}

	if (objId.seg >= map->numSeg)
		mapAll(map);

	return map->base[objId.seg] + map->arena->segs[objId.seg].size - objId.idx * map->objSize;
}

//
// allocate next available object id
//

uint64_t allocObjId(DbMap *map, DbAddr *free, DbAddr *wait) {
ObjId objId;

	lockLatch(free->latch);

	// see if there is a free object in the free queue
	// otherwise create a new frame of new objects

	while (!(objId.bits = getNodeFromFrame(map, free) & ADDR_BITS)) {
		if (!getNodeWait(map, free, wait))
			if (!initObjIdFrame(map, free)) {
				unlockLatch(free->latch);
				return 0;
			}
	}

	unlockLatch(free->latch);
	return objId.bits;
}
