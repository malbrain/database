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

//	open map given red/black rbEntry

DbMap *arenaRbMap(DbMap *parent, RedBlack *rbEntry) {
ArenaDef *arenaDef = getObj(parent->db, rbEntry->payLoad);
uint64_t *childMap;
DbMap *map;

	writeLock(parent->childMaps->lock);

	childMap = skipAdd(parent, parent->childMaps->head, arenaDef->id);

	if ((map = (DbMap *)*childMap))
		waitNonZero(map->arena->type);
	else {
		map = openMap(parent, rbKey(rbEntry), rbEntry->keyLen, arenaDef);
		*childMap = (uint64_t)map;
	}

	writeUnlock(parent->childMaps->lock);
	return map;
}

//  install the new arena name and params in the Catalog

RedBlack *createArenaDef(DbMap *parent, char *name, int nameLen, Params *params) {
uint64_t *skipPayLoad;
ArenaDef *arenaDef;
PathStk pathStk[1];
RedBlack *rbEntry;

	lockLatch(parent->arenaDef->nameTree->latch);

	//	see if ArenaDef already exists as a child in the parent

	if ((rbEntry = rbFind(parent->db, parent->arenaDef->nameTree, name, nameLen, pathStk))) {
		unlockLatch(parent->arenaDef->nameTree->latch);
		return rbEntry;
	}

	// otherwise, create new rbEntry in parent
	// with an arenaDef payload

	if ((rbEntry = rbNew(parent->db, name, nameLen, sizeof(ArenaDef))))
		arenaDef = getObj(parent->db, rbEntry->payLoad);
	else {
		unlockLatch(parent->arenaDef->nameTree->latch);
		return NULL;
	}

	arenaDef->id = atomicAdd64(&parent->arenaDef->childId, 1);
	initLock(arenaDef->idList->lock);

	//	add arenaDef to parent's child arenaDef tree

	rbAdd(parent->db, parent->arenaDef->nameTree, rbEntry, pathStk);

	//	add new rbEntry to parent's child id array

	writeLock(parent->arenaDef->idList->lock);
	skipPayLoad = skipAdd (parent->db, parent->arenaDef->idList->head, arenaDef->id);
	*skipPayLoad = rbEntry->addr.bits;
	writeUnlock(parent->arenaDef->idList->lock);

	unlockLatch(parent->arenaDef->nameTree->latch);
	return rbEntry;
}

//  open/create an Object database/store/index arena file
//	call with writeLock(parent->childMaps->lock)

DbMap *openMap(DbMap *parent, char *name, uint32_t nameLen, ArenaDef *arenaDef) {
DbArena *segZero = NULL;
DataBase *db;
DbMap *map;

#ifdef _WIN32
DWORD amt = 0;
#else
int32_t amt = 0;
#endif

	map = db_malloc(sizeof(DbMap) + arenaDef->localSize, true);

	if (!getPath(map, name, nameLen, parent, arenaDef->id)) {
		db_free(map);
		return NULL;
	}

	if ((map->parent = parent))
		map->db = parent->db;
	else
		map->db = map;

	if (!arenaDef->onDisk) {
#ifdef _WIN32
		map->hndl = INVALID_HANDLE_VALUE;
#else
		map->hndl = -1;
#endif
		return initArena(map, arenaDef);
	}

	//	open the onDisk arena file

#ifdef _WIN32
	map->hndl = openPath (map->path);

	if (map->hndl == INVALID_HANDLE_VALUE) {
		db_free(map);
		return NULL;
	}

	lockArena(map);

	segZero = VirtualAlloc(NULL, sizeof(DbArena), MEM_COMMIT, PAGE_READWRITE);

	if (!ReadFile(map->hndl, segZero, sizeof(DbArena), &amt, NULL)) {
		fprintf (stderr, "Unable to read %lld bytes from %s, error = %d", sizeof(DbArena), map->path, errno);
		VirtualFree(segZero, 0, MEM_RELEASE);
		CloseHandle(map->hndl);
		db_free(map);
		return NULL;
	}
#else
	map->hndl = openPath (map->path);

	if (map->hndl == -1) {
		db_free(map);
		return NULL;
	}

	lockArena(map);

#ifdef DEBUG
	fprintf(stderr, "lockArena %s\n", map->path);
#endif
	// read first part of segment zero if it exists

	segZero = valloc(sizeof(DbArena));

	amt = pread(map->hndl, segZero, sizeof(DbArena), 0LL);

	if (amt < 0) {
		fprintf (stderr, "Unable to read %d bytes from %s, error = %d", (int)sizeof(DbArena), map->path, errno);
		unlockArena(map);
		close(map->hndl);
		free(segZero);
		db_free(map);
		return NULL;
	}
#endif
	//	did we create the arena?

	if (amt < sizeof(DbArena)) {
		if ((map = initArena(map, arenaDef)))
			unlockArena(map);
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

	initLock(map->childMaps->lock);
	assert(segZero->segs->size > 0);

	mapZero(map, segZero->segs->size);
#ifdef _WIN32
	VirtualFree(segZero, 0, MEM_RELEASE);
#else
	free(segZero);
#endif

	//	are we opening the Catalog arena?

	if (arenaDef->arenaType == Hndl_catalog) {
		Catalog *cat = (Catalog *)(map->arena + 1);
		map->arenaDef = cat->arenaDef;
	} else
		map->arenaDef = arenaDef;

	unlockArena(map);

	// wait for initialization to finish

	waitNonZero(map->arena->type);
	return map;
}

//	finish creating new arena
//	call with arena locked

DbMap *initArena (DbMap *map, ArenaDef *arenaDef) {
uint64_t initSize = arenaDef->initSize;
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
	if (map->pathLen)
		fprintf(stderr, "InitMap %s at %llu bytes\n", map->path, initSize);
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
		fprintf (stderr, "Unable to initialize file %s, error = %d", map->path, errno);
		close(map->hndl);
		db_free(map);
		return NULL;
	  }
#endif

	//  initialize new arena segment zero

	assert(initSize > 0);

	mapZero(map, initSize);
	map->arena->segs[map->arena->currSeg].nextObject.offset = segOffset >> 3;
	map->arena->objSize = arenaDef->objSize;
	map->arena->segs->size = initSize;
	*map->arena->mutex = ALIVE_BIT;
	map->arena->delTs = 1;

	//	are we creating a database?

	if (arenaDef->arenaType == Hndl_catalog) {
		Catalog *cat = (Catalog *)(map->arena + 1);
		memcpy(cat->arenaDef, arenaDef, sizeof(ArenaDef));
		initLock(cat->arenaDef->idList->lock);
		map->arenaDef = cat->arenaDef;
	} else
		map->arenaDef = arenaDef;

	return map;
}

//  initialize arena segment zero

void mapZero(DbMap *map, uint64_t size) {

	assert(size > 0);

	map->arena = mapMemory (map, 0, size, 0);
	map->base[0] = (char *)map->arena;

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
		fprintf (stderr, "Unable to extend file %s to %ULL, error = %d", map->path, nextSize, errno);
		return false;
	  }
#endif

	if (!mapSeg(map, nextSeg))
		return false;

	map->arena->currSeg = nextSeg;
	map->maxSeg = nextSeg;
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

	*slot.latch = type | ALIVE_BIT;
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

	while (map->maxSeg < map->arena->currSeg)
		if (mapSeg (map, map->maxSeg + 1))
			map->maxSeg++;
		else
			fprintf(stderr, "Unable to map segment %d on map %s\n", map->maxSeg + 1, map->path), exit(1);

	unlockLatch(map->mapMutex);
}

void* getObj(DbMap *map, DbAddr slot) {
	if (!slot.addr) {
		fprintf (stderr, "Invalid zero DbAddr: %s\n", map->path);
		exit(1);
	}

	//  catch up segment mappings

	if (slot.segment > map->maxSeg)
		mapAll(map);

	return map->base[slot.segment] + slot.offset * 8ULL;
}

//	close the arena

void closeMap(DbMap *map) {
	while (map->maxSeg)
		unmapSeg(map, map->maxSeg--);

	map->arena = NULL;
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
		fprintf (stderr, "Invalid zero document index: %s\n", map->path);
		exit(1);
	}

	return map->base[objId.seg] + map->arena->segs[objId.seg].size - objId.index * map->arena->objSize;
}

//
// allocate next available object id
//

uint64_t allocObjId(DbMap *map, FreeList *list, uint16_t idx) {
ObjId objId;

	lockLatch(list[ObjIdType].free->latch);

	// see if there is a free object in the free queue
	// otherwise create a new frame of new objects

	while (!(objId.bits = getNodeFromFrame(map, list[ObjIdType].free))) {
		if (!getNodeWait(map, list[ObjIdType].free, list[ObjIdType].tail))
			if (!initObjIdFrame(map, list[ObjIdType].free)) {
				unlockLatch(list[ObjIdType].free->latch);
				return 0;
			}
	}

	objId.idx = idx;
	unlockLatch(list[ObjIdType].free->latch);
	return objId.bits;
}

// drop an arena and recursively its children

void dropMap(DbMap *map, bool dropDefs) {
uint64_t *skipPayLoad;
uint32_t childIdMax;
DbAddr *next, addr;
RedBlack *rbEntry;
DbAddr rbPayLoad;
SkipNode *node;
DbMap *child;
uint64_t id;
int idx;

	//	wait until arena is created

	waitNonZero(map->arena->type);

	//	are we already dropped?

	lockLatch(map->arena->mutex);

	if (~*map->arena->mutex & ALIVE_BIT) {
		*map->arena->mutex = 0;
		return;
	}

	//  delete our current id from parent's child id list

	id = map->arenaDef->id;
	writeLock(map->parent->arenaDef->idList->lock);
	addr.bits = skipDel(map->parent->db, map->parent->arenaDef->idList->head, id); 
	rbEntry = getObj(map->parent->db, addr);
	writeUnlock(map->parent->arenaDef->idList->lock);

	//	our ArenaDef addr

	rbPayLoad.bits = rbEntry->payLoad.bits;

	//	either delete our entry in our parent's child list and name tree,
	//	or advance our id number so future child creators
	//	build a new file under a new id

	if (dropDefs) {
		rbDel(map->parent->db, map->parent->arenaDef->nameTree, rbEntry); 
		childIdMax = UINT32_MAX;
	} else {
		map->arenaDef->id = atomicAdd64(&map->parent->arenaDef->childId, 1);
		skipPayLoad = skipAdd (map->parent->db, map->parent->arenaDef->idList->head, map->arenaDef->id);
		*skipPayLoad = rbEntry->addr.bits;
		childIdMax = map->arenaDef->childId;
	}

	writeUnlock(map->parent->arenaDef->idList->lock);

	//	remove the ALIVE bits
	//	and drop the children

	do {
	  writeLock(map->arenaDef->idList->lock);
	  next = map->arenaDef->idList->head;

	  if (next->addr)
		node = getObj(map->db, *next);
	  else
		break;

	  if (*node->array->key < childIdMax)
	  	addr.bits = *node->array->val;
	  else
		break;

	  writeUnlock(map->arenaDef->idList->lock);

	  rbEntry = getObj(map->db, addr);
	  child = arenaRbMap(map, rbEntry);
	  dropMap(child, dropDefs);
	} while (true);

	writeUnlock(map->arenaDef->idList->lock);

	//	return arenaDef storage

	freeBlk(map->parent->db, rbPayLoad);

	//  wait for handles to exit
	//	and delete our map

	lockLatch(map->arena->hndlCalls->latch);
	disableHndls(map, map->arena->hndlCalls);
	unlockLatch(map->arena->hndlCalls->latch);
	deleteMap(map);
}
