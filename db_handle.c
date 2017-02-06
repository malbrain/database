#include "db.h"
#include "db_redblack.h"
#include "db_object.h"
#include "db_handle.h"
#include "db_arena.h"
#include "db_map.h"

//	Catalog/Handle arena

char hndlInit[1];
DbMap *hndlMap;
char *hndlPath;

DbAddr *slotHandle(ObjId hndlId) {
	return fetchIdSlot (hndlMap, hndlId);
}

//	return Handle from DbHandle

Handle *getHandle(DbHandle *hndl) {
DbAddr *slot;
ObjId hndlId;

	hndlId.bits = hndl->hndlBits;
	slot = fetchIdSlot (hndlMap, hndlId);

	if ((*slot->latch & KILL_BIT))
		return NULL;

	return getObj(hndlMap, *slot);
}

void initHndlMap(char *path, int pathLen, char *name, int nameLen, bool onDisk) {
ArenaDef arenaDef[1];

	lockLatch(hndlInit);

	if (*hndlInit & TYPE_BITS) {
		unlockLatch(hndlInit);
		return;
	}

	if (pathLen) {
		hndlPath = db_malloc(pathLen + 1, false);
		memcpy(hndlPath, path, pathLen);
		hndlPath[pathLen] = 0;
	}

	if (!name) {
		name = "_Catalog";
		nameLen = strlen(name);
	}

	memset(arenaDef, 0, sizeof(arenaDef));
	arenaDef->params[OnDisk].boolVal = onDisk;
	arenaDef->baseSize = sizeof(Catalog);
	arenaDef->arenaType = Hndl_catalog;
	arenaDef->objSize = sizeof(ObjId);

	hndlMap = openMap(NULL, name, nameLen, arenaDef, NULL);
	hndlMap->db = hndlMap;

	*hndlMap->arena->type = Hndl_catalog;
	*hndlInit = Hndl_catalog;
}

//	make handle from map pointer
//	return hndlId.bits in hndlMap or zero

uint64_t makeHandle(DbMap *map, uint32_t xtraSize, HandleType type) {
DbAddr *listArray;
ObjId *hndlIdx;
DbAddr *slot;
ObjId hndlId;
Handle *hndl;
uint32_t amt;
DbAddr addr;

	// first call?

	if (!(*hndlInit & TYPE_BITS))
		initHndlMap(NULL, 0, NULL, 0, true);

	// total size of the Handle structure

	amt = sizeof(Handle) + xtraSize;

	//	get a new or recycled ObjId

	if (!(hndlId.bits = allocObjId(hndlMap, hndlMap->arena->freeBlk, NULL, 0)))
		return 0;

	slot = fetchIdSlot (hndlMap, hndlId);

	addr.bits = allocBlk(hndlMap, amt, true);
	hndl = getObj(hndlMap, addr);

	//  initialize the new Handle

	hndl->hndlId.bits = hndlId.bits;
	hndl->xtraSize = xtraSize;	// size of following structure
	hndl->hndlType = type;
	hndl->map = map;

	//  allocate recycled frame queues
	//	three times the number of node types
	//	for the index type

	if ((hndl->maxType = map->arenaDef->numTypes)) {
		hndl->frameIdx = arrayAlloc(map, map->arena->listArray, sizeof(DbAddr) * hndl->maxType * 3);
		listArray = arrayAddr(map, map->arena->listArray, hndl->frameIdx);
		hndl->frames = listArray + (hndl->frameIdx % ARRAY_size) * hndl->maxType * 3;
	}

	//	allocate hndlId array

	hndl->arenaIdx = arrayAlloc(map->db, map->arenaDef->hndlIds, sizeof(ObjId));

	hndlIdx = arrayEntry(map->db, map->arenaDef->hndlIds, hndl->arenaIdx, sizeof(ObjId));
	hndlIdx->bits = hndlId.bits;

	//  install in ObjId slot

	slot->bits = addr.bits;
	return hndlId.bits;
}

//	destroy handle

void destroyHandle(Handle *hndl, DbAddr *slot) {
uint64_t *inUse;

	lockLatch(slot->latch);

	//	already destroyed?

	if (!slot->addr) {
		slot->bits = 0;
		return;
	}

	// release handle freeList

	if (hndl->maxType) {
		lockLatch(hndl->map->arena->listArray->latch);
		inUse = arrayBlk(hndl->map, hndl->map->arena->listArray, hndl->frameIdx);
		inUse[hndl->frameIdx % ARRAY_size / 64] &= ~(1ULL << (hndl->frameIdx % 64));
		unlockLatch(hndl->map->arena->listArray->latch);
	}

	// release handle Id slot

	freeBlk (hndlMap, *slot);
	slot->bits = 0;
}

//	bind handle for use in API call
//	return false if handle closed

Handle *bindHandle(DbHandle *dbHndl) {
DbAddr *slot;
Handle *hndl;
ObjId hndlId;
uint32_t cnt;

	if ((hndlId.bits = dbHndl->hndlBits))
		slot = fetchIdSlot (hndlMap, hndlId);
	else
		return NULL;

	if ((*slot->latch & KILL_BIT))
		return NULL;

	//	increment count of active binds
	//	and capture timestamp if we are the
	//	first handle bind

	hndl = getObj(hndlMap, *slot);
	cnt = atomicAdd32(hndl->bindCnt, 1);

	//	exit if it was reclaimed?

	if ((*slot->latch & KILL_BIT)) {
		dbHndl->hndlBits = 0;

		if (!atomicAdd32(hndl->bindCnt, -1))
			destroyHandle (hndl, slot);

		return NULL;
	}

	//	is there a DROP request for this arena?

	if (hndl->map->arena->mutex[0] & KILL_BIT) {
		atomicOr8(slot->latch, KILL_BIT);
		dbHndl->hndlBits = 0;

		if (!atomicAdd32(hndl->bindCnt, -1))
			destroyHandle (hndl, slot);

		return NULL;
	}

	//  are we the first call after an idle period?
	//	set the entryTs if so.

	if (cnt == 1)
		hndl->entryTs = atomicAdd64(&hndl->map->arena->nxtTs, 1);

	return hndl;
}

//	release handle binding

void releaseHandle(Handle *hndl) {
	atomicAdd32(hndl->bindCnt, -1);
}

//	disable all arena handles
//	by scanning HndlId array

void disableHndls(DbMap *map, DbAddr *array) {
DbAddr *addr;
ObjId hndlId;
int idx, seg;

  if (array->addr) {
	addr = getObj(map->db, *array);

	for (idx = 0; idx <= array->maxidx; idx++) {
	  uint64_t *inUse = getObj(map->db, addr[idx]);
	  ObjId *hndlIdx = (ObjId *)inUse;

	  for (seg = 0; seg < ARRAY_inuse; seg++) {
		uint64_t bits = inUse[seg];
		int slotIdx = 0;

		if (seg == 0) {
			slotIdx = ARRAY_first(sizeof(ObjId));
			bits >>= slotIdx;
		}

		do if (bits & 1) {
		  hndlId.bits = hndlIdx[seg * 64 + slotIdx].bits;
		  DbAddr *slot = fetchIdSlot(hndlMap, hndlId);
		  Handle *hndl = getObj(hndlMap, *slot);
		  DbAddr handle[1];

		  // take control of the handle slot

		  handle->bits = atomicExchange(&slot->bits, 0);

		  //  wait for outstanding activity to finish
		  //  destroy the handle

		  if (handle->addr) {
			atomicOr8(slot->latch, KILL_BIT);
			waitZero32 (hndl->bindCnt);
		  	destroyHandle(hndl, slot);
		  }

		} while (slotIdx++, bits /= 2);
	  }
	}
  }
}

//	find arena's earliest bound handle
//	by scanning HndlId array

uint64_t scanHandleTs(DbMap *map) {
uint64_t lowTs = map->arena->nxtTs + 1;
DbAddr *array = map->arenaDef->hndlIds;
DbAddr *addr;
int idx, seg;

  if (array->addr) {
	addr = getObj(map->db, *array);

	for (idx = 0; idx <= array->maxidx; idx++) {
	  uint64_t *inUse = getObj(map->db, addr[idx]);
	  ObjId *hndlIdx = (ObjId *)inUse;

	  for (seg = 0; seg < ARRAY_inuse; seg++) {
		uint64_t bits = inUse[seg];
		int slotIdx = 0;

		if (seg == 0) {
			slotIdx = ARRAY_first(sizeof(ObjId));
			bits >>= slotIdx;
		}

		do if (bits & 1) {
		  DbAddr *slot = fetchIdSlot(hndlMap, hndlIdx[seg * 64 + slotIdx]);
		  Handle *hndl = getObj(hndlMap, *slot);

		  if (!hndl->bindCnt[0])
			  continue;
		  else
			  lowTs = hndl->entryTs;
		} while (slotIdx++, bits /= 2);
	  }
	}
  }

  return lowTs;
}
