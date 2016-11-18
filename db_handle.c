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

HandleId *slotHandle(uint64_t hndlBits) {
ObjId hndlId;

	hndlId.bits = hndlBits;
	return fetchIdSlot (hndlMap, hndlId);
}

//	return Handle from DbHandle

Handle *getHandle(DbHandle *hndl) {
HandleId *slot;
ObjId hndlId;

	hndlId.bits = hndl->hndlBits;
	slot = fetchIdSlot (hndlMap, hndlId);

	if ((*slot->addr->latch & KILL_BIT))
		return NULL;

	return getObj(hndlMap, *slot->addr);
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
	arenaDef->objSize = sizeof(HandleId);
	arenaDef->arenaType = Hndl_catalog;

	hndlMap = openMap(NULL, name, nameLen, arenaDef, NULL);
	hndlMap->db = hndlMap;

	*hndlMap->arena->type = Hndl_catalog;
	*hndlInit = Hndl_catalog;
}

//	make handle from map pointer
//	return hndlId.bits in hndlMap or zero

uint64_t makeHandle(DbMap *map, uint32_t xtraSize, HandleType type) {
HndlCall *hndlCall;
uint64_t *inUse;
HandleId *slot;
ObjId hndlId;
Handle *hndl;
uint32_t amt;
uint16_t idx;
DbAddr addr;

	// first call?

	if (!(*hndlInit & TYPE_BITS))
		initHndlMap(NULL, 0, NULL, 0, true);

	// total size of the Handle

	amt = sizeof(Handle) + xtraSize;

	//	get a new or recycled HandleId

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

	if((hndl->maxType = map->arenaDef->numTypes)) {
		idx = arrayAlloc(map, map->arena->listArray, sizeof(DbAddr) * amt * 3);
		inUse = arrayBlk(map, map->arena->listArray, idx);
		hndl->frames = (DbAddr *)(inUse + ARRAY_inuse) + ((idx % ARRAY_size) - ARRAY_inuse) * amt * 3;
		hndl->frameIdx = idx;
	}

	//	allocate and initialize a hndlCall for the handle

	idx = arrayAlloc(map->db, map->arenaDef->hndlCalls, sizeof(HndlCall));
	hndl->calls.bits = arrayAddr(map->db, map->arenaDef->hndlCalls, idx);
	hndl->callIdx = idx;

	hndlCall = arrayEntry(map->db, hndl->calls, idx, sizeof(HndlCall));
	hndlCall->hndlId.bits = hndlId.bits;

	//  install in HandleId slot

	slot->addr->bits = addr.bits;
	return hndlId.bits;
}

//	destroy handle
//	call with id slot locked

void destroyHandle(DbMap *map, DbAddr *addr) {
Handle *hndl = getObj(hndlMap, *addr);
uint64_t *inUse;

	//	already destroyed?

	if (!addr->addr)
		return;

	// release handle freeList

	if (hndl->maxType) {
		lockLatch(map->arena->listArray->latch);
		inUse = arrayBlk(map, map->arena->listArray, hndl->frameIdx);
		inUse[hndl->frameIdx % ARRAY_size / 64] &= ~(1ULL << (hndl->frameIdx % 64));
		unlockLatch(map->arena->listArray->latch);
	}

	freeBlk (hndlMap, *addr);
	addr->bits = 0;
}

//	bind handle for use in API call
//	return false if handle closed

Handle *bindHandle(DbHandle *dbHndl) {
HndlCall *hndlCall;
HandleId *slot;
Handle *hndl;
ObjId hndlId;
uint32_t cnt;

	hndlId.bits = dbHndl->hndlBits;
	slot = fetchIdSlot (hndlMap, hndlId);

	if ((*slot->addr->latch & KILL_BIT))
		return NULL;

	//	increment count of active binds
	//	and capture timestamp if we are the
	//	first handle bind

	cnt = atomicAdd32(slot->bindCnt, 1);

	//	exit if it was reclaimed?

	if ((*slot->addr->latch & KILL_BIT)) {
		atomicAdd32(slot->bindCnt, -1);
		return NULL;
	}

	hndl = getObj(hndlMap, *slot->addr);

	//	is there a DROP request for this arena?

	if (hndl->map->arena->mutex[0] & KILL_BIT) {
		lockLatch(slot->addr->latch);
		atomicAdd32(slot->bindCnt, -1);
		destroyHandle (hndl->map, slot->addr);
		return NULL;
	}

	//  are we the first call after an idle period?
	//	set the entryTs if so.

	if (cnt == 1) {
		hndlCall = arrayEntry(hndl->map->db, hndl->calls, hndl->callIdx, sizeof(HndlCall));
		hndlCall->entryTs = atomicAdd64(&hndl->map->arena->nxtTs, 1);
	}

	return hndl;
}

//	release handle binding

void releaseHandle(Handle *hndl) {
HandleId *slot;
ObjId hndlId;

	hndlId.bits = hndl->hndlId.bits;
	slot = fetchIdSlot (hndlMap, hndlId);

	atomicAdd32(slot->bindCnt, -1);
}

//	disable all arena handles
//	by scanning HndlCall array

void disableHndls(DbMap *map, DbAddr *array) {
DbAddr handle[1];
HandleId *slot;
DbAddr *addr;
ObjId hndlId;
int idx, seg;

  if (array->addr) {
	addr = getObj(map, *array);

	for (idx = 0; idx <= array->maxidx; idx++) {
	  uint64_t *inUse = getObj(map, addr[idx]);
	  HndlCall *call = (HndlCall *)(inUse + ARRAY_inuse);

	  for (seg = 0; seg < ARRAY_inuse; seg++) {
		uint64_t bits = inUse[seg] >> ARRAY_inuse;
		int slotIdx = ARRAY_inuse;

		do if (bits & 1) {
		  hndlId.bits = call[seg * 64 + slotIdx - ARRAY_inuse].hndlId.bits;
		  slot = fetchIdSlot(hndlMap, hndlId);

		  handle->bits = atomicExchange(&slot->addr->bits, 0);

		  //  wait for outstanding activity to finish

		  waitZero32 (slot->bindCnt);

		  //  destroy the handle

		  destroyHandle(map, handle);
		} while (slotIdx++, bits /= 2);
	  }
	}
  }
}

//	find arena's earliest bound handle
//	by scanning HndlCall array

uint64_t scanHandleTs(DbMap *map) {
uint64_t lowTs = map->arena->nxtTs + 1;
DbAddr *array = map->arenaDef->hndlCalls;
DbAddr *addr;
int idx, seg;

  if (array->addr) {
	addr = getObj(map->db, *array);

	for (idx = 0; idx <= array->maxidx; idx++) {
	  uint64_t *inUse = getObj(map->db, addr[idx]);
	  HndlCall *call = (HndlCall *)(inUse + ARRAY_inuse);

	  for (seg = 0; seg < ARRAY_inuse; seg++) {
		uint64_t bits = inUse[seg] >> ARRAY_inuse;
		int slotIdx = ARRAY_inuse;

		do if (bits & 1) {
		  HandleId *slot = fetchIdSlot(hndlMap, call[seg * 64 + slotIdx - ARRAY_inuse].hndlId);

		  if (!slot->bindCnt[0])
			  continue;
		  else
			  lowTs = call[slotIdx - 1].entryTs;
		} while (slotIdx++, bits /= 2);
	  }
	}
  }

  return lowTs;
}

