#include "db.h"
#include "db_redblack.h"
#include "db_object.h"
#include "db_handle.h"
#include "db_arena.h"
#include "db_index.h"
#include "db_map.h"

//	Catalog/Handle arena

extern DbMap memMap[1];

char hndlInit[1];
DbMap *hndlMap;
char *hndlPath;

//	allocate handle slot in memMap file

uint64_t newHndlSlot (void)  {
	return allocObjId(memMap, memMap->arena->freeBlk + ObjIdType, NULL);
}

DbAddr *slotHandle(ObjId hndlId) {
	return fetchIdSlot (memMap, hndlId);
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

Handle *makeHandle(DbMap *map, uint32_t xtraSize, HandleType type) {
DbAddr *hndlAddr, *slot;
Handle *handle;
ObjId hndlId;
uint32_t amt;
DbAddr addr;

	// first call?

	if (!(*hndlInit & TYPE_BITS))
		initHndlMap(NULL, 0, NULL, 0, true);

	// total size of the Handle structure

	amt = sizeof(Handle) + xtraSize;

	//	get a new or recycled ObjId

	if (!(hndlId.bits = allocObjId(memMap, memMap->arena->freeBlk + ObjIdType, NULL)))
		return 0;

	slot = fetchIdSlot (memMap, hndlId);

	addr.bits = allocBlk(hndlMap, amt, true);
	handle = getObj(hndlMap, addr);

	//  initialize the new Handle

	handle->addr.bits = addr.bits;
	handle->hndlId.bits = hndlId.bits;
	handle->xtraSize = xtraSize;	// size of following structure
	handle->hndlType = type;
	handle->map = map;

	//  allocate recycled frame queues
	//	three times the number of node types
	//	for the handle type

	if ((*handle->maxType = map->arenaDef->numTypes)) {
		handle->listIdx = arrayAlloc(map, map->arena->listArray, sizeof(DbAddr) * *handle->maxType * 3);
		handle->frames = arrayEntry(map, map->arena->listArray, handle->listIdx);
		arrayActivate(map, map->arena->listArray, handle->listIdx);
	}

	//	allocate hndlId array

	handle->arrayIdx = arrayAlloc(map->db, map->arenaDef->hndlArray, sizeof(ObjId));
	hndlAddr = arrayEntry(map->db, map->arenaDef->hndlArray, handle->arrayIdx);
	hndlAddr->bits = addr.bits;

	arrayActivate(map->db, map->arenaDef->hndlArray, handle->arrayIdx);

	//  install ObjId slot in local memory

	slot->bits = addr.bits;
	return handle;
}

//	disable handle resources

//	called by setter of the status KILL_BIT
//	after bindcnt goes to zero

void disableHndl(Handle *handle) {
	char maxType = atomicExchange8((char *)handle->maxType, 0);

	if (maxType)
		arrayRelease(handle->map, handle->map->arena->listArray, handle->listIdx);
}

//	destroy handle

void destroyHandle(Handle *handle, DbAddr *slot) {
DbAddr addr;

	if (!slot)
		slot = fetchIdSlot (memMap, handle->hndlId);

	lockLatch(slot->latch);

	//	already destroyed?

	if (!slot->addr) {
		slot->bits = 0;
		return;
	}

	disableHndl(handle);

	//  specific handle cleanup

	switch (handle->hndlType) {
	case Hndl_cursor:
		dbCloseCursor((void *)(handle + 1), handle->map);
		break;
	}

	// release the hndlAddr reservation in the arena

	arrayRelease(handle->map->db, handle->map->arenaDef->hndlArray, handle->arrayIdx);

	// zero the handle Id slot

	addr.bits = slot->bits;
	slot->bits = 0;

	//	never return the handle Id slot
	//	but return the memory

	freeBlk (hndlMap, addr);
}

//	bind handle for use in API call
//	return NULL if handle closed

Handle *bindHandle(DbHandle *dbHndl) {
Handle *handle;
DbAddr *slot;
ObjId hndlId;
uint32_t cnt;

	if ((hndlId.bits = dbHndl->hndlBits))
		slot = fetchIdSlot (memMap, hndlId);
	else
		return NULL;

	//	increment count of active binds
	//	and capture timestamp if we are the
	//	first handle bind

	handle = getObj(hndlMap, *slot);
	cnt = atomicAdd32(handle->bindCnt, 1);

	//	exit if it is being closed

	if ((*handle->status & KILL_BIT)) {
		dbHndl->hndlBits = 0;

		if (!atomicAdd32(handle->bindCnt, -1))
			destroyHandle (handle, slot);

		return NULL;
	}

	//	is there a DROP request for this arena?

	if (handle->map->arena->mutex[0] & KILL_BIT) {
		atomicOr8((volatile char *)handle->status, KILL_BIT);
		dbHndl->hndlBits = 0;

		if (!atomicAdd32(handle->bindCnt, -1))
			destroyHandle (handle, slot);

		return NULL;
	}

	//  are we the first call after an idle period?
	//	set the entryTs if so.

	if (cnt == 1)
		handle->entryTs = atomicAdd64(&handle->map->arena->nxtTs, 1);

	return handle;
}

//	release handle binding

void releaseHandle(Handle *handle, DbHandle dbHndl[1]) {

	if (!atomicAdd32(handle->bindCnt, -1)) {
	  if ((*handle->status & KILL_BIT)) {
		destroyHandle (handle, NULL);

	    if (dbHndl)
		  dbHndl->hndlBits = 0;
	  }
	}
}

//	disable all arena handles
//	by scanning HndlId arrayhndl
//	for the dropped arena

void disableHndls(DbMap *map, DbAddr *array) {
Handle *handle;
ArrayHdr *hdr;
int idx, seg;
DbAddr addr;

  if (array->addr) {
	lockLatch(array->latch);
	hdr = getObj(map->db, *array);

	//	process the level zero blocks in the array

	for (idx = 0; idx < (hdr->nxtIdx + ARRAY_size - 1) / ARRAY_size; idx++) {
	  uint64_t *inUse = getObj(map->db, hdr->addr[idx]);
	  DbAddr *hndlAddr = (DbAddr *)inUse;

	  for (seg = 0; seg < ARRAY_inuse; seg++) {
		uint64_t bits = inUse[seg];
		int slotIdx = 0;

		//	sluff unused slots in level zero block

		if (seg == 0) {
			slotIdx = ARRAY_first(sizeof(DbAddr));
			bits >>= slotIdx;
		}

		do if (bits & 1) {
		  addr.bits = hndlAddr[seg * 64 + slotIdx].bits;
		  handle = getObj(hndlMap, addr);

		  atomicOr8((volatile char *)handle->status, KILL_BIT);
		  waitZero32 (handle->bindCnt);
		  disableHndl(handle);
		} while (slotIdx++, bits /= 2);
	  }
	}

	unlockLatch(array->latch);
  }
}

//	find arena's earliest bound handle
//	by scanning HndlId array

uint64_t scanHandleTs(DbMap *map) {
DbAddr *array = map->arenaDef->hndlArray;
uint64_t lowTs = map->arena->nxtTs + 1;
Handle *handle;
ArrayHdr *hdr;
int idx, seg;
DbAddr addr;

  if (array->addr) {
	hdr = getObj(map->db, *array);

	//	process the level zero blocks in the array
	for (idx = 0; idx < (hdr->nxtIdx + ARRAY_size - 1) / ARRAY_size; idx++) {
	  uint64_t *inUse = getObj(map->db, hdr->addr[idx]);
	  DbAddr *hndlAddr = (DbAddr *)inUse;

	  for (seg = 0; seg < ARRAY_inuse; seg++) {
		uint64_t bits = inUse[seg];
		int slotIdx = 0;

		if (seg == 0) {
			slotIdx = ARRAY_first(sizeof(DbAddr));
			bits >>= slotIdx;
		}

		do if (bits & 1) {
		  addr.bits = hndlAddr[seg * 64 + slotIdx].bits;
		  handle = getObj(hndlMap, addr);

		  if (!(*handle->status & KILL_BIT))
		  	  if (handle->bindCnt[0])
			    lowTs = handle->entryTs;

		} while (slotIdx++, bits /= 2);
	  }
	}
  }

  return lowTs;
}
