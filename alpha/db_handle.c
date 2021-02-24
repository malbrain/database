#include "base64.h"
#include "db.h"
#include "db_handle.h"
#include "db_api.h"
#include "db_arena.h"
#include "db_cursor.h"
#include "db_map.h"
#include "db_object.h"
#include "db_redblack.h"

extern char *hndlNames[]; 

DbAddr *listQueue(Handle *handle, int nodeType, FrameList listType) {
DbMap *map = MapAddr(handle);
int listBase = listType * handle->maxType[0] + nodeType;
  return (DbAddr *)((uint8_t *)arrayEntry(map, map->arena->listArray, handle->listIdx + listBase));
}

//	make handle from map pointer
//	leave it bound

Handle *makeHandle(DbMap *dbMap, uint32_t clntSize, uint32_t xtraSize,
  HandleType type) {
  Handle *handle;
  ObjId hndlId, *mapHndls;

  //	get a new or recycled ObjId slot where the handle will live

  if (!(hndlId.bits =
    allocObjId(hndlMap, hndlMap->arena->freeBlk + ObjIdType, NULL)))
    return NULL;

  handle = fetchIdSlot(hndlMap, hndlId);

  //  initialize the new Handle
  //	allocate in HndlMap

  // size of handle client area (e.g. Cursor/Iterator)

  clntSize += 15;
  clntSize &= -16;

  if (sizeof(Handle) + xtraSize > hndlMap->arena->objSize) {
    fprintf(stderr,
      "Error: makeHandle(%s): sizeof (Handle: %d) + (xtraSize: %d) .gt. "
      "(ObjSize: %d)\n",
      hndlNames[type],
      (int)sizeof(Handle), xtraSize, hndlMap->arenaDef->objSize);
    return NULL;
  }

  if ((handle->clntSize = clntSize))
    handle->clientAddr.bits = allocBlk(dbMap, clntSize, true);

  handle->entryTs = atomicAdd64(&dbMap->arena->nxtTs, 1);
  handle->hndlId.bits = hndlId.bits;
  handle->hndlType = type;
  handle->bindCnt[0] = 1;

  //  allocate recycled frame queues
  //	three times the number of node types
  //	for the handle type

  if ((*handle->maxType = dbMap->arenaDef->numTypes))
    handle->listIdx = arrayAlloc(dbMap, dbMap->arena->listArray,
    sizeof(DbAddr) * *handle->maxType * 3);

  //	allocate entry slot in the open hndlId array for this arena
  //	and fill in the id for the handle's map

  handle->arrayIdx =
    arrayAlloc(dbMap, dbMap->arenaDef->hndlArray, sizeof(ObjId));

  mapHndls = arrayEntry(dbMap, dbMap->arenaDef->hndlArray, handle->arrayIdx);
  mapHndls->bits = hndlId.bits;

  if (debug)
    printf("listIdx = %.6d  maxType = %.3d arrayIdx = %.6d  hndl:%s\n",

    handle->listIdx, *handle->maxType, handle->arrayIdx,
    hndlNames[handle->hndlType]);

  handle->mapAddr = db_memAddr(dbMap);
  return handle;
}

// assign Catalog docStore idx slot

//	delete handle resources

//	called by setter of the status KILL_BIT
//	after bindcnt goes to zero

void destroyHandle(Handle *handle) {
  char maxType = atomicExchange8((uint8_t *)handle->maxType, 0);
  DbMap *dbMap = MapAddr(handle);
  ArenaDef *arenaDef = dbMap->arenaDef;
  uint32_t count;

  if (!maxType) return;
  if (handle->clntSize) freeBlk(dbMap, handle->clientAddr);

  arrayRelease(dbMap, dbMap->arena->listArray, handle->listIdx);
  arrayRelease(dbMap, dbMap->arenaDef->hndlArray, handle->arrayIdx);

  //  specific handle cleanup

  switch (handle->hndlType) {
    case Hndl_cursor:
      dbCloseCursor(getObj(dbMap, handle->clientAddr), dbMap);
      break;
  }

  // release the hndlId reservation in the arena

  arrayRelease(dbMap, dbMap->arenaDef->hndlArray, handle->arrayIdx);

  //	never return the handle Id slot
  //	but return the memory

  freeBlk(dbMap, handle->clientAddr);

  // zero the handle Id status

  if (~dbMap->drop[0] & KILL_BIT) return;

  lockLatch(arenaDef->hndlArray->latch);
  count = disableHndls(arenaDef->hndlArray);
  unlockLatch(arenaDef->hndlArray->latch);

  if (!count)
    if (!*dbMap->openCnt) closeMap(dbMap);
}

//	enter api with a handle

bool enterHandle(Handle *handle, DbMap *map) {
  int cnt = atomicAdd32(handle->bindCnt, 1);

  //  are we the first call after an idle period?
  //	set the entryTs if so.

  if (cnt == 1) handle->entryTs = atomicAdd64(&map->arena->nxtTs, 1);

  //	exit if the handle is being closed

  if ((*handle->status & KILL_BIT)) {
    if (!atomicAdd32(handle->bindCnt, -1)) destroyHandle(handle);

    return false;
  }

  //	is there a DROP request for this arena?

  if (map->drop[0] & KILL_BIT) {
    atomicOr8((volatile uint8_t *)handle->status, KILL_BIT);

    if (!atomicAdd32(handle->bindCnt, -1)) destroyHandle(handle);

    return false;
  }

  return true;
}

//	bind handle for use in API call
//	return NULL if handle closed

Handle *bindHandle(DbHandle *dbHndl, HandleType hndlType) {
  Handle *handle = HandleAddr(dbHndl);
  HandleType type = handle->hndlType;

  switch (hndlType) {
    case Hndl_anyIdx:
      if (type != Hndl_artIndex && type != Hndl_btree1Index &&
          type != Hndl_btree2Index)
        return NULL;

      break;

    case Hndl_any:
      break;

    default:
      if (hndlType != type) 
          return NULL;

      break;
  }

  //	increment count of active binds
  //	and capture timestamp if we are the
  //	first handle bind

  if (enterHandle(handle, MapAddr(handle))) return handle;

  return NULL;
}

//	release handle binding

void releaseHandle(Handle *handle) {
  if (!atomicAdd32(handle->bindCnt, -1)) {
    if ((*handle->status & KILL_BIT)) {
      destroyHandle(handle);
    }
  }
}

//	disable all arena handles
//	by scanning HndlId arrayhndl
//	for dropped arenas

//  call with array DbAddr latched
//	return count of bound handles

uint32_t disableHndls(DbAddr *array) {
  uint32_t count = 0;
  Handle *handle;
  ArrayHdr *hdr;
  int slot, seg;
  ObjId objId;

  if (array->addr) {
    hdr = getObj(hndlMap, *array);

    //	process the level zero blocks in the array

    for (slot = 0; slot < hdr->maxLvl0; slot++) {
      uint64_t *inUse = getObj(hndlMap, hdr->addr[slot]);
      DbAddr *hndlAddr = (DbAddr *)inUse;

      for (seg = 0; seg < ARRAY_inuse; seg++) {
        uint64_t bits = inUse[seg];
        int slotIdx = 0;

        //	sluff unused slots in level zero block

        if (seg == 0) {
          slotIdx = ARRAY_first(sizeof(DbAddr));
          bits >>= slotIdx;
        }

        do
          if (bits & 1) {
            objId.bits = hndlAddr[seg * 64 + slotIdx].bits;
            handle = fetchIdSlot(hndlMap, objId);

            atomicOr8((volatile uint8_t *)handle->status, KILL_BIT);
            count += *handle->bindCnt;
          }
        while (slotIdx++, bits /= 2);
      }
    }
  }

  return count;
}

//	find arena's earliest bound handle
//	by scanning HndlId array

uint64_t scanHandleTs(DbMap *map) {
  DbAddr *array = map->arenaDef->hndlArray;
  uint64_t lowTs = map->arena->nxtTs + 1;
  Handle *handle;
  ArrayHdr *hdr;
  int slot, seg;
  DbAddr addr;

  if (array->addr) {
    hdr = getObj(hndlMap, *array);

    //	process all the level zero blocks in the array

    for (slot = 0; slot < hdr->maxLvl0; slot++) {
      uint64_t *inUse = getObj(hndlMap, hdr->addr[slot]);
      DbAddr *hndlAddr = (DbAddr *)inUse;

      for (seg = 0; seg < ARRAY_inuse; seg++) {
        uint64_t bits = inUse[seg];
        int slotIdx = 0;

        if (seg == 0) {
          slotIdx = ARRAY_first(sizeof(DbAddr));
          bits >>= slotIdx;
        }

        do
          if (bits & 1) {
            addr.bits = hndlAddr[seg * 64 + slotIdx].bits;
            handle = getObj(hndlMap, addr);

            if (!(*handle->status & KILL_BIT))
              if (handle->bindCnt[0]) lowTs = handle->entryTs;
          }
        while (slotIdx++, bits /= 2);
      }
    }
  }

  return lowTs;
}
