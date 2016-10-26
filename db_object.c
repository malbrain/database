#include "db.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_map.h"

extern DbMap memMap[1];

//	find array block for existing idx

uint64_t *arrayBlk(DbMap *map, DbAddr *array, uint32_t idx) {
DbAddr *addr;

	if (!array->addr)
		return NULL;

	addr = getObj(map, *array);
	return getObj(map, addr[idx / 64]);
}

//	return payload address for an array element idx

void *arrayElement(DbMap *map, DbAddr *array, uint16_t idx, size_t size) {
uint64_t *inUse;
uint8_t *base;
DbAddr *addr;

	lockLatch(array->latch);

	if (!array->addr) {
		array->bits = allocBlk(map, sizeof(DbAddr) * 256, true) | ADDR_MUTEX_SET;
		addr = getObj(map, *array);
		addr->bits = allocBlk(map, sizeof(uint64_t) + size * 63, true);
		inUse = getObj(map, *addr);
		*inUse = 1ULL;
	} else
		addr = getObj(map, *array);

	while (idx / 64 > array->maxidx)
	  if (array->maxidx == 255) {
#ifdef DEBUG
		fprintf(stderr, "Array Overflow file: %s\n", map->path);
#endif
		return NULL;
	  } else
		addr[++array->maxidx].bits = allocBlk(map, sizeof(uint64_t) + size * 63, true);

	inUse = getObj(map, addr[idx / 64]);
	*inUse |= 1ULL << idx % 64;

	base = (uint8_t *)(inUse + 1);
	base += size * ((idx % 64) - 1);
	unlockLatch(array->latch);

	return (void *)base;
}

//	allocate an array element index

uint16_t arrayAlloc(DbMap *map, DbAddr *array, size_t size) {
unsigned long bits[1];
uint64_t *inUse;
DbAddr *addr;
int idx, max;

	lockLatch(array->latch);

	if (!array->addr) {
		array->bits = allocBlk(map, sizeof(DbAddr) * 256, true) | ADDR_MUTEX_SET;
		addr = getObj(map, *array);
		addr->bits = allocBlk(map, sizeof(uint64_t) + size * 63, true);
		inUse = getObj(map, *addr);
		*inUse = 1ULL;
	} else
		addr = getObj(map, *array);

	for (idx = 0; idx <= array->maxidx; idx++) {
		inUse = getObj(map, addr[idx]);

		//  skip completely used array entry

		if (inUse[0] == ULLONG_MAX)
			continue;

#		ifdef _WIN32
		  _BitScanForward64(bits, ~inUse[0]);
#		else
		  *bits = (__builtin_ffs (~inUse[0])) - 1;
#		endif

		*inUse |= 1ULL << *bits;
		unlockLatch(array->latch);
		return *bits + idx * 64;
	}

	// current array is full
	//	allocate a new segment

	if (array->maxidx == 255) {
		fprintf(stderr, "Array Overflow file: %s\n", map->path);
		exit(1);
	 }

	addr[++array->maxidx].bits = allocBlk(map, sizeof(uint64_t) + size * 63, true);
	inUse = getObj(map, addr[idx]);
	*inUse = 3ULL;

	unlockLatch(array->latch);
	return array->maxidx * 64 + 1;
}

//	make handle from map pointer

uint64_t makeHandle(DbMap *map, uint32_t xtraSize, uint32_t listMax, HandleType type) {
DbAddr *array = map->handleArray;
uint64_t *inUse;
DbAddr *addr;
Handle *hndl;
uint32_t amt;
uint16_t idx;

	amt = sizeof(Handle) + xtraSize;
	idx = arrayAlloc(map, array, sizeof(DbAddr));
	addr = arrayElement(map, array, idx, sizeof(DbAddr));

	if ((addr->bits = allocBlk(memMap, amt, false)))
		hndl = getObj(memMap, *addr);
	else
		return 0;

	memset (hndl, 0, amt);
	hndl->xtraSize = xtraSize;	// size of following structure
	hndl->maxType = listMax;	// number of list entries
	hndl->hndlType = type;
	hndl->arenaIdx = idx;
	hndl->map = map;

	if (listMax) {
		idx = arrayAlloc(map, map->arena->listArray, sizeof(FreeList) * listMax);
		inUse = arrayBlk(map, map->arena->listArray, idx);
		hndl->list = (FreeList *)(inUse + 1) + ((idx % 64) - 1) * listMax;
		hndl->listIdx = idx;
	}

	idx = arrayAlloc(map, map->arena->hndlCalls, sizeof(HndlCall));
	hndl->calls = arrayElement(map, map->arena->hndlCalls, idx, sizeof(HndlCall));
	hndl->calls->entryIdx = idx;
	return addr->bits;
}

//	return handle

void returnHandle(Handle  *hndl) {
uint64_t *inUse;

	lockLatch(hndl->map->handleArray->latch);

	// release freeList

	if (hndl->list) {
		lockLatch(hndl->map->arena->listArray->latch);
		inUse = arrayBlk(hndl->map, hndl->map->arena->listArray, hndl->listIdx);
		inUse[0] &= ~(1ULL << (hndl->listIdx % 64));
		unlockLatch(hndl->map->arena->listArray->latch);
	}

	// clear handle in-use bit

	inUse = arrayBlk(hndl->map, hndl->map->handleArray, hndl->arenaIdx);
	inUse[0] &= ~(1ULL << (hndl->arenaIdx % 64));
	unlockLatch(hndl->map->handleArray->latch);
}

//	bind handle for use in API call
//	return false if arena dropped

DbStatus bindHandle(DbHandle *dbHndl, Handle **hndl) {

	lockLatch(dbHndl->handle.latch);

	if (!dbHndl->handle.addr)
		return DB_ERROR_handleclosed;

	*hndl = getObj(memMap, dbHndl->handle);

	//	increment count of active binds
	//	and capture timestamp if we are the
	//	first handle bind

	if (atomicAdd32((*hndl)->calls->entryCnt, 1) == 1)
		(*hndl)->calls->entryTs = atomicAdd64(&(*hndl)->map->arena->nxtTs, 1);

	//	is there a DROP request active?

	if (~(*hndl)->map->arena->mutex[0] & ALIVE_BIT) {
		(*hndl)->map = NULL;
		releaseHandle((*hndl));
		return DB_ERROR_arenadropped;
	}

	unlockLatch(dbHndl->handle.latch);
	return DB_OK;
}

//	release handle binding

void releaseHandle(Handle *hndl) {
	atomicAdd32(hndl->calls->entryCnt, -1);
}

//	peel off 64 bit suffix value from key
//	return number of key bytes remaining

uint32_t get64(uint8_t *key, uint32_t len, uint64_t *where) {
uint32_t xtrabytes = key[len - 1] & 0x7;
uint64_t result;
int idx = 0;

	len -= xtrabytes + 2;
	result = key[len] & 0x1f;

	while (idx++ < xtrabytes) {
	  result <<= 8;
	  result |= key[len + idx];
	}

	result <<= 5;
	result |= key[len + idx] >> 3;

	if (where)
		*where = result;

	return len;
}

// concatenate key with 64 bit value
// returns length of concatenated key

uint32_t store64(uint8_t *key, uint32_t keylen, uint64_t recId) {
uint64_t tst64 = recId >> 10;
uint32_t xtrabytes = 0;
uint32_t idx;

	while (tst64)
		xtrabytes++, tst64 >>= 8;

    key[keylen + xtrabytes + 1] = (recId & 0x1f) << 3 | xtrabytes;

    recId >>= 5;

    for (idx = xtrabytes; idx; idx--) {
        key[keylen + idx] = (recId & 0xff);
        recId >>= 8;
    }

    key[keylen] = recId | (xtrabytes << 5);
    return keylen + xtrabytes + 2;
}

//	allocate a new timestamp

uint64_t allocateTimestamp(DbMap *map, enum ReaderWriterEnum e) {
DataBase *db = database(map->db);
uint64_t ts;

	ts = *db->timestamp;

	if (!ts)
		ts = atomicAdd64(db->timestamp, 1);

	switch (e) {
	case en_reader:
		while (!isReader(ts))
			ts = atomicAdd64(db->timestamp, 1);
		break;
	case en_writer:
		while (!isWriter(ts))
			ts = atomicAdd64(db->timestamp, 1);
		break;

	default: break;
	}

	return ts;
}

//	reader == even

bool isReader(uint64_t ts) {
	return !(ts & 1);
}

//	writer == odd

bool isWriter(uint64_t ts) {
	return (ts & 1);
}

//	committed == not reader

bool isCommitted(uint64_t ts) {
	return (ts & 1);
}

//	find arena's earliest bound handle
//	by scanning HndlCall array

uint64_t scanHandleTs(DbMap *map) {
uint64_t lowTs = map->arena->nxtTs + 1;
DbAddr *array = map->arena->hndlCalls;
DbAddr *addr;
int idx;

  if (array->addr) {
	addr = getObj(map, *array);

	for (idx = 0; idx <= array->maxidx; idx++) {
	  uint64_t *inUse = getObj(map, addr[idx]);
	  HndlCall *call = (HndlCall *)(inUse + 1);
	  uint64_t bits = *inUse;
	  int bit = 0;

	  while (bit++, bits /= 2) {
		if (bits & 1) {
		  if (!call[bit].entryCnt[0])
			continue;
		  else
			lowTs = call[bit].entryTs;
		}
	  }
	}
  }

  return lowTs;
}

