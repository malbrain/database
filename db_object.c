#include "db.h"
#include "db_object.h"
#include "db_handle.h"
#include "db_arena.h"
#include "db_map.h"

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

//	peel off 64 bit suffix value from key
//	return number of key bytes remaining

uint32_t get64(uint8_t *key, uint32_t len, uint64_t *where) {
uint32_t xtraBytes = key[len - 1] & 0x7;
int signBit = key[0] & 0x80 ? 1 : 0;
uint64_t result = 0;
int idx = 0;

	if (signBit)
		result = -1;

	len -= xtraBytes + 2;
	result <<= 4;
	result |= key[len] & 0x0f;

	while (idx++ < xtraBytes) {
	  result <<= 8;
	  result |= key[len + idx];
	}

	result <<= 5;
	result |= key[len + xtraBytes] >> 3;

	if (where)
		*where = result;

	return len;
}

// concatenate key with 64 bit value
// returns length of concatenated key

uint32_t store64(uint8_t *key, uint32_t keyLen, uint64_t recId) {
int64_t tst64 = recId >> 9;
uint32_t xtraBytes = 0;
uint32_t idx, signBit;

	signBit = (int64_t)recId < 0 ? 0 : 1;

	while (tst64)
	  if (!signBit && tst64 == -1)
		break;
	  else
		xtraBytes++, tst64 >>= 8;

    key[keyLen + xtraBytes + 1] = (recId & 0x1f) << 3 | xtraBytes;

    recId >>= 5;

    for (idx = xtraBytes; idx; idx--) {
        key[keyLen + idx] = (recId & 0xff);
        recId >>= 8;
    }

    key[keyLen] = recId | (xtraBytes << 4);
	key[keyLen] |= signBit << 7;

    return keyLen + xtraBytes + 2;
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

