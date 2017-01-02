#include "db.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_map.h"

//	find array block for existing idx

uint64_t *arrayBlk(DbMap *map, DbAddr *array, uint16_t idx) {
DbAddr *addr;

	if (!array->addr)
		return NULL;

	assert(idx % ARRAY_size >= ARRAY_inuse);

	addr = getObj(map, *array);
	return getObj(map, addr[idx / ARRAY_size]);
}

//	return addr of array segment for array index

uint64_t arrayAddr(DbMap *map, DbAddr *array, uint16_t idx) {
DbAddr *addr;

	if (!array->addr)
		return 0;

	assert(idx % ARRAY_size >= ARRAY_inuse);

	addr = getObj(map, *array);
	return addr[idx / ARRAY_size].bits;
}

//	return payload address for an idx from an array segment

void *arrayEntry (DbMap *map, DbAddr addr, uint16_t idx, size_t size) {
uint64_t *inUse = getObj(map, addr);
uint8_t *base;

	assert(idx % ARRAY_size >= ARRAY_inuse);

	base = (uint8_t *)(inUse + ARRAY_inuse);
	base += size * ((idx % ARRAY_size) - ARRAY_inuse);
	return base;
}

//	return payload address for an array element idx

void *arrayElement(DbMap *map, DbAddr *array, uint16_t idx, size_t size) {
uint64_t *inUse;
uint8_t *base;
DbAddr *addr;

	assert(idx % ARRAY_size >= ARRAY_inuse);

	lockLatch(array->latch);

	//	allocate the first level of the Array
	//	and fill it with the first array segment

	if (!array->addr) {
		array->bits = allocBlk(map, sizeof(DbAddr) * 256, true) | ADDR_MUTEX_SET;
		addr = getObj(map, *array);
		addr->bits = allocBlk(map, sizeof(uint64_t) * ARRAY_inuse + size * (ARRAY_size - ARRAY_inuse), true);

		// sluff first slots

		inUse = getObj(map, *addr);
		*inUse = (1ULL << ARRAY_inuse) - 1;
	} else
		addr = getObj(map, *array);

	//	fill-in missing slots

	while (idx / ARRAY_size > array->maxidx)
	  if (array->maxidx == 255) {
#ifdef DEBUG
		fprintf(stderr, "Array Overflow file: %s\n", map->arenaPath);
#endif
		return NULL;
	  } else
		addr[++array->maxidx].bits = allocBlk(map, sizeof(uint64_t) * ARRAY_inuse + size * (ARRAY_size - ARRAY_inuse), true);

	inUse = getObj(map, addr[idx / ARRAY_size]);
	inUse[idx % ARRAY_size / 64] &= ~(1ULL << (idx % 64));

	base = (uint8_t *)(inUse + ARRAY_inuse);
	base += size * ((idx % ARRAY_size) - ARRAY_inuse);
	unlockLatch(array->latch);

	return base;
}

//	allocate an array element index

uint16_t arrayAlloc(DbMap *map, DbAddr *array, size_t size) {
unsigned int bits[1];
uint64_t *inUse;
DbAddr *addr;
int idx, seg;

	lockLatch(array->latch);

	//	initialize empty array

	if (!array->addr) {
		array->bits = allocBlk(map, sizeof(DbAddr) * 256, true) | ADDR_MUTEX_SET;
		addr = getObj(map, *array);
		addr->bits = allocBlk(map, sizeof(uint64_t) * ARRAY_inuse + size * (ARRAY_size - ARRAY_inuse), true);

		// sluff first slots

		inUse = getObj(map, *addr);
		*inUse = (1ULL << ARRAY_inuse) - 1;
	} else
		addr = getObj(map, *array);

	idx = array->maxidx + 1;

	while (idx--) {
		inUse = getObj(map, addr[idx]);

		//  find array segment with available entry

		for (seg = 0; seg < ARRAY_inuse; seg++)
		  if (inUse[seg] < UINT64_MAX)
			break;

		if (seg == ARRAY_inuse)
			continue;

#		ifdef _WIN32
		  _BitScanForward64(bits, ~inUse[seg]);
#		else
		  *bits = (__builtin_ffsll (~inUse[seg])) - 1;
#		endif

		inUse[seg] |= 1ULL << *bits;
		unlockLatch(array->latch);
		return *bits + idx * ARRAY_size + seg * 64;
	}

	// current array segments are full
	//	allocate a new segment

	if (array->maxidx == 255) {
		fprintf(stderr, "Array Overflow file: %s\n", map->arenaPath);
		exit(1);
	 }

	addr[++array->maxidx].bits = allocBlk(map, sizeof(uint64_t) * ARRAY_inuse + size * (ARRAY_size - ARRAY_inuse), true);
	inUse = getObj(map, addr[idx]);

	inUse[0] = (1ULL << ARRAY_inuse) - 1;
	inUse[0] |= 1ULL << ARRAY_inuse;

	unlockLatch(array->latch);
	return array->maxidx * ARRAY_size + ARRAY_inuse;
}

//	peel off 64 bit suffix value from key
//	return number of key bytes remaining

uint32_t get64(char *key, uint32_t len, uint64_t *where) {
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

uint32_t store64(char *key, uint32_t keyLen, uint64_t recId) {
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
