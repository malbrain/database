#include "db.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_frame.h"
#include "db_map.h"

//	return payload address for an allocated array idx

void *arrayEntry (DbMap *map, DbAddr *array, uint16_t idx) {
ArrayHdr *hdr = getObj(map, *array);
uint8_t *base;

	assert(idx % ARRAY_size >= ARRAY_first(hdr->objSize));

	base = getObj(map, hdr->addr[idx / ARRAY_size]);
	base += hdr->objSize * (idx % ARRAY_size);
	return base;
}

//	activate an allocated array index

void arrayActivate(DbMap *map, DbAddr *array, uint16_t idx) {
ArrayHdr *hdr = getObj(map, *array);
uint64_t *inUse;

	assert(idx % ARRAY_size >= ARRAY_first(hdr->objSize));
	lockLatch(array->latch);

	assert(hdr->addr[idx / ARRAY_size].bits);
	inUse = getObj(map, hdr->addr[idx / ARRAY_size]);

	//	set our bit

	inUse[idx % ARRAY_size / 64] |= 1ULL << (idx % 64);
	unlockLatch(array->latch);
}

//	free an allocated array index

void arrayRelease(DbMap *map, DbAddr *array, uint16_t idx) {
ArrayHdr *hdr = getObj(map, *array);
uint64_t *inUse;

	assert(idx % ARRAY_size >= ARRAY_first(hdr->objSize));
	lockLatch(array->latch);

	assert(hdr->addr[idx / ARRAY_size].bits);
	inUse = getObj(map, hdr->addr[idx / ARRAY_size]);

	//	clear our bit

	inUse[idx % ARRAY_size / 64] &= ~(1ULL << (idx % 64));

	//	add the idx to the available entry frame

	addSlotToFrame(map, hdr->availIdx, NULL, (uint64_t)idx);
	unlockLatch(array->latch);
}

//	return payload address for a foreign element idx

void *arrayElement(DbMap *map, DbAddr *array, uint16_t idx, size_t size) {
uint8_t *base;
ArrayHdr *hdr;

	assert(idx % ARRAY_size >= ARRAY_first(size));
	lockLatch(array->latch);

	//	allocate the first level of the Array
	//	and fill it with the first array segment

	if (!array->addr)
		array->bits = allocBlk(map, sizeof(ArrayHdr), true) | ADDR_MUTEX_SET;

	hdr = getObj(map, *array);
	hdr->objSize = size;

	//	fill-in level zero blocks up to the segment for the given idx

	if (idx >= hdr->nxtIdx) {
		for (int seg = (hdr->nxtIdx + ARRAY_size - 1) / ARRAY_size; seg < (idx + ARRAY_size - 1) / ARRAY_size; seg++)
			hdr->addr[seg].bits = allocBlk(map, size * ARRAY_size, true);

		hdr->nxtIdx = idx + 1;
	}

	base = getObj(map, hdr->addr[idx / ARRAY_size]);
	base += size * (idx % ARRAY_size);
	unlockLatch(array->latch);

	return base;
}

//	allocate an array element index

uint16_t arrayAlloc(DbMap *map, DbAddr *array, size_t size) {
ArrayHdr *hdr;
uint16_t idx;

	lockLatch(array->latch);

	//	initialize empty array

	if (!array->addr)
		array->bits = allocBlk(map, sizeof(ArrayHdr), true) | ADDR_MUTEX_SET;

	hdr = getObj(map, *array);
	hdr->maxIdx = ARRAY_size * ARRAY_lvl1;
	hdr->objSize = size;

	//  see if we have a released index to return

	if ((idx = getNodeFromFrame(map, hdr->availIdx))) {
		unlockLatch(array->latch);
		return idx;
	}

	if (hdr->nxtIdx < hdr->maxIdx) {
	  if (!(hdr->nxtIdx % ARRAY_size)) {
		hdr->addr[hdr->nxtIdx / ARRAY_size].bits = allocBlk(map, size * ARRAY_size, true);
		hdr->nxtIdx += ARRAY_first(size);
	  }

	  unlockLatch(array->latch);
	  return hdr->nxtIdx++;
	}

	fprintf(stderr, "Array Overflow max = %d, file: %s\n", hdr->maxIdx, map->arenaPath);
	exit(0);
}

//	peel off 64 bit suffix value from key
//	return number of key bytes taken

uint32_t get64(uint8_t *key, uint32_t len, uint64_t *where) {
uint32_t xtraBytes = key[len - 1] & 0x7;
uint64_t result;
int idx = 0;

	//	set len to the number start

	len -= xtraBytes + 2;

	//  get sign of the result

	if (key[len] & 0x80)
		result = 0;
	else
		result = -1;

	// get high order 3 bits

	result <<= 3;
	result |= key[len] & 0x07;

	//	assemble complete bytes
	//	up to 56 bits

	while (idx++ < xtraBytes) {
	  result <<= 8;
	  result |= key[len + idx];
	}

	//	add in low order 5 bits

	result <<= 5;
	result |= key[len + xtraBytes + 1] >> 3;

	if (where)
		*where = result;

	return xtraBytes + 2;
}

// concatenate key with 64 bit value
// returns number of bytes concatenated

uint32_t store64(uint8_t *key, uint32_t keyLen, uint64_t recId) {
int64_t tst64 = recId >> 9;
uint32_t xtraBytes = 0;
uint32_t idx, signBit;

	//  set high order bit to one for positive values

	signBit = (int64_t)recId < 0 ? 0 : 1;

	while (tst64)
	  if (!signBit && tst64 == -1)
		break;
	  else
		xtraBytes++, tst64 >>= 8;

	//	store low order 5 bits

    key[keyLen + xtraBytes + 1] = (recId & 0x1f) << 3 | xtraBytes;
    recId >>= 5;

	//	store complete bytes
	//	up to 56 bits worth

    for (idx = xtraBytes; idx; idx--) {
        key[keyLen + idx] = (recId & 0xff);
        recId >>= 8;
    }

	//	store high order 3 bits

    key[keyLen] = (recId & 0x7) | (xtraBytes << 3);
	key[keyLen] |= signBit << 7;

    return xtraBytes + 2;
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
