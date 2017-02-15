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

uint32_t get64(uint8_t *key, uint32_t len, uint64_t *where, bool binaryFlds) {
int idx = 0, xtraBytes = key[len - 1] & 0x7;
int off = binaryFlds ? 2 : 0;
uint64_t result;

	//	set len to the number start

	len -= xtraBytes + 2;

	//  get sign of the result
	// 	positive has high bit set

	if (key[len] & 0x80)
		result = 0;
	else
		result = -1;

	// get high order 4 bits

	result <<= 4;
	result |= key[len] & 0x0f;

	//	assemble complete bytes
	//	up to 56 bits

	while (idx++ < xtraBytes) {
	  result <<= 8;
	  result |= key[len + idx];
	}

	//	add in low order 4 bits

	result <<= 4;
	result |= key[len + xtraBytes + 1] >> 4;

	if (where)
		*where = result;

	return off + xtraBytes + 2;
}

// concatenate key with 64 bit value
// returns number of bytes concatenated

uint32_t store64(uint8_t *key, uint32_t keyLen, int64_t recId, bool binaryFlds) {
int off = binaryFlds ? 2 : 0;
int64_t tst64 = recId >> 8;
uint32_t xtraBytes = 0;
uint32_t idx, len;
bool neg;

	neg = (int64_t)recId < 0;

	while (tst64)
	  if (neg && tst64 == -1)
		break;
	  else
		xtraBytes++, tst64 >>= 8;

	//	store low order 4 bits

    key[keyLen + xtraBytes + off + 1] = (recId & 0xf) << 4 | xtraBytes;
    recId >>= 4;

	//	store complete bytes
	//	up to 56 bits worth

    for (idx = xtraBytes; idx; idx--) {
        key[keyLen + off + idx] = (recId & 0xff);
        recId >>= 8;
    }

	//	store high order 4 bits and
	//	the 3 bits of xtraBytes
	//	and the sign bit

    key[keyLen + off] = (recId & 0xf) | (xtraBytes << 4) | 0x80;

	//	if neg, complement the sign bit & xtraBytes bits to
	//	make negative numbers lexically smaller than positive ones

	if (neg)
		key[keyLen + off] ^= 0xf0;

    len = xtraBytes + 2;

	if (binaryFlds) {
		key[keyLen] = len >> 8;
		key[keyLen + 1] = len;
	}

    return len + off;
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

//	de-duplication
//	set mmbrship

uint32_t mmbrSizes[] = { 15, 29, 61, 113, 251, 503, 1021 };

//	determine & set membership
//	return start of hash table entry

uint64_t *findMmbr(DbMmbr *mmbr, uint64_t item) {
uint32_t idx = item % mmbr->max;
uint32_t incr = mmbr->entryCnt;
uint64_t *limit = mmbr->table;
uint64_t *entry;

	if (!idx)
		idx = 1;

	limit += (mmbr->max - 1) * incr;
	entry = mmbr->table + idx * incr;

	while (*entry && *entry != item) {
      if (entry == limit)
		entry = mmbr->table + incr;
	  else
		entry += incr;
	}

	return entry;
}

uint64_t *setMmbr(DbMap *map, DbAddr *addr, uint64_t item, uint8_t entryCnt) {
uint64_t *slot = NULL, *test;
DbMmbr *mmbr, *first;
uint32_t idx;
int redo = 0;

	lockLatch(addr->latch);

	if (!addr->bits) {
	  if ((addr->bits = allocBlk(map, mmbrSizes[0] * entryCnt * sizeof(uint64_t) + sizeof(DbMmbr), true) | ADDR_MUTEX_SET))
		first = getObj(map, *addr);
	  else
		goto mmbrxit;

	  first->entryCnt = entryCnt;
	  first->max = mmbrSizes[0];
	  first->sizeIdx = 0;
	} else if (!(first = getObj(map, *addr)))
	  goto mmbrxit;

	if (!item) {
	  first->zeroItem = true;
	  slot = first->table;
	  goto mmbrxit;
	}

	//  look in the first table

	slot = findMmbr(first, item);
	mmbr = first;

	//	otherwise, examine the remainingn set tables

	if (!*slot)
	  while ((mmbr = mmbr->next.bits ? getObj(map, mmbr->next) : NULL))
		if (*(test = findMmbr(mmbr, item))) {
		  slot = test;
		  goto mmbrxit;
		}

	//	fill in item value
	//	in the first table

	mmbr = first;
    *slot = item;

	//	first table overflow?

	if (3 * ++first->cnt / 2 > mmbrSizes[first->sizeIdx]) {
	  if (first->sizeIdx < sizeof(mmbrSizes) / sizeof(uint32_t))
		redo = ++first->sizeIdx;

	  if (!(first->next.bits = allocBlk(map, mmbrSizes[first->sizeIdx] * entryCnt * sizeof(uint64_t) + sizeof(DbMmbr), true))) {
		slot = NULL;
		goto mmbrxit;
	  }

	  mmbr = getObj(map, first->next);
	  mmbr->max = mmbrSizes[first->sizeIdx];
	  mmbr->zeroItem = first->zeroItem;
	  mmbr->entryCnt = entryCnt;
	  mmbr->cnt = 0;

	  // copy over item zero, if it exists

	  memcpy (mmbr->table, first->table, entryCnt * sizeof(uint64_t));

	  // transfer items from old to bigger?

	  if (!redo)
		goto mmbrxit;

	  // skip slot for zero item

	  for (idx = 1; idx < first->max * entryCnt; idx += entryCnt)
		if (first->table[idx]) {
		  test = findMmbr(mmbr, item);

		  if (first->table[idx] == item)
			slot = test;

		  memcpy(test, first->table + idx, entryCnt * sizeof(uint64_t));
	  	  mmbr->cnt++;
	    }
	}

mmbrxit:
	unlockLatch(addr->latch);
	return test;
}
