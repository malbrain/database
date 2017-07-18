#include "db.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_frame.h"
#include "db_map.h"

uint16_t arrayFirst(uint32_t objSize) {

	//  must at least have room for bit map

	if (objSize < ARRAY_size / 8)
		objSize = ARRAY_size / 8;

	return (uint16_t)(ARRAY_first(objSize));
}

//	return payload address for an allocated array idx

void *arrayEntry (DbMap *map, DbAddr *array, uint16_t idx) {
ArrayHdr *hdr;
uint8_t *base;

	if (array->addr)
		hdr = getObj(map, *array);
	else
		return NULL;

	if(idx % ARRAY_size < ARRAY_first(hdr->objSize))
		return NULL;

	base = getObj(map, hdr->addr[idx / ARRAY_size]);
	base += hdr->objSize * (idx % ARRAY_size);
	return base;
}

//	free an allocated array index

void arrayRelease(DbMap *map, DbAddr *array, uint16_t idx) {
ArrayHdr *hdr = getObj(map, *array);
uint16_t slot = idx / ARRAY_size;
uint64_t *inUse;

	assert(idx % ARRAY_size >= ARRAY_first(hdr->objSize));
	lockLatch(array->latch);

	assert(hdr->addr[slot].bits);
	inUse = getObj(map, hdr->addr[slot]);

	//	clear our bit

	inUse[idx % ARRAY_size / 64] &= ~(1ULL << (idx % 64));

	//	set firstx for this level 0 block

	if (hdr->addr[slot].firstx > idx % ARRAY_size / 64)
		hdr->addr[slot].firstx = idx % ARRAY_size / 64;

	if (hdr->level0 > slot)
		hdr->level0 = slot;

	unlockLatch(array->latch);
}

//	return payload address for unallocated, foreign generated element idx
//	N.b. ensure the size of both arrays is exactly the same.

void *arrayElement(DbMap *map, DbAddr *array, uint16_t idx, uint32_t size) {
uint64_t *inUse;
uint8_t *base;
ArrayHdr *hdr;

	//  must at least have room for bit map

	if (size < ARRAY_size / 8)
		size = ARRAY_size / 8;

	assert(idx % ARRAY_size >= ARRAY_first(size));
	lockLatch(array->latch);

	//	allocate the first level of the Array
	//	and fill it with the first array segment

	if (!array->addr)
		array->bits = allocBlk(map, sizeof(ArrayHdr), true) | ADDR_MUTEX_SET;

	hdr = getObj(map, *array);
	hdr->objSize = size;

	//	fill-in level zero blocks up to the segment for the given idx

	while (hdr->maxLvl0 < idx / ARRAY_size + 1) {
		hdr->addr[hdr->maxLvl0].bits = allocBlk(map, size * ARRAY_size, true);
		inUse = getObj(map, hdr->addr[hdr->maxLvl0]);
		*inUse = (1ULL << ARRAY_first(size)) - 1;
		hdr->maxLvl0++;
	}

	inUse = getObj(map, hdr->addr[idx / ARRAY_size]);
	base = (uint8_t *)inUse;

	inUse += idx % ARRAY_size / 64;
	*inUse |= 1ULL << (idx % 64);

	base += size * (idx % ARRAY_size);
	unlockLatch(array->latch);

	return base;
}

//	allocate an array element index

uint16_t arrayAlloc(DbMap *map, DbAddr *array, uint32_t size) {
unsigned long bits[1];
uint16_t seg, slot;
uint64_t *inUse;
ArrayHdr *hdr;

	lockLatch(array->latch);

	//  must at least have room for bit map

	if (size < ARRAY_size / 8)
		size = ARRAY_size / 8;

	//	initialize empty array

	if (!array->addr)
		array->bits = allocBlk(map, sizeof(ArrayHdr), true) | ADDR_MUTEX_SET;

	//	get the array header

	hdr = getObj(map, *array);
	hdr->objSize = size;

	//	find a level 0 block that's not full
	//	and scan it for an empty element

	for (slot = hdr->level0; slot < ARRAY_lvl1; slot++) {
	  if (!hdr->addr[slot].addr) {
		hdr->addr[slot].bits = allocBlk(map, size * ARRAY_size, true);

		inUse = getObj(map, hdr->addr[slot]);
		*inUse = (1ULL << ARRAY_first(size)) - 1;
		*inUse |= 1ULL << ARRAY_first(size);

		if (hdr->maxLvl0 < slot + 1)
			hdr->maxLvl0 = slot + 1;

		hdr->level0 = slot;
		unlockLatch(array->latch);
		return (uint16_t)(ARRAY_first(size) + slot * ARRAY_size);
	  }

	  seg = hdr->addr[slot].firstx;

	  if (seg == ARRAY_inuse)
		continue;

	  inUse = getObj(map, hdr->addr[slot]);

	  while (seg < ARRAY_inuse)
		if (inUse[seg] < ULLONG_MAX)
		  break;
		else
		  seg++;

	  if (seg < ARRAY_inuse) {
#		ifdef _WIN32
 		  _BitScanForward64(bits, ~inUse[seg]);
#		else
		  *bits = (__builtin_ffs (~inUse[seg])) - 1;
#		endif
  
		  //  set our bit

 		  inUse[seg] |= 1ULL << *bits;

		  if (inUse[seg] < ULLONG_MAX)
			hdr->addr[slot].firstx = (uint8_t)seg;
		  else
			hdr->addr[slot].firstx = ARRAY_inuse;

		  hdr->level0 = slot;
		  unlockLatch(array->latch);
		  return (uint16_t)(*bits + slot * ARRAY_size + seg * 64);
	  }
	}
  	
	fprintf(stderr, "Array Overflow file: %s\n", map->arenaPath);
	unlockLatch(array->latch);
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

//	calc size required to store64

uint32_t size64(int64_t recId, bool binaryFlds) {
int off = binaryFlds ? 2 : 0;
int64_t tst64 = recId >> 8;
uint32_t xtraBytes = 0;
bool neg;

	neg = recId < 0;

	while (tst64)
	  if (neg && tst64 == -1)
		break;
	  else
		xtraBytes++, tst64 >>= 8;

    return xtraBytes + 2 + off;
}

// concatenate key with 64 bit value
// returns number of bytes concatenated

uint32_t store64(uint8_t *key, uint32_t keyLen, int64_t recId, bool binaryFlds) {
int off = binaryFlds ? 2 : 0;
int64_t tst64 = recId >> 8;
uint32_t xtraBytes = 0;
uint32_t idx, len;
bool neg;

	neg = recId < 0;

	while (tst64)
	  if (neg && tst64 == -1)
		break;
	  else
		xtraBytes++, tst64 >>= 8;

	//	store low order 4 bits

    key[keyLen + xtraBytes + off + 1] = (uint8_t)((recId & 0xf) << 4 | xtraBytes);
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

    key[keyLen + off] = (uint8_t)((recId & 0xf) | (xtraBytes << 4) | 0x80);

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

//	de-duplication
//	set mmbrship

uint16_t mmbrSizes[] = { 13, 29, 61, 113, 251, 503, 1021, 2039, 4093, 8191, 16381, 32749, 65521};

//	determine set membership
//	return hash table entry

uint64_t *findMmbr(DbMmbr *mmbr, uint64_t keyVal, bool add) {
uint64_t *entry = mmbr->table + keyVal % mmbr->max;
uint64_t *limit = mmbr->table + mmbr->max;
uint64_t *first = NULL, item;

	while ((item = *entry)) {
	  if (item == keyVal)
		return entry;
	  if (add && item == ~0LL && !first)
		first = entry;
      if (++entry == limit)
		entry = mmbr->table;
	}

	return first ? first : entry;
}

//  mmbr slot probe enumerators
//	only handle first mmbr table

//	return forward occupied entries in mmbr table
//	call w/mmbr locked

void *allMmbr(DbMmbr *mmbr, uint64_t *entry) {
	if (!entry)
		entry = mmbr->table - 1;

	while (++entry < mmbr->table + mmbr->max)
	  if (*entry && *entry < ~0ULL)
		return entry;

	return NULL;
}

//	return reverse occupied entries in mmbr table
//	call w/mmbr locked

void *revMmbr(DbMmbr *mmbr, uint64_t *entry) {
	if (!entry)
		entry = mmbr->table + mmbr->max;

	while (entry-- > mmbr->table)
	  if (*entry && *entry < ~0ULL)
		return entry;

	return NULL;
}

//	advance hash table entry to next slot
//	call w/mmbr locked

void *nxtMmbr(DbMmbr *mmbr, uint64_t *entry) {
	if (++entry < mmbr->table + mmbr->max)
		return entry;

	//	wrap around to first slot

	return mmbr->table;
}

//  start ptr in first mmbr hash table slot
//	call w/mmbr locked

void *getMmbr(DbMmbr *mmbr, uint64_t keyVal) {
	return mmbr->table + keyVal % mmbr->max;
}

//	initialize new mmbr set
//	call w/addr latched

DbMmbr *iniMmbr(DbMap *map, DbAddr *addr, int minSize) {
DbMmbr *first;
int idx;

	for (idx = 0; idx < sizeof(mmbrSizes) / sizeof(uint16_t); idx++)
	  if (minSize < mmbrSizes[idx])
		break;

	if ((addr->bits = allocBlk(map, mmbrSizes[idx] * sizeof(uint64_t) + sizeof(DbMmbr), true) | ADDR_MUTEX_SET))
		first = getObj(map, *addr);
	else {
		fprintf(stderr, "iniMmbr: out of memory");
		exit(0);
	}

	first->max = mmbrSizes[idx];
	first->sizeIdx = idx;
	return first;
}

//	expand DbMmbr to larger size

DbMmbr *xtnMmbr(DbMap *map, DbAddr *addr, DbMmbr *first) {
uint64_t next = addr->bits, item;
DbMmbr *mmbr;
int redo = 0;

	if (first->sizeIdx < sizeof(mmbrSizes) / sizeof(uint16_t))
	  redo = ++first->sizeIdx;

	if (!(addr->bits = allocBlk(map, mmbrSizes[first->sizeIdx] * sizeof(uint64_t) + sizeof(DbMmbr), true))) {
		fprintf(stderr, "xtnMmbr: out of memory");
		exit(0);
	}

	//	make a new first mmbr

	mmbr = getObj(map, *addr);
	mmbr->max = mmbrSizes[first->sizeIdx];
	mmbr->next.bits = next;

	// transfer items from old to bigger?

	if (redo)
	  for (int idx = 0; idx < first->max; idx++)
		if ((item = first->table[idx]))
		  if (item != ~0LL)
		  	*findMmbr(mmbr, item, true) = item, mmbr->cnt++;

	return mmbr;
}

//	return first available empty slot
//	and increment population count

uint64_t *newMmbr(DbMap *map, DbAddr *addr, uint64_t hash) {
DbMmbr *first = getObj(map, *addr);
uint64_t item;
uint16_t idx;

	if (3 * first->cnt / 2 > mmbrSizes[first->sizeIdx])
	  first = xtnMmbr(map, addr, first);

	idx = hash % first->max;
	first->cnt++;

	while ((item = first->table[idx])) {
	  if (item == ~0LL)
		break;
      if (++idx == first->max)
		idx = 0;
	}

	return first->table + idx;
}

//	set mmbr hash table slot
//	call w/addr locked
//	~0LL > keyVal > 0

uint64_t *setMmbr(DbMap *map, DbAddr *addr, uint64_t keyVal, bool add) {
uint64_t *slot, *test;
DbMmbr *mmbr, *first;

	// initialize empty mmbr set

	if (addr->addr)
	  first = getObj(map, *addr);
	else
	  first = iniMmbr(map, addr, 3);

	//  look in the first table

	slot = findMmbr(first, keyVal, add);
	mmbr = first;

	//	otherwise, examine the remainingn set tables

	if (*slot == 0 || *slot == ~0LL)
	  while ((mmbr = mmbr->next.bits ? getObj(map, mmbr->next) : NULL))
		if (*(test = findMmbr(mmbr, keyVal, false)))
		  return test;

	//	table overflow?

	if (3 * first->cnt / 2 > mmbrSizes[first->sizeIdx]) {
	  first = xtnMmbr(map, addr, first);
	  slot = findMmbr(first, keyVal, true);
	}

	first->cnt++;
	return slot;
}
