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
	base += (uint64_t)hdr->objSize * (idx % ARRAY_size);
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

	base += (uint64_t)size * (idx % ARRAY_size);
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

	//  element payload size must at least hold bit map
	//	of assigned elements in array's idx zero

	if (size < ARRAY_size / 8)
		size = ARRAY_size / 8;

	//	initialize empty array

	if (!array->addr)
		array->bits = allocBlk(map, sizeof(ArrayHdr), true) | ADDR_MUTEX_SET;

	//	get the array header

	hdr = getObj(map, *array);

	//	set the array element payload object size
	//	or check if it matches this array's payload size

	if( hdr->objSize == 0 )
		hdr->objSize = size;

	if (hdr->objSize != size)
		db_abort(hdr, "array element payload size must remain constant", 0);

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
		return (uint16_t)(ARRAY_first(size) + (uint64_t)slot * ARRAY_size);
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
		  *bits = (__builtin_ffsll (~inUse[seg])) - 1;
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
int redo = 0, idx;
DbMmbr *mmbr;

	if (first->sizeIdx < sizeof(mmbrSizes) / sizeof(uint16_t))
	  redo = ++first->sizeIdx;

	if (!(addr->bits = allocBlk(map, (uint32_t)mmbrSizes[first->sizeIdx] * sizeof(uint64_t) + sizeof(DbMmbr), true))) {
		fprintf(stderr, "xtnMmbr: out of memory");
		exit(0);
	}

	//	make a new first mmbr

	mmbr = getObj(map, *addr);
	mmbr->max = mmbrSizes[first->sizeIdx];
	mmbr->next.bits = next;

	// transfer items from old to bigger?

	if (redo)
	  for (idx = 0; idx < first->max; idx++)
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
