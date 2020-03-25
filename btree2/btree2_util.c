#include "btree2.h"
#include "btree2_slot.h"
#include <stdlib.h>

//	debug slot function

#ifdef DEBUG
Btree2Slot *btree2Slot(Btree2Page *page, uint32_t off)
{
	return slotptr(page, off);
}

uint8_t *btree2Key(Btree2Slot *slot)
{
	return slotkey(slot);
}

#undef slotkey
#undef slotptr
#define slotkey(s) btree2Key(s)
#define slotptr(p,x) btree2Slot(p,x)
#endif

//	calc size of slot

uint32_t btree2SlotSize(Btree2Slot *slot, uint8_t skipBits, uint8_t height)
{
uint8_t *key = slotkey(slot);
uint32_t size;

	size = sizeof(*slot) + keylen(key) + keypre(key);
	size += (height ? height : slot->height) * sizeof(uint16_t);
	return size;
}

uint32_t lcg_parkmiller(uint32_t *state);

// generate slot tower height (1-15)
//	w/frequency 1/2 down to 1/65536


uint32_t btree2GenHeight(Handle *index) {
Btree2HandleXtra *hndlXtra = ((Btree2HandleXtra *)(index + 1));
uint32_t nrand32 = mynrand48(hndlXtra->nrandState);
  // uint32_t nrand32 = lcg_parkmiller(index->lcgState);

	nrand32 |= 0x10000;

#ifdef _WIN32
	return __lzcnt(nrand32);
#else
	return __builtin_clz(nrand32);
#endif
}

//	calculate amount of space needed to install slot in page
//	include key length bytes, and tower height

uint32_t btree2SizeSlot (uint32_t keySize, uint8_t height)
{
uint32_t amt = (uint16_t)(sizeof(Btree2Slot) + height * sizeof(uint16_t) + keySize);

	return amt + (keySize > 127 ? 2 : 1);
}

// allocate space for new slot (in skipBits units)

uint16_t btree2AllocSlot(Btree2Page *page, uint32_t bytes) {
uint16_t base = (sizeof(*page) + (1ULL << page->skipBits) - 1) >> page->skipBits;
uint16_t size = (uint16_t)(bytes + (1ULL << page->skipBits) - 1) >> page->skipBits;
union Btree2Alloc alloc[1], before[1];

	do {
		*before->word = *page->alloc->word;
		*alloc->word = *before->word;

		if( alloc->nxt > base + size )
	  	  if( alloc->state == Btree2_pageactive )
			alloc->nxt -= size;
		  else
			return 0;
		else
			return 0;

	} while( !atomicCAS32(page->alloc->word, *before->word, *alloc->word) );

	return alloc->nxt;
}

uint64_t btree2Get64 (Btree2Slot *slot) {
uint8_t *key = slotkey(slot);

	return get64 (keystr(key), keylen(key));
}

uint32_t btree2Store64 (Btree2Slot *slot, uint64_t value) {
uint8_t *key = slotkey(slot);

	return store64(keystr(key), keylen(key), value);
}
