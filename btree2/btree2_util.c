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

uint16_t btree2SlotSize(Btree2Slot *slot, uint8_t skipBits)
{
uint8_t *key = slotkey(slot);
uint32_t size;

	size = sizeof(*slot) + slot->height * sizeof(uint16_t) + keylen(key);
	size += (1 << skipBits) - 1;
	size >>= skipBits;
	return size;
}

// generate slot tower height

uint8_t btree2GenHeight(Handle *index) {
uint32_t nrand32 = mynrand48(index->nrandState);
unsigned long height;

#ifdef _WIN32
	_BitScanReverse((unsigned long *)&height, nrand32);
	height++;
#else
	height = __builtin_clz(nrand32);
#endif
	return height % Btree2_maxskip + 1;
}

//	allocate btree2 pageNo

uint64_t btree2AllocPageNo (Handle *index) {
	return allocObjId(index->map, listFree(index,ObjIdType), listWait(index,ObjIdType));
}

bool btree2RecyclePageNo (Handle *index, uint64_t bits) {
	 return addSlotToFrame(index->map, listHead(index,ObjIdType), listWait(index,ObjIdType), bits);
}

uint16_t btree2SizeSlot (Btree2Page *page, uint32_t totKeySize, uint8_t height)
{
uint16_t amt = (uint16_t)(sizeof(Btree2Slot) + height * sizeof(uint16_t) + totKeySize);

	amt += (1LL << page->skipBits) - 1;
	return amt >> page->skipBits;
}

// allocate space for new slot
//	return page offset or zero on overflow

uint16_t btree2AllocSlot(Btree2Page *page, uint16_t size) {
union Btree2Alloc alloc[1], before[1];

	do {
		*before->word = *page->alloc->word;
		*alloc->word = *before->word;

		if( alloc->nxt > size && alloc->nxt - size > sizeof(*page) )
	  	  if( alloc->state == Btree2_pageactive )
			alloc->nxt -= size;
		  else
			return 0;
		else
			return 0;

	} while( !atomicCAS32(page->alloc->word, *alloc->word, *before->word) );

	return alloc->nxt;
}

bool btree2RecyclePage(Handle *index, int type, uint64_t bits) {
	return addSlotToFrame(index->map, listHead(index,type), listWait(index,type), bits);
}

//  find and load page at given level for given key
//	leave page rd or wr locked as requested

DbStatus btree2LoadPage(DbMap *map, Btree2Set *set, uint8_t *key, uint32_t keyLen, uint8_t lvl) {
Btree2Index *btree2 = btree2index(map);
uint8_t drill = 0xff, *ptr;
Btree2Page *prevPage = NULL;
ObjId prevPageNo;
ObjId *pageNoPtr;

  set->pageNo.bits = btree2->root.bits;
  pageNoPtr = fetchIdSlot (map, set->pageNo);
  set->pageAddr.bits = pageNoPtr->bits;
  prevPageNo.bits = 0;

  //  start at our idea of the root level of the btree2 and drill down

  do {
	set->parent.bits = prevPageNo.bits;
	set->page = getObj(map, set->pageAddr);

//	if( set->page->free )
//		return DB_BTREE_error;

	assert(lvl <= set->page->lvl);

	//  find key on page at this level
	//  and descend to requested level

	if( (btree2FindSlot (set, key, keyLen)) ) {
	  if( drill == lvl )
		return DB_OK;

	  // find next non-dead slot -- the fence key if nothing else

	  btree2SkipDead (set);

	  // get next page down

	  ptr = slotkey(set->slot);
	  set->pageNo.bits = btree2GetPageNo(ptr + keypre(ptr), keylen(ptr));

	  assert(drill > 0);
	  drill--;
	  continue;
	 }

	//  or slide right into next page

	set->pageNo.bits = set->page->right.bits;
  } while( set->pageNo.bits );

  // return error on end of right chain

  return DB_BTREE_error;
}

