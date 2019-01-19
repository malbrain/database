#include "btree2.h"

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

//	allocate btree2 pageNo

uint64_t allocPageNo (Handle *index) {
	return allocObjId(index->map, listFree(index,ObjIdType), listWait(index,ObjIdType));
}

bool recyclePageNo (Handle *index, uint64_t bits) {
	 return addSlotToFrame(index->map, listHead(index,ObjIdType), listWait(index,ObjIdType), bits);
}

uint16_t btree2SlotAlloc(Btree2Page *page, uint32_t totKeySize, uint8_t height) {
uint16_t amt = (sizeof(Btree2Slot) + totKeySize + (1 << page->skipBits) - 1) / (1 << page->skipBits);
union Btree2Alloc {
	struct {
		uint16_t nxt;	// next skip list storage unit
		Btree2PageState state : 16;
	};
	uint32_t pageAlloc[1];
} pagexchg[1];

	pagexchg->nxt = 65535;
	return pagexchg->nxt;
}

uint64_t allocPage(Handle *index, int type, uint32_t size) {
	return allocObj(index->map, listFree(index, type), listWait(index,type), type, size, false);
}

bool recyclePage(Handle *index, int type, uint64_t bits) {
	return addSlotToFrame(index->map, listHead(index,type), listWait(index,type), bits);
}

// split the root and raise the height of the btree2
// call with key for smaller half and right page addr and root statuslocked.

// DbStatus btree2SplitRoot(Handle *index, Btree2Set *root, DbAddr right, uint8_t *leftKey) { }

//split set->page splice pages left to right

DbStatus btree2SplitPage (Handle *index, Btree2Set *set) {
Btree2Index *btree2 = btree2index(index->map);
Btree2Page *leftPage, *rightPage;
uint8_t leftKey[Btree2_maxkey];
Btree2Slot *rSlot, *lSlot;
ObjId lPageNo, rPageNo;
uint16_t off, keyLen;
DbAddr left, right;
ObjId *pageNoPtr;
uint16_t *tower;
uint8_t *key;
DbStatus stat;

	if( (left.bits = btree2NewPage(index, set->page->lvl, set->page->pageType)) )
		leftPage = getObj(index->map, left);
	else
		return DB_ERROR_outofmemory;

	if( (right.bits = btree2NewPage(index, set->page->lvl, set->page->pageType)) )
		rightPage = getObj(index->map, right);
	else
		return DB_ERROR_outofmemory;

	//	copy over smaller first half keys from old page into new left page

	tower = set->page->skipHead;

	while( leftPage->nxt > leftPage->size / 2 )
		if( (off = tower[0]) ) {
			lSlot = slotptr(set->page,off);
			if( install8(lSlot->state, Btree2_slotactive, Btree2_slotmoved) == Btree2_slotactive )
				btree2InstallSlot(leftPage, lSlot);
			tower = lSlot->tower;
		} else
			break;

	//	splice pages together

	rightPage->left.bits = leftPage->pageNo.bits;
	leftPage->right.bits = rightPage->pageNo.bits;

	//	copy over remaining slots from old page into new right page

	while( rightPage->nxt > rightPage->size / 2)
		if( (off = tower[0]) ) {
			rSlot = slotptr(set->page,off);
			if( install8(&rSlot->state, Btree2_slotactive, Btree2_slotmoved) == Btree2_slotactive )
				btree2InstallSlot(rightPage, rSlot);
			tower = rSlot->tower;
		} else
			break;

	//	allocate a new pageNo for left page,
	//	reuse existing pageNo for right page

	lPageNo.bits = allocObjId(index->map, btree2->freePage, NULL);
	pageNoPtr = fetchIdSlot (index->map, lPageNo);
	pageNoPtr->bits = left.bits;

	//	install left page addr into original pageNo slot

	pageNoPtr = fetchIdSlot(index->map, set->pageNo);
	pageNoPtr->bits = left.bits;

	//	install right page addr into right pageNo slot

	pageNoPtr = fetchIdSlot(index->map, rightPage->pageNo);
	pageNoPtr->bits = right.bits;

	//	extend left fence key with
	//	the left page number on non-leaf page.

	key = slotkey(lSlot);
	keyLen = keylen(key);

	if( set->page->lvl)
		keyLen -= sizeof(uint64_t);		// strip off pageNo

	if( keyLen + sizeof(uint64_t) < 128 )
		off = 1;
	else
		off = 2;

	//	copy leftkey and add its pageNo

	memcpy (leftKey + off, key + keypre(key), keyLen);
	btree2PutPageNo(leftKey + off, keyLen, lPageNo.bits);
	keyLen += sizeof(uint64_t);

	//	insert key for left page in parent

	if( (stat = btree2InsertKey(index, leftKey, keyLen, set->page->lvl + 1, Btree2_slotactive)) )
		return stat;

	//	install right page addr into original pageNo slot

	pageNoPtr = fetchIdSlot(index->map, set->pageNo);
	pageNoPtr->bits = right.bits;

	//	recycle original page to free list
	//	and pageNo

	if(!recyclePageNo(index, set->pageNo.bits))
		return DB_ERROR_outofmemory;

	if( !recyclePage(index, set->page->pageType, set->pageAddr.bits))
		return DB_ERROR_outofmemory;
				
	return DB_OK;
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

	if( !set->page->kill )
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

