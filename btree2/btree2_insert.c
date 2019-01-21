#include "btree2.h"
#include "btree2_slot.h"

DbStatus btree2InsertKey(Handle *index, void *key, uint32_t keyLen, uint8_t lvl, Btree2SlotState type) {
uint8_t height = btree2GenHeight(index);
uint32_t totKeyLen = keyLen;
Btree2Set set[1];
DbStatus stat;

	if (keyLen < 128)
		totKeyLen += 1;
	else
		totKeyLen += 2;

	while (true) {
	  if ((stat = btree2LoadPage(index->map, set, key, keyLen - (lvl ? sizeof(uint64_t) : 0), lvl)))
		return stat;

	  if ((stat = btree2CleanPage(index, set, totKeyLen, height))) {
		if (stat == DB_BTREE_needssplit) {
		  if ((stat = btree2SplitPage(index, set, height)))
			return stat;
		  else
			continue;
	    } else
			return stat;
	  }

	  // add the key to the page

	  return btree2InstallKey (set->page, key, keyLen, height);
	}

	return DB_OK;
}

//	check page for space available,
//	clean if necessary and return
//	>0 number of skip units needed
//	=0 split required

DbStatus btree2CleanPage(Handle *index, Btree2Set *set, uint32_t totKeyLen, uint8_t height) {
Btree2Index *btree2 = btree2index(index->map);
uint16_t skipUnit = 1 << set->page->skipBits;
uint32_t spaceReq, size = btree2->pageSize;
int type = set->page->pageType;
Btree2Page *newPage;
Btree2Slot *slot;
DbAddr addr;
ObjId *pageNoPtr;
uint16_t *tower, off;


	if( !set->page->lvl )
		size <<= btree2->leafXtra;

	spaceReq = (sizeof(Btree2Slot) + set->height * sizeof(uint16_t) + totKeyLen + skipUnit - 1) / skipUnit;

	if( set->page->alloc->nxt + spaceReq <= size)
		return spaceReq;

	//	skip cleanup and proceed directly to split
	//	if there's not enough garbage
	//	to bother with.

	if( *set->page->garbage < size / 5 )
		return DB_BTREE_needssplit;

	if( (addr.bits = btree2NewPage(index, set->page->lvl, set->page->pageType)) )
		newPage = getObj(index->map, addr);
	else
		return DB_ERROR_outofmemory;

	//	copy over keys from old page into new page

	tower = set->page->skipHead;

	while( newPage->alloc->nxt < newPage->size / 2 )
		if( (off = tower[0]) ) {
			slot = slotptr(set->page,off);
			if( atomicCAS8(slot->state, Btree2_slotactive, Btree2_slotmoved) == Btree2_slotactive )
				btree2InstallSlot(newPage, slot, height);
			tower = slot->tower;
		} else
			break;

	//	install new page addr into original pageNo slot

	pageNoPtr = fetchIdSlot(index->map, set->pageNo);
	pageNoPtr->bits = addr.bits;

	return DB_BTREE_needssplit;
}

// split the root and raise the height of the btree2
// call with key for smaller half and right page addr and root statuslocked.

// DbStatus btree2SplitRoot(Handle *index, Btree2Set *root, DbAddr right, uint8_t *leftKey) { }

//split set->page splice pages left to right

DbStatus btree2SplitPage (Handle *index, Btree2Set *set, uint8_t height) {
Btree2Index *btree2 = btree2index(index->map);
Btree2Page *leftPage, *rightPage;
uint8_t leftKey[Btree2_maxkey];
Btree2Slot *rSlot, *lSlot;
uint16_t off, keyLen, max;
DbAddr left, right;
ObjId *pageNoPtr;
ObjId lPageNo;
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
	max = leftPage->size / 2 >> set->page->skipBits;

	while( leftPage->alloc->nxt > max )
		if( (off = tower[0]) ) {
			lSlot = slotptr(set->page,off);
			if( atomicCAS8(lSlot->state, Btree2_slotactive, Btree2_slotmoved) )
				btree2InstallSlot(leftPage, lSlot, height);
			tower = lSlot->tower;
		} else
			break;

	//	splice pages together

	rightPage->left.bits = leftPage->pageNo.bits;
	leftPage->right.bits = rightPage->pageNo.bits;

	//	copy over remaining slots from old page into new right page

	while( rightPage->alloc->nxt > rightPage->size / 2)
		if( (off = tower[0]) ) {
			rSlot = slotptr(set->page,off);
			if( atomicCAS8(rSlot->state, Btree2_slotactive, Btree2_slotmoved) )
				btree2InstallSlot(rightPage, rSlot, height);
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

	if(!btree2RecyclePageNo(index, set->pageNo.bits))
		return DB_ERROR_outofmemory;

	if( !btree2RecyclePage(index, set->page->pageType, set->pageAddr.bits))
		return DB_ERROR_outofmemory;
				
	return DB_OK;
}

//	update page's fence key in its parent

DbStatus btree2FixKey (Handle *index, uint8_t *fenceKey, uint8_t lvl) {
uint32_t keyLen = keylen(fenceKey);
Btree2Set set[1];
uint8_t *ptr;
DbStatus stat;

	if ((stat = btree2LoadPage(index->map, set, fenceKey + keypre(fenceKey), keyLen - sizeof(uint64_t), lvl)))
		return stat;

	ptr = slotkey(set->slot);

	// update child pageNo

	assert(!memcmp(ptr, fenceKey, keyLen + keypre(fenceKey) - sizeof(uint64_t)));
	assert(keylen(ptr) == keyLen);

	memcpy(ptr + keypre(ptr) + keylen(ptr) - sizeof(uint64_t), fenceKey + keypre(fenceKey) + keyLen - sizeof(uint64_t), sizeof(uint64_t));

	return DB_OK;
}

uint16_t btree2InstallSlot (Btree2Page *page, Btree2Slot *slot, uint8_t height) {
	return btree2InstallKey (page, slotkey(slot), keylen(slotkey(slot)), height);
}

//	install new key onto page
//	return page offset, or zero

uint16_t btree2InstallKey (Btree2Page *page, uint8_t *key, uint32_t keyLen, uint8_t height) {
uint32_t prefixLen, slotSize;
Btree2Slot *newSlot;
uint16_t off;
uint8_t *ptr;

	//	calculate key length

	keyLen = keylen(key);
	prefixLen = keyLen < 128 ? 1 : 2;

	//	allocate space on page

	slotSize = btree2SizeSlot(page, prefixLen + keyLen, height); 

	// copy slot onto page

	if( (off = btree2AllocSlot(page, slotSize)) ) {
		newSlot = slotptr(page, off);
		ptr = slotkey(newSlot);

		if( keyLen < 128 )	
			*ptr++ = keyLen;
		else
			*ptr++ = keyLen/256 | 0x80, *ptr++ = keyLen;

		//	fill in new slot

		memcpy (ptr, key, keyLen);
		newSlot->slotState = Btree2_slotactive;
		newSlot->height = height;
	}

	return off;
}

