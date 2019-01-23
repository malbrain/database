#include "btree2.h"
#include "btree2_slot.h"

DbStatus btree2InsertKey(Handle *index, uint8_t *key, uint32_t keyLen, uint8_t lvl, Btree2SlotState type) {
uint8_t height = btree2GenHeight(index);
uint32_t totKeyLen = keyLen, slotSize;
Btree2Set set[1];
DbStatus stat;
uint16_t off;

	if (keyLen < 128)
		totKeyLen += 1;
	else
		totKeyLen += 2;

	memset(set, 0, sizeof(set));

	while (true) {
	  if ((stat = btree2LoadPage(index, set, key, keyLen, lvl)))
		return stat;

	  slotSize = btree2SizeSlot(set->page, totKeyLen, height);

	  if( (off = btree2AllocSlot(set->page, slotSize)) )
	    return btree2InstallKey (index, set, off, key, keyLen, height);

	  if ((stat = btree2CleanPage(index, set)))
		return stat;
	}

	return DB_OK;
}

//	check page for space available,
//	clean if necessary and return
//	>0 number of skip units needed
//	=0 split required

DbStatus btree2CleanPage(Handle *index, Btree2Set *set) {
Btree2Index *btree2 = btree2index(index->map);
uint16_t skipUnit = 1 << set->page->skipBits;
uint32_t size = btree2->pageSize;
int type = set->page->pageType;
Btree2Page *newPage;
Btree2Slot *slot;
DbAddr addr;
ObjId *pageNoPtr;
uint16_t *tower, off;


	if( !set->page->lvl )
		size <<= btree2->leafXtra;

	//	skip cleanup and proceed directly to split
	//	if there's not enough garbage
	//	to bother with.

	if( *set->page->garbage < size / 5 )
		return btree2SplitPage(index, set);

	if( (addr.bits = btree2NewPage(index, set->page->lvl, set->page->pageType)) )
		newPage = getObj(index->map, addr);
	else
		return DB_ERROR_outofmemory;

	//	copy over keys from old page into new page

	tower = set->page->skipHead;

	while( newPage->alloc->nxt < newPage->size / 2 )
		if( (off = tower[0]) ) {
			slot = slotptr(set->page,off);
			if( atomicCAS8(slot->state, Btree2_slotactive, Btree2_slotmoved) )
				btree2InstallSlot(index, newPage, slot, btree2GenHeight(index));
			tower = slot->tower;
		} else
			break;

	//	install new page addr into original pageNo slot

	pageNoPtr = fetchIdSlot(index->map, set->pageNo);
	pageNoPtr->bits = addr.bits;
	return DB_OK;
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
				btree2InstallSlot(index, leftPage, lSlot, btree2GenHeight(index));
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
				btree2InstallSlot(index, rightPage, rSlot, btree2GenHeight(index));
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

	memset (set, 0, sizeof(*set));

	if ((stat = btree2LoadPage(index, set, fenceKey + keypre(fenceKey), keyLen - sizeof(uint64_t), lvl)))
		return stat;

	ptr = slotkey(set->slot);

	// update child pageNo

	assert(!memcmp(ptr, fenceKey, keyLen + keypre(fenceKey) - sizeof(uint64_t)));
	assert(keylen(ptr) == keyLen);

	memcpy(ptr + keypre(ptr) + keylen(ptr) - sizeof(uint64_t), fenceKey + keypre(fenceKey) + keyLen - sizeof(uint64_t), sizeof(uint64_t));

	return DB_OK;
}

//	install slot onto page

uint16_t btree2InstallSlot (Handle *index, Btree2Page *page, Btree2Slot *slot, uint8_t height) {
uint32_t slotSize, totKeyLen;
uint8_t *key = slotkey(slot);
uint32_t keyLen = keylen(key);
Btree2Set set[1];
DbStatus stat;
uint16_t off;

	totKeyLen = keylen(key) + keypre(key);
	memset (set, 0, sizeof(*set));

	while( true ) {
	  if ((stat = btree2LoadPage(index, set, key + keypre(key), keylen(key), page->lvl)))
		return stat;

	  slotSize = btree2SizeSlot(set->page, totKeyLen, height);

	  if( (off = btree2AllocSlot(set->page, slotSize)) )
	    return btree2InstallKey (index, set, off, key, keyLen, height);

	  if ((stat = btree2CleanPage(index, set)))
		return stat;
	}
}

//	install new key onto page
//	return page offset, or zero

bool btree2InstallKey (Handle *index, Btree2Set *set, uint16_t off, uint8_t *key, uint32_t keyLen, uint8_t height) {
Btree2Slot *slot = slotptr(set->page, off);
uint8_t *ptr;

	// copy slot onto page

	ptr = slotkey(slot);

	if( keyLen < 128 )	
		*ptr++ = keyLen;
	else
		*ptr++ = keyLen/256 | 0x80, *ptr++ = keyLen;

	//	fill in new slot

	memcpy (ptr, key, keyLen);
	slot->slotState = Btree2_slotactive;
	slot->height = height;

	if( btree2FillTower(set, 0) )
		return off;

	return 0;
}

