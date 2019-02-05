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
	  if ((stat = btree2LoadPage(index->map, set, key, keyLen, lvl)))
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
uint16_t *tower, fwd[Btree2_maxskip];
uint32_t size = btree2->pageSize;
Btree2Page *newPage;
Btree2Slot *slot;
DbAddr addr;
ObjId *pageNoPtr;


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

	memset (fwd, 0, sizeof(fwd));
	tower = set->page->head;

	while( newPage->alloc->nxt < newPage->size / 2 && tower[0] ) {
		slot = slotptr(set->page, tower[0]);
		if (atomicCAS8(slot->state, Btree2_slotactive, Btree2_slotmoved)) {
			if ((newPage->fence = btree2InstallSlot(index, newPage, slot, fwd)))
				tower = slot->tower;
			else
				return DB_ERROR_indexnode;
		}
	}

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
uint16_t *tower, fwd[Btree2_maxskip];
Btree2Page *leftPage, *rightPage;
uint8_t leftKey[Btree2_maxkey];
Btree2Slot *rSlot, *lSlot;
uint16_t off, keyLen, max;
DbAddr left, right;
ObjId *pageNoPtr;
ObjId lPageNo;
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

	tower = set->page->head;
	memset (fwd, 0, sizeof(fwd));
	max = leftPage->size / 2 >> set->page->skipBits;

	while( leftPage->alloc->nxt > max && tower[0] ) {
		lSlot = slotptr(set->page, tower[0]);
		if (atomicCAS8(lSlot->state, Btree2_slotactive, Btree2_slotmoved)) {
			if ((leftPage->fence = btree2InstallSlot(index, leftPage, lSlot, fwd)))
				tower = lSlot->tower;
			else
				return DB_ERROR_indexnode;
		}
	}

	//	splice pages together

	rightPage->left.bits = leftPage->pageNo.bits;
	leftPage->right.bits = rightPage->pageNo.bits;

	//	copy over remaining slots from old page into new right page

	memset (fwd, 0, sizeof(fwd));

	while( rightPage->alloc->nxt > rightPage->size / 2 && tower[0] ) {
		rSlot = slotptr(set->page, tower[0]);
		if( atomicCAS8(rSlot->state, Btree2_slotactive, Btree2_slotmoved) ) {
			if ((rightPage->fence = btree2InstallSlot(index, rightPage, rSlot, fwd)))
				tower = rSlot->tower;
			else
				return DB_ERROR_indexnode;
		}
	}

	//	allocate a new pageNo for left page,
	//	reuse existing pageNo for right page

	if ((lPageNo.bits = btree2AllocPageNo(index)))
		pageNoPtr = fetchIdSlot(index->map, lPageNo);
	else
		return DB_ERROR_outofmemory;

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
	btree2PutPageNo(lSlot,  lPageNo);
	keyLen += sizeof(uint64_t);

	//	insert key for left page in parent

	if( (stat = btree2InsertKey(index, leftKey, keyLen, set->page->lvl + 1, Btree2_slotactive)) )
		return stat;

	//	install right page addr into original pageNo slot

	pageNoPtr = fetchIdSlot(index->map, set->pageNo);
	pageNoPtr->bits = right.bits;

	//	recycle original page to free list
	//	and pageNo

	if( !btree2RecyclePageNo(index, set->pageNo) )
		return DB_ERROR_outofmemory;

	if( !btree2RecyclePage(index, set->page->pageType, set->pageAddr) )
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

	if ((stat = btree2LoadPage(index->map, set, fenceKey + keypre(fenceKey), keyLen - sizeof(uint64_t), lvl)))
		return stat;

	ptr = slotkey(set->slot);

	// update child pageNo

	assert(!memcmp(ptr, fenceKey, keyLen + keypre(fenceKey) - sizeof(uint64_t)));
	assert(keylen(ptr) == keyLen);

	memcpy(ptr + keypre(ptr) + keylen(ptr) - sizeof(uint64_t), fenceKey + keypre(fenceKey) + keyLen - sizeof(uint64_t), sizeof(uint64_t));

	return DB_OK;
}

//	move slot from one page to another page

uint16_t btree2InstallSlot (Handle *index, Btree2Page *page, Btree2Slot *source, uint16_t *fwd) {
uint8_t height = btree2GenHeight(index);
uint8_t *key = slotkey(source);
uint32_t keyLen = keylen(key);
uint32_t slotSize, totKeyLen;
Btree2Slot *slot;
Btree2Slot *prev;
uint16_t off;
uint8_t *ptr;
int idx;

	totKeyLen = keylen(key) + keypre(key);
	slotSize = btree2SizeSlot(page, totKeyLen, height);

	if( (off = btree2AllocSlot(page, slotSize)) )
		slot = slotptr(page, off);
	else
		return 0;

	// copy slot onto page

	ptr = slotkey(slot);

	if( keyLen < 128 )	
		*ptr++ = keyLen;
	else
		*ptr++ = keyLen/256 | 0x80, *ptr++ = keyLen;

	memcpy (ptr, key, keyLen);

	//	fill in new slot attributes & tower

	*slot->state = Btree2_slotactive;
	slot->height = height;

	for( idx = 0; idx < height; idx++ ) {
	  if( fwd[idx] ) {
		prev = slotptr(page, fwd[idx]);
		prev->tower[idx] = off;
	  } else
		page->head[idx] = off;

	  fwd[idx] = off;
	}

	return off;
}

//	install new key onto page at given offset

bool btree2InstallKey (Handle *index, Btree2Set *set, uint16_t off, uint8_t *key, uint32_t keyLen, uint8_t height) {
Btree2Slot *slot = slotptr(set->page, off);
uint8_t *ptr;

	// copy slot onto page

	ptr = slotkey(slot);

	if( keyLen < 128 )	
		*ptr++ = keyLen;
	else
		*ptr++ = keyLen/256 | 0x80, *ptr++ = keyLen;

	//	fill in new slot attributes

	memcpy (ptr, key, keyLen);
	*slot->state = Btree2_slotfill;
	slot->height = height;
	return true;
}

