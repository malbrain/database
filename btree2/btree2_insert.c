#include "btree2.h"
#include "btree2_slot.h"

bool btree2LinkTower(Btree2Page *page, uint16_t *prev, uint8_t *bitLatch, int idx, uint16_t off );

DbStatus btree2InsertKey(Handle *index, uint8_t *key, uint32_t keyLen, uint64_t suffixValue, uint8_t lvl, Btree2SlotState type) {
Btree2Index *btree2 = btree2index(index->map);
uint8_t height = btree2GenHeight(index);
uint8_t keyBuff[MAX_key];
uint32_t slotSize;
Btree2Set set[1];
DbStatus stat;
uint16_t off;

	if (keyLen > MAX_key)
		return DB_ERROR_keylength;

	memcpy(keyBuff, key, keyLen);
    keyLen += store64 (keyBuff, keyLen, suffixValue, btree2->base->binaryFlds);

	memset(set, 0, sizeof(set));

	while (true) {
	  if ((stat = btree2LoadPage(index->map, set, keyBuff, keyLen, lvl)))
		return stat;

	  slotSize = btree2SizeSlot(set->page, keyLen, height);

	  if( (off = btree2AllocSlot(set->page, slotSize)) )
	    return btree2InstallKey (index, set, off, keyBuff, keyLen, height);

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
uint32_t size = btree2->pageSize, max;
Btree2Page *newPage;
Btree2Slot *slot;
DbAddr addr;
ObjId *pageNoPtr;

	//	skip cleanup and proceed directly to split
	//	if there's not enough garbage
	//	to bother with.

	if( *set->page->garbage < size / 5 )
		return btree2SplitPage(index, set);

	if( (addr.bits = btree2NewPage(index, set->page->lvl)) )
		newPage = getObj(index->map, addr);
	else
		return DB_ERROR_outofmemory;

	//	copy over keys from old page into new page

	max = newPage->size >> newPage->skipBits;
	memset(fwd, 0, sizeof(fwd));
	tower = set->page->towerHead;

	while (newPage->alloc->nxt < max / 2 && tower[0]) {
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
// call with key for smaller half and right page addr and root status locked.

// DbStatus btree2SplitRoot(Handle *index, Btree2Set *root, DbAddr right, uint8_t *leftKey) { }

//	split set->page and splice two new pages left and right
//	reuse existiing pageNo for new left page until proper left fence key
//	is installed in parent with the new left pageNo, then switch original pageNo
//	to point at new right page.

//	Note:  this function has been optimized and is not thread safe. (the fwd array in specific)

DbStatus btree2SplitPage (Handle *index, Btree2Set *set) {
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

	if( (left.bits = btree2NewPage(index, set->page->lvl)) )
		leftPage = getObj(index->map, left);
	else
		return DB_ERROR_outofmemory;

	if( (right.bits = btree2NewPage(index, set->page->lvl)) )
		rightPage = getObj(index->map, right);
	else
		return DB_ERROR_outofmemory;

	//	copy over smaller first half keys from old page into new left page

	tower = set->page->towerHead;
	memset (fwd, 0, sizeof(fwd));
	max = leftPage->size >> leftPage->skipBits;

	while( leftPage->alloc->nxt > max / 2 && tower[0] ) {
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
	max = rightPage->size >> rightPage->skipBits;

	while( rightPage->alloc->nxt > max / 2 && tower[0] ) {
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
		keyLen -= size64(key, keyLen);		// strip off pageNo

	//	insert key for left page in parent

	if( (stat = btree2InsertKey(index, key, keyLen, lPageNo.bits, set->page->lvl + 1, Btree2_slotactive)) )
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

//	move slot from one page to another page
//	note:  this function is not thread safe

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
		page->towerHead[idx] = off;

	  fwd[idx] = off;
	}

	if( height > *page->height )
		*page->height = height;

	return off;
}

//	install new key onto page at assigned offset,  fill-in skip list tower slots
//	this function IS thread safe, and uses latches for synchronization

DbStatus btree2InstallKey (Handle *index, Btree2Set *set, uint16_t off, uint8_t *key, uint32_t keyLen, uint8_t height) {
Btree2Slot *slot = slotptr(set->page, off), *prev;
uint8_t *ptr, prevHeight;
uint16_t prevFwd;
int idx;

	// install slot into page

	ptr = slotkey(slot);

	if( keyLen < 128 )	
		*ptr++ = keyLen;
	else
		*ptr++ = keyLen/256 | 0x80, *ptr++ = keyLen;

	//	fill in new slot attributes

	memcpy (ptr, key, keyLen);
	*slot->state = Btree2_slotactive;
	slot->height = height;

	while( prevHeight = *set->page->height, height > prevHeight )
	  if (atomicCAS8(set->page->height, prevHeight, height))
		break;

	//  splice new slot into tower structure

	for( idx = 0; idx < height; ) {

		//	prevSlot[idx] > 0 => prevFwd is last key smaller than new key
		//	scan right from prevFwd until next slot has a larger key

		if( (prevFwd = set->prevSlot[idx]) ) {
			prev = slotptr(set->page, prevFwd); 

			if( btree2LinkTower(set->page, prev->tower, prev->bitLatch, idx, off) )
				idx += 1;

			continue;

		//	page->towerHead[idx] > 0 => prevFwd is smallest key in the index
		//	lock page->bitLatch 

		} else if( set->page->towerHead[idx] ) {
			if( btree2LinkTower(set->page, set->page->towerHead, set->page->bitLatch, idx, off ) )
				idx += 1;

			continue;

		//	both above failed => add first key in empty index

		} else {
			lockLatchGrp (set->page->bitLatch, idx);

			if( set->page->towerHead[idx] ) {
				unlockLatchGrp(set->page->bitLatch, idx);
				continue;
			}

			set->page->towerHead[idx] = off;
			unlockLatchGrp (set->page->bitLatch, idx++);
			continue;
		}
	}

	return DB_OK;
}

//	link slot into one lvl of tower list

bool btree2LinkTower(Btree2Page *page, uint16_t *tower, uint8_t *bitLatch, int idx, uint16_t off ) {
Btree2Slot *next, *slot = slotptr(page, off);
uint8_t *key = slotkey(slot), *prevLatch = NULL;
uint32_t keyLen = keylen(key);
uint16_t *prevTower;

  do {
	lockLatchGrp(bitLatch, idx);

	if( tower[idx] )
		next = slotptr(page, tower[idx]);
	else
		break;

	if( prevLatch )
		unlockLatchGrp (prevLatch, idx);

	prevLatch = bitLatch;
	prevTower = tower;

	bitLatch = next->bitLatch;
	tower = next->tower;

	//  if another node was inserted on the left,
	//	continue to the right until it is greater

	//  compare two keys, return > 0, = 0, or < 0
	//  =0: all key fields are same
	//  -1: key2 > key1
	//  +1: key2 < key1

  } while( btree2KeyCmp (slotkey(next), key, keyLen) > 0 );

	//  link forward pointers

	slot->tower[idx] = tower[idx];
	tower[idx] = off;

	if( prevLatch )
		unlockLatchGrp (prevLatch, idx);

	unlockLatchGrp(bitLatch, idx);
	return true;
}
