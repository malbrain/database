#include "btree2.h"
#include "btree2_slot.h"

bool btree2LinkTower(Btree2Page *page, uint16_t *prev, uint8_t *bitLatch, uint8_t idx, uint16_t off );

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

	slotSize = btree2SizeSlot (keyLen, height);
	memset(set, 0, sizeof(set));

	while (true) {
	  if ((stat = btree2LoadPage(index->map, set, keyBuff, keyLen, lvl)))
		return stat;

	  if( (off = btree2AllocSlot(set->page, slotSize)) )
	    return  btree2InstallKey(index, set, off, keyBuff, keyLen, height);

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

// DbStatus btreeNewRoot (index, rightPage, lvl)

DbStatus btree2NewRoot(Handle *index, Btree2Page *rightPage, int lvl) {
Btree2Index *btree2 = btree2index(index->map);
uint32_t keyLen, newLen = 0;
Btree2Slot *slot, *source;
Btree2Page *rootPage;
ObjId *rootObjId;
uint8_t *key, *dest;
uint16_t off, height = 1;
DbAddr root;

	if( (root.bits = btree2NewPage (index, lvl + 1)) )
		rootPage = getObj (index->map, root);
	else
		return DB_ERROR_outofmemory;

	rootPage->pageNo.bits = btree2->root.bits;
	rootPage->attributes = Btree2_rootPage;

 	source = slotptr(rightPage, rightPage->fence);

	key = slotkey(source);
	keyLen = keylen(key);

	//	strip pageno from the key in non-leaf pages

	if( lvl > 0 )
		keyLen -= size64(keystr(key), keyLen);

	newLen = calc64(rightPage->pageNo.bits, btree2->base->binaryFlds) + keyLen;

	newLen = btree2SizeSlot (newLen, height);

	if( (off = btree2AllocSlot(rootPage, newLen)) )
		slot = slotptr(rootPage, off);
	else
		return DB_ERROR_pageisgarbage;

	slot->height = 1;
	*slot->state = Btree2_slotactive;

	*rootPage->height = 1;
	rootPage->towerHead[0] = off;

	dest = slotkey(slot);

	if( newLen > 128 * 256 )
		return DB_ERROR_keylength;

	if( newLen > 125 )
		*dest++ = (newLen + 2) >> 8 | 0x80, *dest++ = newLen + 2;
	else
		*dest++ = newLen + 1;																						
	memcpy(dest, keystr(key), keyLen);
	store64(dest, keyLen, rightPage->pageNo.bits, btree2->base->binaryFlds);

	//   install new root page
	
	rootObjId = fetchIdSlot(index->map, rootPage->pageNo);
	rootObjId->bits = root.bits;
	return DB_OK;
}

//	split set->page and splice two new pages left and right
//	reuse existiing pageNo for new left page until proper left fence key
//	is installed in parent with the new left pageNo, then switch original pageNo
//	to point at new right page.

//	Note:  this function has been optimized and is not thread safe. (the fwd array in specific)


DbStatus btree2SplitPage (Handle *index, Btree2Set *set) {
uint16_t *tower, fwd[Btree2_maxskip];
uint8_t *lKey, lvl = set->page->lvl;
Btree2Page *leftPage, *rightPage;
Btree2Slot *rSlot, *lSlot;
uint16_t lKeyLen, max;
DbAddr left, right;
ObjId *lPageNoPtr;
ObjId *rPageNoPtr;
DbStatus stat;

	if( (left.bits = btree2NewPage(index, lvl)) )
		leftPage = getObj(index->map, left);
	else
		return DB_ERROR_outofmemory;

	if( (right.bits = btree2NewPage(index, lvl)) )
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

	//	reuse existing pageNo for right page 
	//	and allocate new pageNo for left page

	rightPage->pageNo.bits = set->page->pageNo.bits;
    rPageNoPtr = fetchIdSlot(index->map, rightPage->pageNo);

	if ((leftPage->pageNo.bits = btree2AllocPageNo(index)))
		lPageNoPtr = fetchIdSlot(index->map, leftPage->pageNo);
	else
		return DB_ERROR_outofmemory;

	//	splice pages together

	rightPage->left.bits = leftPage->pageNo.bits;
    rightPage->right.bits = set->page->right.bits;

	leftPage->right.bits = rightPage->pageNo.bits;
    leftPage->left.bits = set->page->left.bits;

	//	install left page addr into original pageNo slot and
	//	into new left pageNo (as soon as right page is installed
	//	its backward link will go to left page)

    lPageNoPtr->bits = left.bits;
    rPageNoPtr->bits = left.bits;

	//	insert left fence key into parent 

    lKey = slotkey(lSlot);
	lKeyLen = keylen(lKey);

	if( lvl )
		lKeyLen -= size64(lKey, lKeyLen);		// strip off pageNo

	if( lvl + 1 > set->rootLvl )
		if( (stat = btree2NewRoot (index, rightPage, lvl)) )
			return stat;

	if( (stat = btree2InsertKey(index, keystr(lKey), lKeyLen, leftPage->pageNo.bits, lvl + 1, Btree2_slotactive)) )
		return stat;

	//	install right page addr into original pageNo slot

	rPageNoPtr->bits = right.bits;

	//	recycle original page to free list

	if( !btree2RecyclePage(index, set->page->pageType, set->pageAddr) )
		return DB_ERROR_outofmemory;
				
	return DB_OK;
}

void btree2SetPageTower (Btree2Page *page, uint16_t height) {
uint8_t prevHeight;

	while( prevHeight = *page->height, height > prevHeight )
	  if (atomicCAS8(page->height, prevHeight, height))
		break;

}

//	move slot from one page to another page
//	note:  this function is not thread safe

uint16_t btree2InstallSlot (Handle *index, Btree2Page *page, Btree2Slot *source, uint16_t *fwd) {
uint8_t height = btree2GenHeight(index);
uint8_t *key;
uint32_t keyLen;
uint32_t slotSize;
Btree2Slot *slot;
Btree2Slot *prev;
uint16_t off;
uint8_t *dest;
int idx;

	key = slotkey(source);
	keyLen = keylen(key);

	slotSize = btree2SizeSlot(keyLen, height);

	if( (off = btree2AllocSlot(page, slotSize)) )
		slot = slotptr(page, off);
	else
		return 0;

	//	fill in new slot attributes & tower

	*slot->state = Btree2_slotactive;
	slot->height = height;

	// copy slot onto page

	dest = slotkey(slot);

	if( keyLen < 127 )	
		*dest++ = keyLen + 1;
	else
		*dest++ = (keyLen + 2) >> 8 | 0x80, *dest++ = keyLen + 2;

	memcpy (dest, keystr(key), keyLen);

	for( idx = 0; idx < height; idx++ ) {
	  if( fwd[idx] ) {
		prev = slotptr(page, fwd[idx]);
		prev->tower[idx] = off;
	  } else
		if( idx > *page->height )
		  btree2SetPageTower(page, idx);

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
uint8_t *dest, prevHeight, tst;
uint16_t prevFwd;
int idx = 0, lenOff;

	// install slot into page

	*slot->state = Btree2_slotactive;
	slot->height = height;

	dest = slotkey(slot);

	if( keyLen < 127 )	
		*dest++ = keyLen + 1;
	else
		*dest++ = (keyLen + 2) >> 8 | 0x80, *dest++ = keyLen + 2;

	//	fill in new slot attributes

	memcpy (dest, key, keyLen);

	//  splice new slot into tower structure

	while( idx < height ) {

		//	prevSlot[idx] > 0 => prevFwd is last key smaller than new key
		//	scan right from prevFwd until next slot has a larger key

		while( *set->page->height < idx ) {
			tst = *set->page->height;
			lockLatchGrp (set->page->bitLatch, tst);

			if( !set->page->towerHead[tst] )
				set->page->towerHead[tst] = off;

			atomicCAS8(set->page->height, tst, tst + 1);
			unlockLatchGrp(set->page->bitLatch, tst);
		}

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

bool btree2LinkTower(Btree2Page *page, uint16_t *tower, uint8_t *bitLatch, uint8_t idx, uint16_t off ) {
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

  } while( btree2KeyCmp (slotkey(next), keystr(key), keyLen) > 0 );

	//  link forward pointers

	slot->tower[idx] = tower[idx];
	tower[idx] = off;

	if( prevLatch )
		unlockLatchGrp (prevLatch, idx);

	unlockLatchGrp(bitLatch, idx);
	return true;
}
