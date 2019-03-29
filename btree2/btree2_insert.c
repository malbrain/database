#include "btree2.h"
#include "btree2_slot.h"

uint16_t btree2InstallSlot (Btree2Page *page, Btree2Slot *source, uint8_t height);
bool btree2LinkTower(Btree2Set *set, uint16_t *tower, uint8_t *bitLatch, uint8_t idx);
void btree2FillTower(Btree2Page *page, uint16_t off, uint16_t *fwd, uint8_t height);

DbStatus btree2InsertKey(Handle *index, uint8_t *key, uint32_t keyLen, uint32_t sfxLen, uint8_t lvl, Btree2SlotState type) {
Btree2Index *btree2 = btree2index(index->map);
uint32_t slotSize, totLen = keyLen + sfxLen;
uint8_t height = btree2GenHeight(index);
Btree2Set set[1];
DbStatus stat;
uint16_t next;

    assert (height <= 16);
	
	if (totLen > MAX_key)
		return DB_ERROR_keylength;

	slotSize = btree2SizeSlot (totLen, height);

	do {
	  memset(set, 0, sizeof(set));
	  next = btree2LoadPage(index->map, set, key, totLen, lvl);

	  if( (set->off = btree2AllocSlot (set->page, slotSize) ))
		 return btree2InstallKey (set, key, totLen, height);

	   if( (stat = btree2CleanPage (index, set)))
		 return stat;
	} while( true);

	return DB_OK;
}

//	check page for space available,
//	clean or split if necessary

DbStatus btree2CleanPage(Handle *index, Btree2Set *set) {
Btree2Index *btree2 = btree2index(index->map);
uint16_t *tower, fwd[Btree2_maxtower], off;
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

	memset(fwd, 0, sizeof(fwd));
	tower = set->page->towerHead;

	while (tower[0]) {
		uint8_t height = btree2GenHeight(index);
		slot = slotptr (set->page, tower[0]);

		if (atomicCAS8(slot->state, Btree2_slotactive, Btree2_slotmoved)) {
			if( (off = btree2InstallSlot(newPage, slot, height)) )
				btree2FillTower (newPage, off, fwd, height);
			else
				return DB_ERROR_indexnode;
		}

		tower = slot->tower;
	}

	//	install new page addr into original pageNo slot

	pageNoPtr = fetchIdSlot(index->map, set->pageNo);
	pageNoPtr->bits = addr.bits;
	return DB_OK;
}


//	split set->page and splice two new pages left and right
//	reuse existiing pageNo for new right page until proper left fence key
//	is installed in parent with the new left pageNo, then switch original pageNo
//	to point at new right page.

//	Note:  this function has been optimized and is not thread safe. (the fwd array in specific)

DbStatus btree2SplitPage (Handle *index, Btree2Set *set) {
uint8_t *key, lvl = set->page->lvl, keyBuff[MAX_key];
Btree2Page *leftPage, *rightPage, *rootPage = NULL, *tmpPage;
Btree2Index *btree2 = btree2index(index->map);
Btree2Slot *rSlot, *lSlot, *slot;
uint16_t keyLen, max, next, off;
uint16_t fwd[Btree2_maxtower];
DbAddr left, right, root;
DbAddr *tmpPageNoPtrL;
DbAddr *tmpPageNoPtrR;
DbAddr *lPageNoPtr;
DbAddr *rPageNoPtr;
DbAddr *rootObjId;
uint32_t sfxLen;
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

	memset (fwd, 0, sizeof(fwd));
	max = leftPage->size >> leftPage->skipBits;

	if( (next = set->page->towerHead[0]) )
	  do {
		uint8_t height = btree2GenHeight (index);
		lSlot = slotptr(set->page, next);

		if (atomicCAS8(lSlot->state, Btree2_slotactive, Btree2_slotmoved)) {
		  if( (off = btree2InstallSlot(leftPage, lSlot, height)))
			btree2FillTower (leftPage, off, fwd, height);
		  else
			return DB_ERROR_indexnode;
		}

		next = lSlot->tower[0];

	  } while( next && leftPage->alloc->nxt > max / 2 );

	leftPage->rFence = off;
	rightPage->lFence = btree2InstallSlot(rightPage, lSlot, 0);

	//	copy over remaining slots from old page into new right page

	memset (fwd, 0, sizeof(fwd));

	if( next )
	  do {
		uint8_t height = btree2GenHeight (index);
		rSlot = slotptr(set->page, next);

		if( atomicCAS8(rSlot->state, Btree2_slotactive, Btree2_slotmoved) ) {
		  if( (off = btree2InstallSlot(rightPage, rSlot, height)) )
			btree2FillTower (rightPage, off, fwd, height);
		  else	
			return DB_ERROR_indexnode;
		}

		next = rSlot->tower[0];

	  }	while( next );

	//	reuse existing parent key/pageNo for right page
	//	allocate new pageNo and insert new key for left page

	rightPage->pageNo.bits = set->page->pageNo.bits;
    rPageNoPtr = fetchIdSlot(index->map, rightPage->pageNo);

	if ((leftPage->pageNo.bits = btree2AllocPageNo(index)))
		lPageNoPtr = fetchIdSlot(index->map, leftPage->pageNo);
	else
		return DB_ERROR_outofmemory;

	//	splice pages together

	rightPage->left.bits = leftPage->pageNo.bits;
	leftPage->right.bits = rightPage->pageNo.bits;

    if( (leftPage->left.bits = set->page->left.bits) ) {
		slot = slotptr (set->page, set->page->lFence);
		leftPage->lFence = btree2InstallSlot (leftPage, slot, 0);
	}

	if( (rightPage->right.bits = set->page->right.bits) ) {
		slot = slotptr (set->page, set->page->rFence);
		rightPage->rFence = btree2InstallSlot (rightPage, slot, 0);
	}

	//	install left and right page addr into pageNo slots

    lPageNoPtr->bits = left.bits;
    rPageNoPtr->bits = right.bits;

	if(set->page->left.bits) {
		tmpPageNoPtrL = fetchIdSlot (index->map, set->page->left);
		tmpPage = getObj (index->map, *tmpPageNoPtrL);
		tmpPage->right.bits = leftPage->pageNo.bits;
	}

	if(set->page->right.bits) {
		tmpPageNoPtrR = fetchIdSlot (index->map, set->page->right);
		tmpPage = getObj (index->map, *tmpPageNoPtrR);
		tmpPage->left.bits = rightPage->pageNo.bits;
	} else
		rightPage->stopper.bits = set->page->stopper.bits;

	//  if root page was split, install a new root page

	if( lvl + 1 > set->rootLvl ) {
	  if( (root.bits = btree2NewPage (index, lvl + 1)) )
		rootPage = getObj (index->map, root);
	  else
		return DB_ERROR_outofmemory;

	  if ((rootPage->pageNo.bits = btree2AllocPageNo(index)))
		rootObjId = fetchIdSlot(index->map, rootPage->pageNo);
	  else
		return DB_ERROR_outofmemory;

	  rootPage->stopper.bits = rightPage->pageNo.bits;
	  rootPage->attributes = Btree2_rootPage;

	  btree2->root.bits = rootPage->pageNo.bits;
	  rootObjId->bits = root.bits;
	}

	//	insert left fence key into parent 

    key = slotkey(lSlot);
	keyLen = keylen(key);

	if( lvl )
		keyLen -= size64(keystr(key), keyLen);		// strip off pageNo
	
	memcpy(keyBuff, keystr(key), keyLen);
	sfxLen = store64(keyBuff, keyLen, leftPage->pageNo.bits, btree2->base->binaryFlds);
	
	if( (stat = btree2InsertKey (index, keyBuff, keyLen, sfxLen, lvl + 1, Btree2_slotactive)) )
		return stat;

	// rightmost leaf page number is always 1

	if( lvl == 0 ) { 
		if( leftPage->left.bits == 0 )
			btree2->left.bits = leftPage->pageNo.bits;
		if( rightPage->right.bits == 0 )
			btree2->right.bits = rightPage->pageNo.bits;
	}

	//	recycle original page to free list

	if( !btree2RecyclePage(index, set->page->pageType, set->pageAddr) )
		return DB_ERROR_outofmemory;
				
	return DB_OK;
}

//	move slot from one page to a replacement page
//	This function IS thread safe

uint16_t btree2InstallSlot (Btree2Page *page, Btree2Slot *source, uint8_t height) {
uint8_t *key;
uint32_t keyLen;
uint32_t slotSize;
Btree2Slot *slot;
Btree2Slot *prev;
uint16_t off;
uint8_t *dest;
int idx;

    key = slotkey (source);
	keyLen = keylen(key);

	slotSize = btree2SizeSlot(keyLen, height);

	if( (off = btree2AllocSlot(page, slotSize)) )
		slot = slotptr(page, off);
	else
		return 0;

	//	fill in new slot attributes & key value

	*slot->state = Btree2_slotactive;
	slot->height = height;

	// copy slot onto page

	dest = slotkey(slot);

	if( keyLen < 128 )	
		*dest++ = keyLen;
	else
		*dest++ = (keyLen >> 8) | 0x80, *dest++ = keyLen;

	memcpy (dest, keystr(key), keyLen);
	return off;
}

//	fill tower for transferred in-order slot
//	this function IS NOT thread safe.

void btree2FillTower(Btree2Page *page, uint16_t off, uint16_t *fwd, uint8_t height) {
Btree2Slot *slot = slotptr(page, off);
int idx;

    slot->height = height;

	if( height > page->height )
	  page->height = height;

	for( idx = 0; idx < height; idx++ ) {
	  if( fwd[idx] ) {
		Btree2Slot *prev = slotptr(page, fwd[idx]);
		prev->tower[idx] = off;
	  } else
		page->towerHead[idx] = off;

	  fwd[idx] = off;
	}
}

//	install new unordered key onto page at assigned offset,
//	this function IS thread safe, and uses latches for synchronization

DbStatus btree2InstallKey (Btree2Set *set, uint8_t *key, uint32_t keyLen, uint8_t height) {
Btree2Slot *slot, *prev;
uint16_t prevFwd;
uint8_t *dest;
int idx = 0;

	// install new slot into page

	slot = slotptr(set->page, set->off);
	*slot->state = Btree2_slotactive;
	slot->height = height;

	dest = slotkey(slot);

	if( keyLen < 128 )	
		*dest++ = keyLen;
	else
		*dest++ = (keyLen >> 8 | 0x80), *dest++ = keyLen;

	memcpy (dest, key, keyLen);

	//  splice new slot into tower structure

	while( idx < height ) {
		if( idx >= set->page->height)  {
			lockLatchGrp (set->page->bitLatch, idx);

			if( idx >= set->page->height )
			  if( !set->page->towerHead[idx] )
				set->page->towerHead[idx] = set->off;

			++set->page->height;
			unlockLatchGrp (set->page->bitLatch, idx);
			idx += 1;
			continue;
		}

		//	prevOff[idx] > 0 => prev is last slot smaller than new key
		//	scan right from prevOff until next slot has a larger key

		prevFwd = set->prevOff[idx];
		
		if( prevFwd >= TowerSlotOff ) {
			prev = slotptr(set->page, prevFwd); 

 			if( btree2LinkTower(set, prev->tower, prev->bitLatch, idx) )
				idx += 1;

			continue;
		}

		//	page->towerHead == 0, install new key slot there

		if( btree2LinkTower(set, set->page->towerHead, set->page->bitLatch, idx) )
			idx += 1;

		continue;
	}

	return DB_OK;
}

//	link slot into one lvl of tower list

bool btree2LinkTower(Btree2Set *set, uint16_t *tower, uint8_t *bitLatch, uint8_t idx) {
Btree2Slot *nxtSlot, *slot = slotptr(set->page, set->off);
uint8_t *key = slotkey(slot);
uint32_t keyLen = keylen(key);

  do {
	lockLatchGrp(bitLatch, idx);

	if( tower[idx] == 0 )
		break;

	//  if another node was inserted on the left,
	//	continue to the right until it is greater

	//  compare two keys, return > 0, = 0, or < 0
	//  =0: all key fields are same
	//  -1: key2 > key1
	//  +1: key2 < key1

	nxtSlot = slotptr(set->page, tower[idx]);

	if (btree2KeyCmp (slotkey (nxtSlot), keystr (key), keyLen) >= 0)
		break;

	unlockLatchGrp (bitLatch, idx);

	tower = nxtSlot->tower;
	bitLatch = nxtSlot->bitLatch;

  } while( true );

//  link forward pointers

  slot->tower[idx] = tower[idx];
  tower[idx] = set->off;

  unlockLatchGrp (bitLatch, idx);
  return true;
}
