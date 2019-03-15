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
uint16_t next;

	if (keyLen > MAX_key)
		return DB_ERROR_keylength;

 	memcpy(keyBuff, key, keyLen);

    keyLen += store64 (keyBuff, keyLen, suffixValue, btree2->base->binaryFlds);

	slotSize = btree2SizeSlot (keyLen, height);
	memset(set, 0, sizeof(set));

	do {
	  next = btree2LoadPage(index->map, set, keyBuff, keyLen, lvl);

	  if( (set->off = btree2AllocSlot (set->page, slotSize) ))
		 return btree2InstallKey (index, set, keyBuff, keyLen, height);

	   if( (stat = btree2CleanPage (index, set)))
		 return stat;
	} while( true);

	return DB_OK;
}

//	check page for space available,
//	clean or split if necessary

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

// create and install new root page with right page fence key
//	and raise the height of the btree2

DbStatus btree2NewRoot(Handle *index, Btree2Page *rightPage, Btree2Page *leftPage, int lvl) {
Btree2Index *btree2 = btree2index(index->map);
uint32_t keyLen, newLen, totLen;
Btree2Slot *slot, *source;
Btree2Page *rootPage;
ObjId *rootObjId;
uint8_t *key, *dest;
uint16_t off;
DbAddr root;

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

	source = slotptr(leftPage, leftPage->fence);

	key = slotkey(source);
	keyLen = keylen(key);

	//	strip pageno from the key in non-leaf pages

	if( lvl > 0 )
		keyLen -= size64(keystr(key), keyLen);

	newLen = calc64(leftPage->pageNo.bits, btree2->base->binaryFlds);
	totLen = btree2SizeSlot (newLen + keyLen, 1);

	if( (off = btree2AllocSlot(rootPage, totLen)) )
		slot = slotptr(rootPage, off);
	else
		return DB_ERROR_pageisgarbage;

	slot->height = 1;
	*slot->state = Btree2_slotactive;
	dest = slotkey(slot);

	*rootPage->height = 1;
	rootPage->towerHead[0] = off;

	keyLen += newLen;

	if( keyLen > 128 * 256 )
		return DB_ERROR_keylength;

	if( keyLen > 127 )
		*dest++ = keyLen >> 8 | 0x80, *dest++ = keyLen;
	else
		*dest++ = keyLen;

	memcpy(dest, keystr(key), keyLen - newLen);
	store64(dest, keyLen - newLen, leftPage->pageNo.bits, btree2->base->binaryFlds);

	//   install new root page, and return to insert left fence key into root
	
	btree2->root.bits = rootPage->pageNo.bits;
	rootObjId->bits = root.bits;
	return DB_OK;
}

//	split set->page and splice two new pages left and right
//	reuse existiing pageNo for new right page until proper left fence key
//	is installed in parent with the new left pageNo, then switch original pageNo
//	to point at new right page.

//	Note:  this function has been optimized and is not thread safe. (the fwd array in specific)


DbStatus btree2SplitPage (Handle *index, Btree2Set *set) {
Btree2Index *btree2 = btree2index(index->map);
uint8_t *lKey, lvl = set->page->lvl;
Btree2Page *leftPage, *rightPage;
uint16_t fwd[Btree2_maxskip];
uint16_t lKeyLen, max, next;
Btree2Slot *rSlot, *lSlot;
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

	memset (fwd, 0, sizeof(fwd));
	max = leftPage->size >> leftPage->skipBits;

	if( (next = set->page->towerHead[0]) )
	  do {
		lSlot = slotptr(set->page, next);

		if (atomicCAS8(lSlot->state, Btree2_slotactive, Btree2_slotmoved)) {
			if (!(leftPage->fence = btree2InstallSlot(index, leftPage, lSlot, fwd)))
				return DB_ERROR_indexnode;
		}

		next = lSlot->tower[0];

	  } while( next && leftPage->alloc->nxt > max / 2 );

	//	copy over remaining slots from old page into new right page

	memset (fwd, 0, sizeof(fwd));
	max = rightPage->size >> rightPage->skipBits;

	if( next)
	  do {
		rSlot = slotptr(set->page, next);

		if( atomicCAS8(rSlot->state, Btree2_slotactive, Btree2_slotmoved) ) {
			if (!(rightPage->fence = btree2InstallSlot(index, rightPage, rSlot, fwd)))
				return DB_ERROR_indexnode;
		}

		next = rSlot->tower[0];

	  }	while( next ); //&& rightPage->alloc->nxt > max / 2 );

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
    rightPage->right.bits = set->page->right.bits;

	leftPage->right.bits = rightPage->pageNo.bits;
    leftPage->left.bits = set->page->left.bits;

	//	install left page addr into original pageNo slot and
	//	into a new left key/pageNo 

    lPageNoPtr->bits = left.bits;
    rPageNoPtr->bits = left.bits;

	//	insert left fence key into parent 

    lKey = slotkey(lSlot);
	lKeyLen = keylen(lKey);

	if( lvl )
		lKeyLen -= size64(keystr(lKey), lKeyLen);		// strip off pageNo
	
	//  if root page was split, install a new root page

	if( lvl + 1 > set->rootLvl )
		stat = btree2NewRoot (index, rightPage, leftPage, lvl);
	else 
		stat = btree2InsertKey (index, keystr (lKey), lKeyLen, leftPage->pageNo.bits, lvl, Btree2_slotactive);

	if( stat )
		return stat;

	//	install right page addr into its pageNo ObjId slot

	rPageNoPtr->bits = right.bits;

	// rightmost leaf page is always 1

	if( lvl == 0 )
		if( leftPage->left.bits == 0 )
			btree2->left.bits = leftPage->pageNo.bits;

	//	recycle original page to free list

	if( !btree2RecyclePage(index, set->page->pageType, set->pageAddr) )
		return DB_ERROR_outofmemory;
				
	return DB_OK;
}

//	move slot from one page to a replacement page
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

    key = slotkey (source);
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

	if( keyLen < 128 )	
		*dest++ = keyLen;
	else
		*dest++ = (keyLen >> 8) | 0x80, *dest++ = keyLen;

	memcpy (dest, keystr(key), keyLen);

	if( height > *page->height )
		  *page->height = height;

	for( idx = 0; idx < height; idx++ ) {
	  if( fwd[idx] ) {
		prev = slotptr(page, fwd[idx]);
		prev->tower[idx] = off;
	  } else
		  page->towerHead[idx] = off;

	  fwd[idx] = off;
	}

	return off;
}

//	install new key onto page at assigned offset,  fill-in skip list tower slots
//	this function IS thread safe, and uses latches for synchronization

DbStatus btree2InstallKey (Handle *index, Btree2Set *set, uint8_t *key, uint32_t keyLen, uint8_t height) {
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
		if( idx >= *set->page->height)  {
			lockLatchGrp (set->page->bitLatch, idx);

			if( idx >= *set->page->height )
			  if( !set->page->towerHead[idx] )
				set->page->towerHead[idx] = set->off;

			++set->page->height[0];
			unlockLatchGrp (set->page->bitLatch, idx);
			idx += 1;
			continue;
		}

		//	prevSlot[idx] > 0 => prevFwd is last key smaller than new key
		//	scan right from prevFwd until next slot has a larger key

		if( (prevFwd = set->prevOff[idx]) ) {
			prev = slotptr(set->page, prevFwd); 

 			if( btree2LinkTower(set->page, prev->tower, prev->bitLatch, idx, set->off) )
				idx += 1;

			continue;
		}

		//	page->towerHead[idx] > 0 => prevFwd is smallest key in the index
		//	lock page->bitLatch 

		if( btree2LinkTower(set->page, set->page->towerHead, set->page->bitLatch, idx, set->off ) )
			idx += 1;

		continue;
	}

	return DB_OK;
}

//	link slot into one lvl of tower list

bool btree2LinkTower(Btree2Page *page, uint16_t *tower, uint8_t *bitLatch, uint8_t idx, uint16_t off ) {
Btree2Slot *next, *slot = slotptr(page, off);
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

	next = slotptr(page, tower[idx]);

	if (btree2KeyCmp (slotkey (next), keystr (key), keyLen) >= 0)
		break;

	unlockLatchGrp (bitLatch, idx);

	tower = next->tower;
	bitLatch = next->bitLatch;

  } while( true );

//  link forward pointers

  slot->tower[idx] = tower[idx];
  tower[idx] = off;

  unlockLatchGrp (bitLatch, idx);
  return true;
}
