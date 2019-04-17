#include "btree1.h"

//	debug slot function

#ifdef DEBUG
Btree1Slot *btree1Slot(Btree1Page *page, uint32_t idx)
{
	return slotptr(page, idx);
}

uint8_t *btree1Key(Btree1Page *page, uint32_t idx)
{
	return keyptr(page, idx);
}

uint8_t *btree1Addr(Btree1Page *page, uint32_t off)
{
	return keyaddr(page, off);
}

#undef keyptr
#undef keyaddr
#undef slotptr
#define keyptr(p,x) btree1Key(p,x)
#define keyaddr(p,o) btree1Addr(p,o)
#define slotptr(p,x) btree1Slot(p,x)
#endif

uint32_t Splits;

// split the root and raise the height of the btree1
// call with key for smaller half and right page addr.

DbStatus btree1SplitRoot(Handle *index, Btree1Set *root, DbAddr right, uint8_t *leftKey) {
Btree1Index *btree1 = btree1index(index->map);
uint32_t keyLen, nxt = btree1->pageSize;
Btree1Page *leftPage, *rightPage;
Btree1Slot *slot;
uint8_t *dest;
uint32_t off, amt;
DbAddr left;

	//  Obtain an empty page to use, and copy the current
	//  root contents into it, e.g. lower keys

	if( (left.bits = btree1NewPage(index, root->page->lvl)) )
		leftPage = getObj(index->map, left);
	else
		return DB_ERROR_outofmemory;

	//	copy in new smaller keys into left page
	//	(clear the latches)

	memcpy (leftPage->latch + 1, root->page->latch + 1, btree1->pageSize - sizeof(*leftPage->latch));
	rightPage = getObj(index->map, right);
	rightPage->left.bits = left.bits;

	// preserve the page info at the bottom
	// of higher keys and set rest to zero

	memset(root->page+1, 0, btree1->pageSize - sizeof(*root->page));

	// insert stopper key on root page
	// pointing to right half page 
	// and increase the root height

	amt = calc64(right.bits, false);
	slot = slotptr(root->page, 2);
	slot->type = Btree1_stopper;
	slot->off = nxt -= amt + 1;

	dest = keyaddr(root->page, nxt);
	*dest++ = amt;

	store64(dest, 0, right.bits, false);

	// next insert lower keys (left) fence key on newroot page as
	// first key and reserve space for the key.

	keyLen = keylen(leftKey);

	if( leftPage->lvl )
		keyLen -= size64(keystr(leftKey), keyLen);

	amt = keyLen + calc64(left.bits, false);
	
	if( amt > 127 )
		off = 2;
	else
		off = 1;

	slot = slotptr(root->page, 1);
	slot->type = Btree1_indexed;
	slot->off = nxt -= amt + off;

	//	construct lower (left) page key

	dest = keyaddr(root->page, nxt);

	if (off == 1)
		*dest++ = amt;
	else
		*dest++ = amt / 256 | 0x80, *dest++ = amt;

	memcpy (dest, keystr(leftKey), keyLen);
	store64(dest, keyLen, left.bits, false);
	
	root->page->right.bits = 0;
	root->page->min = nxt;
	root->page->cnt = 2;
	root->page->act = 2;
	root->page->lvl++;

	// release root page

	btree1UnlockPage(root->page, Btree1_lockWrite);
	return DB_OK;
}

//  split already locked full node
//	return with pages unlocked.

DbStatus btree1SplitPage (Handle *index, Btree1Set *set) {
Btree1Index *btree1 = btree1index(index->map);
uint32_t cnt = 0, idx = 0, max, nxt;
Btree1Slot librarian, *source, *dest;
uint32_t size = btree1->pageSize;
Btree1Page *frame, *rightPage;
uint8_t lvl = set->page->lvl;
uint8_t *leftKey, *rightKey;
uint32_t totLen, keyLen;
uint8_t *key = NULL;
DbAddr right, addr;
bool stopper;
DbStatus stat;

#ifdef DEBUG
	atomicAdd32(&Splits, 1);
#endif

	librarian.bits = 0;
	librarian.type = Btree1_librarian;
	librarian.dead = 1;

	if( !set->page->lvl )
		size <<= btree1->leafXtra;

	//	get new page and write higher keys to it.

	if( (right.bits = btree1NewPage(index, lvl)) )
		rightPage = getObj(index->map, right);
	else
		return DB_ERROR_outofmemory;

	max = set->page->cnt;
	cnt = max / 2;
	nxt = size;
	idx = 0;

	source = slotptr(set->page, cnt);
	dest = slotptr(rightPage, 0);

	while( source++, cnt++ < max ) {
		if( source->dead )
			continue;

		key = keyaddr(set->page, source->off);
		totLen = keylen(key) + keypre(key);
		nxt -= totLen;

		rightKey = keyaddr(rightPage, nxt);
		memcpy (rightKey, key, totLen);
		rightPage->act++;

		//	add librarian slot

		if (cnt < max) {
			(++dest)->bits = librarian.bits;
			dest->off = nxt;
			idx++;
		}

		//  add actual slot

		(++dest)->bits = source->bits;
		dest->off = nxt;
		idx++;
	}

	stopper = dest->type == Btree1_stopper;

	rightPage->min = nxt;
	rightPage->cnt = idx;
	rightPage->lvl = lvl;

	// link right node

	if( set->pageNo.type != Btree1_rootPage ) {
		rightPage->right.bits = set->page->right.bits;
		rightPage->left.bits = set->pageNo.bits;

		if( !lvl && rightPage->right.bits ) {
			Btree1Page *farRight = getObj(index->map, rightPage->right);
			btree1LockPage (farRight, Btree1_lockLink);
			farRight->left.bits = right.bits;
			btree1UnlockPage (farRight, Btree1_lockLink);
		}
		if( !lvl && !rightPage->right.bits )
			btree1->right.bits = right.bits;
	}

	//	copy lower keys from temporary frame back into old page

	if( (addr.bits = btree1NewPage(index, lvl)) )
		frame = getObj(index->map, addr);
	else
		return DB_ERROR_outofmemory;

	memcpy (frame, set->page, size);
	memset (set->page+1, 0, size - sizeof(*set->page));

	set->page->garbage = 0;
	set->page->act = 0;
	nxt = size;
	max /= 2;
	cnt = 0;
	idx = 0;

	//  ignore librarian max key

	if( slotptr(frame, max)->type == Btree1_librarian )
		max--;

	source = slotptr(frame, 0);
	dest = slotptr(set->page, 0);

#ifdef DEBUG
	key = keyaddr(frame, source[2].off);
	assert(keylen(key) > 0);
#endif
	//  assemble page of smaller keys from temporary frame copy

	while( source++, cnt++ < max ) {
		if( source->dead )
			continue;

		leftKey = keyaddr(frame, source->off);
		totLen = keylen(leftKey) + keypre(leftKey);
		nxt -= totLen;

		memcpy (keyaddr(set->page, nxt), leftKey, totLen);

		//	add librarian slot, except before fence key

		if (cnt < max) {
			(++dest)->bits = librarian.bits;
			dest->off = nxt;
			idx++;
		}

		//	add actual slot

		(++dest)->bits = source->bits;
		dest->off = nxt;
		idx++;

		set->page->act++;
	}

	set->page->right.bits = right.bits;
	set->page->min = nxt;
	set->page->cnt = idx;

	// if current page is the root page, split it

	if (set->pageNo.type == Btree1_rootPage) {
          if (!(stat = btree1SplitRoot(index, set, right, leftKey)))
            if (addSlotToFrame(index->map, listFree(index, addr.type), NULL,
                               addr.bits))
              return DB_OK;
            else
              return DB_ERROR_outofmemory;
          else
            return stat;
        return stat;
	}

	// insert new fences in their parent pages

	btree1LockPage (rightPage, Btree1_lockParent);
	btree1LockPage (set->page, Btree1_lockParent);
	btree1UnlockPage (set->page, Btree1_lockWrite);

	keyLen = keylen(leftKey);

	if (set->page->lvl) 
		keyLen -= size64(keystr(leftKey), keyLen);  // strip off pageNo

    if ((stat = btree1InsertSfxKey(index, keystr(leftKey), keyLen, set->pageNo.bits, lvl + 1, Btree1_indexed)))
		return stat;

	// switch fence for right block of larger keys to new right page

	if( (stat = btree1FixKey(index, rightKey, set->pageNo.bits, right.bits, lvl+1, stopper) ))
		return stat;

	btree1UnlockPage (set->page, Btree1_lockParent);
	btree1UnlockPage (rightPage, Btree1_lockParent);

	if (addSlotToFrame(index->map, listFree(index, addr.type), NULL, addr.bits))
          return DB_OK;

    return DB_ERROR_outofmemory;
}

//	check page for space available,
//	clean if necessary and return
//	false - page needs splitting
//	true  - ok to insert

DbStatus btree1CleanPage(Handle *index, Btree1Set *set, uint32_t totKeyLen) {
Btree1Index *btree1 = btree1index(index->map);
Btree1Slot librarian, *source, *dest;
uint32_t size = btree1->pageSize;
Btree1Page *page = set->page;
uint32_t max = page->cnt;
uint32_t len, cnt, idx;
uint32_t newSlot = max;
Btree1PageType type;
Btree1Page *frame;
uint8_t *key;
DbAddr addr;

	librarian.bits = 0;
	librarian.type = Btree1_librarian;
	librarian.dead = 1;

	if( !page->lvl ) {
		size <<= btree1->leafXtra;
		type = Btree1_leafPage;
	} else {
		type = Btree1_interior;
	}

	if( page->min >= (max+1) * sizeof(Btree1Slot) + sizeof(*page) + totKeyLen )
		return DB_OK;

	//	skip cleanup and proceed directly to split
	//	if there's not enough garbage
	//	to bother with.

	if( page->garbage < size / 5 )
		return DB_BTREE_needssplit;

	if( (addr.bits = allocObj(index->map, listFree(index, type), NULL, type, size, false)) )
		frame = getObj(index->map, addr);
	else
		return DB_ERROR_outofmemory;

	memcpy (frame, page, size);

	// skip page info and set rest of page to zero

	memset (page+1, 0, size - sizeof(*page));
	page->garbage = 0;
	page->act = 0;

	cnt = 0;
	idx = 0;

	source = slotptr(frame, cnt);
	dest = slotptr(page, idx);

	// clean up page first by
	// removing deleted keys

	while( source++, cnt++ < max ) {
		if( cnt == set->slotIdx )
			newSlot = idx + 2;

		if( source->dead )
			continue;

		// copy the active key across

		key = keyaddr(frame, source->off);
		len = keylen(key) + keypre(key);
		size -= len;

		memcpy ((uint8_t *)page + size, key, len);

		// make a librarian slot

		if (cnt < max) {
			(++dest)->bits = librarian.bits;
			++idx;
		}

		// set up the slot

		(++dest)->bits = source->bits;
		dest->off = size;
		idx++;

		page->act++;
	}

	page->min = size;
	page->cnt = idx;

	//	update insert slot index
	//	for newly cleaned-up page

	set->slotIdx = newSlot;

	//  return temporary frame

	addSlotToFrame(index->map, listFree(index,addr.type), NULL, addr.bits);

	//	see if page has enough space now, or does it still need splitting?

	if( page->min >= (idx+1) * sizeof(Btree1Slot) + sizeof(*page) + totKeyLen )
		return DB_OK;

	return DB_BTREE_needssplit;
}

//  compare two keys, return > 0, = 0, or < 0
//  =0: all key fields are same
//  -1: key2 > key1
//  +1: key2 < key1

int btree1KeyCmp (uint8_t *key1, uint8_t *key2, uint32_t len2)
{
uint32_t len1 = keylen(key1);
int ans;

	key1 += keypre(key1);

	if((ans = memcmp (key1, key2, len1 > len2 ? len2 : len1)))
		return ans;

	if( len1 > len2 )
		return 1;
	if( len1 < len2 )
		return -1;

	return 0;
}

//  find slot in page that is .ge. given search key

//	return zero if past end of all slots
//	return slot idx for key that is .ge. passed key.

uint32_t btree1FindSlot (Btree1Page *page, uint8_t *key, uint32_t keyLen)
{
uint32_t diff, higher = page->cnt + 1, low = 1, slot;
bool stopper = false;

	if( page->lvl && !page->right.bits )
	  higher--, stopper = true;

	//	low is a candidate.
	//  higher is already
	//	tested as .ge. the given key.
	//  loop ends when they meet

	while( (diff = higher - low) ) {
	  slot = low + diff / 2;

//	  if( !stopper || slot < page->cnt )
		if( btree1KeyCmp (keyptr (page, slot), key, keyLen) < 0 )
		  low = slot + 1;
		else
		  higher = slot, stopper = false;
//	  else
//		return page->cnt;
	}

	return higher;
}

//  find and load page at given level for given key
//	leave page rd or wr locked as requested

//	Librarian slots have the same key offset as their higher neighbor

DbStatus btree1LoadPage(DbMap *map, Btree1Set *set, void *key, uint32_t keyLen, uint8_t lvl, Btree1Lock lock) {
Btree1Index *btree1 = btree1index(map);
uint8_t drill = 0xff, *ptr;
Btree1Page *prevPage = NULL;
Btree1Lock mode, prevMode;
DbAddr prevPageNo;
uint64_t bits;

  bits = btree1->root.bits;
  prevPageNo.bits = 0;

  //  start at our idea of the root level of the btree1 and drill down

  while( (set->pageNo.bits = bits) ) {

	// determine lock mode of drill level

	mode = (drill == lvl) ? lock : Btree1_lockRead; 
	set->page = getObj(map, set->pageNo);

	//	release parent or left sibling page

	if( prevPageNo.bits ) {
	  btree1UnlockPage(prevPage, prevMode);
	  prevPageNo.bits = 0;
	}

 	// obtain mode lock

	btree1LockPage(set->page, mode);

	if( set->page->free )
		return DB_BTREE_error;

	// re-read and re-lock root after determining actual level of root

	if( set->page->lvl != drill) {
		assert(drill == 0xff);
		drill = set->page->lvl;

		if( lock != Btree1_lockRead && drill == lvl ) {
		  btree1UnlockPage(set->page, mode);
		  continue;
		}
	}

	assert(lvl <= set->page->lvl);

	prevPageNo.bits = set->pageNo.bits;
	prevPage = set->page;
	prevMode = mode;

	//  find key on page at this level
	//  and descend to requested level

	if( set->page->kill ) {
		bits = set->page->right.bits;
		continue;
	}

	// find slot on page

	if( (set->slotIdx = btree1FindSlot (set->page, key, keyLen) )) {

	  if(drill == lvl)
		return DB_OK;

	  // find next higher non-dead slot

	  while( set->slotIdx < set->page->cnt )
		if(slotptr (set->page, set->slotIdx)->dead)
		  set->slotIdx++;
	    else
		  return DB_BTREE_error;

	  // get next page down
	
	  ptr = keyptr(set->page, set->slotIdx);
	  bits = get64(keystr(ptr), keylen(ptr), false);

	  assert(drill > 0);
	  drill--;
	  continue;
	}

	//	if page is empty

	if( set->page->cnt == 0 )
		return DB_OK;

	//  or slide right into next page

	bits = set->page->right.bits;
  }

  // return error on end of right chain

  return DB_BTREE_error;
}
