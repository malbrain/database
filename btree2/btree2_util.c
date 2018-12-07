#include "btree2.h"

//	debug slot function

#ifdef DEBUG
Btree2Slot *btree2Slot(Btree1Page *page, uint32_t idx)
{
	return slotptr(page, idx);
}

uint8_t *btree2Key(Btree1Page *page, uint32_t idx)
{
	return keyptr(page, idx);
}

uint8_t *btree2Addr(Btree1Page *page, uint32_t off)
{
	return keyaddr(page, off);
}

#undef keyptr
#undef keyaddr
#undef slotptr
#define keyptr(p,x) btree2Key(p,x)
#define keyaddr(p,o) btree2Addr(p,o)
#define slotptr(p,x) btree2Slot(p,x)
#endif

uint32_t Splits;

// split the root and raise the height of the btree2
// call with key for smaller half and right page addr.

DbStatus btree2SplitRoot(Handle *index, Btree2Set *root, DbAddr right, uint8_t *leftKey) {
Btree2Index *btree2 = btree2index(index->map);
uint32_t keyLen, nxt = btree2->pageSize;
Btree2Page *leftPage, *rightPage;
ObjId leftPageNo;
Btree2Slot *slot;
uint8_t *ptr;
uint32_t off;
DbAddr left;

	//  Obtain an empty page to use, and copy one halfthe current
	//  root contents into it, e.g. lower keys

	if( (left.bits = btree2NewPage(index, root->page->lvl)) )
		leftPage = getObj(index->map, left);
	else
		return DB_ERROR_outofmemory;

	//	copy over new smaller keys into left page

	memset(root->page+1, 0, btree2->pageSize - sizeof(*root->page));

	// key on root page
	// pointing to right half page 
	// and increase the root height

	nxt -= 1 + sizeof(uint64_t);
	slot = slotptr(root->page, 2);
	slot->off = nxt;

	ptr = keyaddr(root->page, nxt);
	btree2PutPageNo(ptr + 1, 0, right.bits);
	ptr[0] = sizeof(uint64_t);

	// next insert lower keys (left) fence key on newroot page as
	// first key and reserve space for the key.

	keyLen = keylen(leftKey);
	off = keypre(leftKey);

	nxt -= keyLen + off;
	slot = slotptr(root->page, 1);
	slot->type = Btree2_indexed;
	slot->off = nxt;

	//	construct lower (left) page key

	ptr = keyaddr(root->page, nxt);
	memcpy (ptr + off, leftKey + keypre(leftKey), keyLen - sizeof(uint64_t));
	btree2PutPageNo(ptr + off, keyLen - sizeof(uint64_t), left.bits);

	if (off == 1)
		ptr[0] = keyLen;
	else
		ptr[0] = keyLen / 256 | 0x80, ptr[1] = keyLen;
	
	root->page->right.bits = 0;
	root->page->min = nxt;
	root->page->cnt = 2;
	root->page->act = 2;
	root->page->lvl++;

	return DB_OK;
}

//  split already locked full node
//	return with pages unlocked.

DbStatus btree2SplitPage (Handle *index, Btree2Set *set) {
uint8_t leftKey[Btree2_maxkey], rightKey[Btree2_maxkey];
Btree2Index *btree2 = btree2index(index->map);
uint32_t cnt = 0, idx = 0, max, nxt, off;
Btree2Slot *source, *dest;
uint32_t size = btree2->pageSize;
Btree1Page *frame, *rightPage;
uint8_t lvl = set->page->lvl;
uint32_t totLen, keyLen;
uint8_t *key = NULL;
DbAddr right, addr;
DbStatus stat;

#ifdef DEBUG
	atomicAdd32(&Splits, 1);
#endif

	if( !set->page->lvl )
		size <<= btree2->leafXtra;

	//	get new page and write higher keys to it.

	if( (right.bits = btree2NewPage(index, lvl)) )
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

		memcpy (keyaddr(rightPage, nxt), key, totLen);
		rightPage->act++;

		}

		//  add actual slot

		(++dest)->bits = source->bits;
		dest->off = nxt;
		idx++;
	}

	//	remember right fence key for larger page
	//	extend right leaf fence key with
	//	the right page number on leaf page.

	keyLen = keylen(key);

	if( set->page->lvl)
		keyLen -= sizeof(uint64_t);		// strip off pageNo

	if( keyLen + sizeof(uint64_t) < 128 )
		off = 1;
	else
		off = 2;

	//	copy key and add pageNo

	memcpy (rightKey + off, key + keypre(key), keyLen);
	btree2PutPageNo(rightKey + off, keyLen, right.bits);
	keyLen += sizeof(uint64_t);

	if (off == 1)
		rightKey[0] = keyLen;
	else
		rightKey[0] = keyLen / 256 | 0x80, rightKey[1] = keyLen;

	rightPage->min = nxt;
	rightPage->cnt = idx;
	rightPage->lvl = lvl;

	// link right node

	if( set->pageNo.type != Btree1_rootPage ) {
		rightPage->right.bits = set->page->right.bits;
		rightPage->left.bits = set->pageNo.bits;

		if( !lvl && rightPage->right.bits ) {
			Btree2Page *farRight = getObj(index->map, rightPage->right);
			farRight->left.bits = right.bits;
			btree2UnlockPage (farRight, Btree1_lockLink);
		}
	}

	//	copy lower keys from temporary frame back into old page

	if( (addr.bits = btree2NewPage(index, lvl)) )
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

		key = keyaddr(frame, source->off);
		totLen = keylen(key) + keypre(key);
		nxt -= totLen;

		memcpy (keyaddr(set->page, nxt), key, totLen);

		//	add actual slot

		(++dest)->bits = source->bits;
		dest->off = nxt;
		idx++;

		set->page->act++;
	}

	set->page->right.bits = right.bits;
	set->page->min = nxt;
	set->page->cnt = idx;

	//	remember left fence key for smaller page
	//	extend left leaf fence key with
	//	the left page number.

	keyLen = keylen(key);

	if( set->page->lvl)
		keyLen -= sizeof(uint64_t);		// strip off pageNo

	if( keyLen + sizeof(uint64_t) < 128 )
		off = 1;
	else
		off = 2;

	//	copy key and add pageNo

	memcpy (leftKey + off, key + keypre(key), keyLen);
	btree2PutPageNo(leftKey + off, keyLen, set->pageNo.bits);
	keyLen += sizeof(uint64_t);

	if (off == 1)
		leftKey[0] = keyLen;
	else
		leftKey[0] = keyLen / 256 | 0x80, leftKey[1] = keyLen;

	//  return temporary frame

	addSlotToFrame(index->map, listFree(index, addr.type), NULL, addr.bits);

	// if current page is the root page, split it

	if( set->pageNo.type == Btree1_rootPage )
		return btree2SplitRoot (index, set, right, leftKey);

	// insert new fence for reformulated left block of smaller keys

	if( (stat = btree2InsertKey(index, leftKey + keypre(leftKey), keylen(leftKey), lvl+1, Btree1_indexed) ))
		return stat;

	// switch fence for right block of larger keys to new right page

	if( (stat = btree2FixKey(index, rightKey, lvl+1) ))
		return stat;

	return DB_OK;
}

//	check page for space available,
//	clean if necessary and return
//	false - page needs splitting
//	true  - ok to insert

DbStatus btree2CleanPage(Handle *index, Btree2Set *set, uint32_t totKeyLen) {
Btree2Index *btree2 = btree2index(index->map);
uint32_t size = btree2->pageSize;
Btree2Page *page = set->page;
Btree2Slot *source, *dest;
uint32_t max = page->cnt;
uint32_t len, cnt, idx;
uint32_t newSlot = max;
Btree1PageType type;
Btree1Page *frame;
uint8_t *key;
DbAddr addr;

	if( !page->lvl ) {
		size <<= btree2->leafXtra;
		type = Btree2_leafPage;
	} else {
		type = Btree2_interior;
	}

	if( page->min >= (max+1) * sizeof(Btree2Slot) + sizeof(*page) + totKeyLen )
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

	if( page->min >= (idx+1) * sizeof(Btree2Slot) + sizeof(*page) + totKeyLen )
		return DB_OK;

	return DB_BTREE_needssplit;
}

//  compare two keys, return > 0, = 0, or < 0
//  =0: all key fields are same
//  -1: key2 > key1
//  +1: key2 < key1

int btree2KeyCmp (uint8_t *key1, uint8_t *key2, uint32_t len2)
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

//  find slot in page for given key at a given level

uint32_t btree2FindSlot (Btree1Page *page, uint8_t *key, uint32_t keyLen)
{
uint32_t diff, higher = page->cnt, low = 1, slot;
uint32_t good = 0;

	assert(higher > 0);

	//	tested as .ge. the passed key.

	while( (diff = higher - low) ) {
		slot = low + diff / 2;
		if( btree2KeyCmp (keyptr(page, slot), key, keyLen) < 0 )
			low = slot + 1;
		else
			higher = slot, good++;
	}

	//	return zero if key is on next right page

	return good ? higher : 0;
}

//  find and load page at given level for given key
//	leave page rd or wr locked as requested

DbStatus btree2LoadPage(DbMap *map, Btree2Set *set, void *key, uint32_t keyLen, uint8_t lvl) {
Btree2Index *btree2 = btree2index(map);
uint8_t drill = 0xff, *ptr;
Btree1Page *prevPage = NULL;
DbAddr prevPageNo;

  set->pageNo.bits = btree2->root.bits;
  prevPageNo.bits = 0;

  //  start at our idea of the root level of the btree2 and drill down

  do {
	// determine lock mode of drill level

	mode = (drill == lvl) ? lock : Btree1_lockRead; 
	set->page = getObj(map, set->pageNo);

	//	release parent or left sibling page

	if( prevPageNo.bits ) {
	  btree2UnlockPage(prevPage, prevMode);
	  prevPageNo.bits = 0;
	}

	if( set->page->free )
		return DB_BTREE_error;

	assert(lvl <= set->page->lvl);

	prevPageNo.bits = set->pageNo.bits;
	prevPage = set->page;
	prevMode = mode;

	//  find key on page at this level
	//  and descend to requested level

	if( !set->page->kill )
	 if( (set->slotIdx = btree2FindSlot (set->page, key, keyLen)) ) {
	  if( drill == lvl )
		return DB_OK;

	  // find next non-dead slot -- the fence key if nothing else

	  while( slotptr(set->page, set->slotIdx)->dead )
		if( set->slotIdx++ < set->page->cnt )
		  continue;
		else
		  return DB_BTREE_error;

	  // get next page down

	  ptr = keyptr(set->page, set->slotIdx);
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
