//	btree1_util.c

#include "btree1.h"

//	debug slot function

#ifndef DEBUG
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

//uint32_t Splits;

// split the root and raise the height of the btree1
// call with fence key for smaller (left) half and right page addr.

DbStatus btree1SplitRoot(Handle *index, Btree1Set *set, PageId right, uint8_t *leftKey) {
DbMap *idxMap = MapAddr(index);
Btree1Index *btree1 = btree1index(idxMap);
uint32_t keyLen, nxt = btree1->pageSize;
Btree1Page *leftPage, *rightPage;
DbAddr *rightAddr;
Btree1Slot *slot;
uint32_t off, amt;

	//  copy lower keys into new left empty page from passed
	//  old root page contents into it, e.g. lower keys

	if( (leftPage = btree1NewPage(index, root->page->lvl, )) )
		memcpy(leftPage->latch + 1, root->page->latch + 1, btree1->pageSize - sizeof(*leftPage->latch));
	else
		return DB_ERROR_outofmemory;

	//  circlar sibling relations

	rightAddr = fetchIdSlot(idxMap, right);
	rightPage = getObj(idxMap, *rightAddr);

	rightPage->left = leftPage->self;
	leftPage->right = rightPage->self;

	// preserve the page info at the bottom
	// of higher keys page and set rest to zero

	memset(root->page+1, 0, btree1->pageSize - sizeof(*root->page));

	// insert stopper key on new root page
	// pointing to the new right half page 
	// and increase the root height

	// key slot 2 constructed with no keyLen, right pageId

	slot = slotptr(root->page, 2);
	slot->type = Btree1_stopper;

	// next insert lower keys (left) fence key on newroot page as
	// first key and reserve space for the key.

	keyLen = keylen(leftKey);

	if( leftPage->lvl )
		keyLen -= Btree1_pagenobytes;

	amt = keyLen + Btree1_pagenobytes;

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
	amt = store64(dest, keyLen, left.bits);

	dest[keyLen + Btree1_pagenobytes - 1] = Btree1_pagenobytes - amt;

	while( amt < Btree1_pagenobytes - 1)
		dest[keyLen + amt++] = 0;

	root->page->right.bits = 0;
	root->page->min = nxt;
	root->page->cnt = 2;
	root->page->act = 2;
	root->page->lvl++;

	// release root page

	btree1UnlockPage(root->page, Btree1_lockWrite);
	return DB_OK;
}

//  split already locked full node into two (left & right)
//  each with 1/2 of the keys lower (left) and higher (right)
//  if this was the root page, pass lower/upper to split root
//	return with pages unlocked.

DbStatus btree1SplitPage (Handle *index, Btree1Set *set) {
DbMap *idxMap = MapAddr(index);
Btree1Index *btree1 = btree1index(idxMap);
uint8_t *leftKey, *rightKey, *fence;
uint32_t cnt = 0, idx = 0, max, nxt;
Btree1Slot librarian, *source, *dest;
uint32_t size = btree1->pageSize;
Btree1Page *leftPage, *rightPage;
uint32_t leftLen, rightLen;
uint8_t lvl = set->page->lvl;
uint32_t totLen, keyLen;
uint8_t *key, *ptr;
DbAddr right, addr;
bool stopper;
DbStatus stat;

	if( stats )
		atomicAdd32(&Splits, 1);

	librarian->bits = 0;
	librarian.type = Btree1_librarian;
	librarian.dead = 1;

	if( !set->page->lvl )
		size <<= btree1->leafXtra;

	//	get new page and write higher keys to it.

	if( (right.bits = btree1NewPage(index, lvl)) )
		rightPage = getObj(idxMap, right);
	else
		return DB_ERROR_outofmemory;

	max = set->page->cnt;
	cnt = max / 2;
	nxt = size - 1;
	idx = 0;
	}
}

//move a segment of keys to a new page
//	idx - last source slot moved to dest
//	cnt - number of source slots to move

DbStatus btree1SplitWork(Handle *index, Btree1Page *srcPage, Btree1Page *dstPage, uint32_t idx, uint32_t cnt ) {
DbMap *idxMap = MapAddr(index);
Btree1Index *btree1 = btree1index(idxMap);
		uint8_t *leftKey, *rightKey, *fence;
		uint32_t cnt = 0, idx = 0, max, nxt;
Btree1Slot librarian, *source, *dest;

	source = slotptr(srcPage, idx);
	dest = slotptr(dstPage, dstPage->cnt);

	while( source++, cnt-- ) {
		if( source->dead )
			continue;

		//	add librarian slot per coniguration

		if (dstpage->cnt % index->librarianDensity == 0) {
			(++dest)->bits = librarian.bits;
			dest->off = nxt;
			dstPage->cnt++;
		}

		//  transfer slot key

		key = keyaddr(srcPage, source->off);
		nxt -= source->length;

		ptr = keyaddr(destPage, nxt);
		memcpy(ptr, key, source->length);
		dstPage->cnt++;

		//	transfer slot meta data

		(++dest)->bits[0] = source->bits[0]
		(dest)->bits[1] = source->bits[1];
		destPage->min = nxt;
		dest->off = nxt;
	}
}

#ifdef DEBUG
	key = keyaddr(frame, source[2].off);
	assert(keylen(key) > 0);
#endif	
/*	//  assemble page of smaller keys from temporary frame copy

 	while( source++, cnt++ < max ) {
		if( source->dead )
			continue;

		key = keyaddr(frame, source->off);
		totLen = keylen(key) + keypre(key);
		nxt -= totLen;

		ptr = keyaddr(set->page, nxt);
		memcpy (ptr, key, totLen);
		set->page->act++;

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
	}

	memcpy(leftKey, key, totLen);
	set->page->min = nxt;
	set->page->cnt = idx;

	// link right node

	rightPage->right.bits = set->page->right.bits;
	rightPage->left.bits = set->pageNo.bits;

	if( (stopper = rightPage->right.bits) ) {
		Btree1Page *farRight = getObj(idxMap, rightPage->right);
		btree1LockPage (farRight, Btree1_lockLink);
		farRight->left.bits = right.bits;
		btree1UnlockPage (farRight, Btree1_lockLink);
	}

	if( !lvl && !rightPage->right.bits )
		btree1->right.bits = right.bits;

	// link left page to right page
	//	leaving existing left and far left links in place

	set->page->right.bits = right.bits;

	// if current full page is the root page, split it

	if (set->page->type == Btree1_rootPage) {
      if (!(stat = btree1SplitRoot(index, set, fenceKey)))
        if (addSlotToFrame(idxMap, listFree(index, addr.type), NULL,
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
		keyLen -= Btree1_pagenobytes;  // strip off pageNo

	//	add key for page of smaller keys to parent 

    if ((stat = btree1InsertSfxKey(index, keystr(leftKey), keyLen, set->pageNo.bits, lvl + 1, Btree1_indexed)))
		return stat;

	// switch parent key for larger keys to new right page

	if( (stat = btree1FixKey(index, rightKey, set->pageNo.bits, right.bits, lvl+1, !stopper) ))
		return stat;

	btree1UnlockPage (set->page, Btree1_lockParent);
	btree1UnlockPage (rightPage, Btree1_lockParent);

	if (addSlotToFrame(idxMap, listFree(index, addr.type), NULL, addr.bits))
          return DB_OK;

    return DB_ERROR_outofmemory;
}

//	check page for space available,
//	clean if necessary and return
//	false - page needs splitting
//	true  - ok to insert

DbStatus btree1CleanPage(Handle *index, Btree1Set *set) {
DbMap *idxMap = MapAddr(index);
Btree1Index *btree1 = btree1index(idxMap);
Btree1Slot librarian, *source, *dest;
uint32_t size = btree1->pageSize;
Btree1Page *page = set->page;
uint32_t max = page->cnt;
uint32_t len, cnt, idx;
uint32_t newSlot = max;
Btree1PageType type;
Btree1Page *frame;
uint32_t totKeyLen;
uint8_t *key;
DbAddr addr;

	librarian.bits = 0;
	librarian.type = Btree1_librarian;
	librarian.dead = 1;

	if( !page->lvl ) {
		size <<= btree1->leafXtra;
		type = Btree1_leafPage;
	} else
		type = Btree1_interior;

	if( page->min >= (max+1) * sizeof(Btree1Slot) + sizeof(*page) + totKeyLen )
		return DB_OK;

	//	skip cleanup and proceed directly to split
	//	if there's not enough garbage
	//	to bother with.

	if( page->garbage < size / 5 )
		return DB_BTREE_needssplit;

	if( (addr.bits = allocObj(idxMap, listFree(index, type), NULL, type, size, false)) )
		frame = getObj(idxMap, addr);
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

	addSlotToFrame(idxMap, listFree(index,addr.type), NULL, addr.bits);

	//	see if page has enough space now, or does it still need splitting?

	if( page->min >= (idx+1) * sizeof(Btree1Slot) + sizeof(*page) + totKeyLen )
		return DB_OK;

	return DB_BTREE_needssplit;
}

