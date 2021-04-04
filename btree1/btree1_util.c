//	btree1_util.c

#include "btree1.h"

//	debug slot function

#ifndef _DEBUG
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

uint32_t librarianDensity = 3;
extern bool stats;
uint32_t Splits;

// function to copy keys from one page to another

//	move a segment of keys to a new page
//	idx - first source slot moved to dest (start with zero)
//	max - final source slot to be moved

uint32_t btree1SplitCopy(Btree1Page *destPage, Btree1Page *slotPage, uint32_t idx, uint32_t max) {
uint32_t librarianIdx = 0, cnt = destPage->cnt;
Btree1Slot *slot, *dest;

  dest = slotptr(destPage, destPage->cnt);

  while (++idx <= max) {	
	slot = slotptr(slotPage, idx);

	if (slot->dead)
		continue;

	// dest page overflow

	if (destPage->min < sizeof(Btree1Slot) * cnt + sizeof(Btree1Page))
		return idx;

	//  librarian slot inserts
	//	never the highest slot index
	
	if(librarianDensity )
	  if(++librarianIdx % librarianDensity == 0) {
	    dest->bits[0] = 0;
		dest->type = Btree1_librarian;
		dest->dead = true;
		dest++;
		cnt++;
	  };

	dest->off = destPage->min -= slot->length;
	dest->payLoad = slot->payLoad;
	dest->length = slot->length;
	dest->suffix = slot->suffix;
	dest++;
	cnt++;

	memcpy(destPage->base + dest->off, slotPage->base + slot->off, slot->length);
  }

  destPage->cnt += cnt;
  return idx;
}

//  split already locked full node into two (left & right)
//  each with 1/2 of the keys lower (left) and higher (right)
//  if this was the root page, pass lower/upper to split root
//	return with pages unlocked.
// split the page and raise the height of the btree1
// call with key for smaller (left) half and right page addr.

DbStatus btree1SplitPage(Handle *index, Btree1Set	*set) {
DbMap *idxMap = MapAddr(index);
Btree1Index *btree1 = btree1index(idxMap);
Btree1Page *leftPage, *rightPage, *rootPage;
uint8_t lvl = set->page->lvl;
uint32_t max, idx;
Btree1Slot *dest, *slot;

if (stats)
atomicAdd32(&Splits, 1);

//  copy lower keys into a new empty left page

	if ((leftPage = btree1NewPage(index, lvl, Btree1_interior)))
		max = set->page->cnt;
	else
		return DB_ERROR_outofmemory;

	if( !(idx = btree1SplitCopy(leftPage, set->page, 0, max / 2)))
		return DB_ERROR_outofmemory;

	//	construct higher (rightPage) page
	//	from remaining half of old root (set->page)

	if (!(rightPage = btree1NewPage(index, lvl, Btree1_interior )))
		return DB_ERROR_outofmemory;

	//	fill lower keys (leftPage) page
	//	from lower half of overflowing (set->page)

	if( !( idx = btree1SplitCopy(rightPage, set->page, idx, max)))
		return DB_ERROR_outofmemory;

	rightPage->left = leftPage->self;
	rightPage->right = set->page->right;

	leftPage->left = set->page->left;
	leftPage->right = rightPage->self;

	if(set->page->type == Btree1_rootPage){ 

		// insert stopper key on new root page
		// pointing to the new right half page 

		if (!(rootPage = btree1NewPage(index, lvl + 1, Btree1_rootPage)))
			return DB_ERROR_outofmemory;

		rootPage->cnt = 2;
		rootPage->act = 2;

		// newroot slot 2 constructed right pageId

		dest = slotptr(rootPage, 2);
		dest->type = Btree1_stopper;
		dest->payLoad.bits = rightPage->self.bits;

		// highest lower keys (left) on newroot
		//	and higher keys (stopper)

		slot = slotptr(leftPage, leftPage->cnt);

		dest = slotptr(rootPage, 1);
		dest->type = Btree1_indexed;
		dest->off = leftPage->min -= slot->length;
		dest->length = slot->length;
		dest->suffix = slot->suffix;
		dest->payLoad.bits = leftPage->self.bits;

		memcpy(keyaddr(rootPage, dest->off), keyaddr(leftPage, slot->off), slot->length);

		// install new root, return old root
		btree1->root.bits = rootPage->self.bits;
		//
//if (addSlotToFrame(idxMap, listFree(index, set->page->self.type), NULL,
//		set->page->self.bits))
			return DB_OK;
//		else
			return DB_ERROR_outofmemory;
	}
	// todo: locks & freelist
//install new root
	return DB_OK;
}

	// insert left/right page fence keys

	// insert new fence in the parent page
/*
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

*/
