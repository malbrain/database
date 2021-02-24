//	btree_insert.c

#include "btree1.h"

extern bool debug;

DbStatus btree1LoadStopper(DbMap *map, Btree1Set *set, uint8_t lvl);

DbStatus btree1InsertKey(Handle *index, uint8_t *key, uint32_t keyLen, uint64_t aux, uint32_t auxCnt, uint8_t lvl, Btree1SlotType type) {
DbMap *idxMap = MapAddr(index);
uint32_t slotIdx, slotOff, idx;
uint32_t idx, length, fence;
Btree1Slot *slot;
Btree1Set set[1];
Btree1Page *page;
uint32_t avail;
DbStatus stat;
int64_t *ptr;

	length = keyLen + auxCnt * sizeof(uint64_t);

	while (true) {
	  memset(set, 0, sizeof(set));
	  set->keyLen = keyLen;
	  set->keyVal = key;
	  set->aux = aux;
	  set->auxCnt = auxCnt;
	  set->length = length;

	  DbStatus btree1LoadPage(DbMap * map, Btree1Set * set, bool findGood, bool stopper, uint8_t lvl);

	  if ((stat = btree1LoadPage(idxMap, set, key, keyLen,  lvl, true, stopper, Btree1_lockWrite)))
		return stat;
	  if(length + sizeof(Btree1Slot) > )
	  if ((stat = btree1CleanPage(index, set)))
		if (stat == DB_BTREE_needssplit) {
		  if ((stat = btree1SplitPage(index, set)))
			return stat;
		  else
			continue;
	    } else
			return stat;
	  break;
	}

	// set->page is empty

	if( !set->page->cnt ) {
		slot = slotptr(set->page, 1);
		set->page->act = 1;
		set->page->cnt = 1;
		set->slotIdx = 1;
		return btree1FillSlot(index, set);
	}

	if(debug) {
		slot = slotptr(set->page, set->page->cnt);
		fence = slot->off;
	}

	set->slot = slotptr(set->page, set->slotIdx);

    if( set->slotIdx < set->page->cnt )
	  if(slot->type == Btree1_librarian)
	    set->slotIdx++, slot++;

	page = set->page;
	ptr = keyaddr(page, slot->off);

	//	check for duplicate key already on the page

	if( set->slotIdx <= page->cnt )
	  if( !btree1KeyCmp(slot, key, keyLen) )
		return DB_ERROR_duplicatekey;
		
	//	if previous slot is a librarian/dead slot, use it

	if( set->slotIdx > 1 && slot[-1].type == Btree1_indexed) {
		set->slotIdx--, slot--;
		return btree1FillSlot(index, set);
	}

	//	slot now points to where the new 
	//	key will be inserted

	//	find first dead/librarian slot

	for( idx = 0; idx + set->slotIdx <= page->cnt; idx++ )
	  if (set->slotIdx > 1 && slot[-1].type == Btree1_indexed)
			break;

	//	slots 0 thru idx-1 get moved up one slot

	if( idx + set->slotIdx >= page->cnt )
		page->cnt++;

	//  move these slots out of our way

	while( idx-- ) {
		slot[idx + 1].bits[0] = slot[idx].bits[0];
		slot[idx + 1].bits[1] = slot[idx].bits[1];
	}

	return btree1FillSlot(index, set);
}

//	fill new key in new slot

uint32_t btree1FillSlot(DbMap *idxMap, Btree1Set *set) {
Btree1Page *page = set->page;
uint8_t *ptr = keyaddr(page, page->min);
uint32_t min = sizeof(Btree1Page);

	min += sizeof

	if(page->min < sizeof (Btree1Page) * page->cnt +)

	// add the key with its suffix to the page, putting suffixbytes at to

		page->act++;
	memcpy (ptr, key, totLen);
	slot->bits = set->page->min;
	slot->type = type;

	if( debug && set->page->right.bits ) {
		slot = slotptr(set->page, set->page->cnt);
		if(fence != slot->off)
			fence += 0;
	}

	btree1UnlockPage (set->page, Btree1_lockWrite);
	return DB_OK;
}

//	switch higher page parent fence key to new upper page in its parent

DbStatus btree1FixKey (Handle *index, uint8_t *fenceKey, uint64_t prev, uint64_t suffix, uint8_t lvl, bool stopper) {
DbMap *idxMap = MapAddr(index);
uint32_t keyLen, sfxLen, len;
uint8_t *ptr, *key, *dest;
ObjId oldPageId;
Btree1Set set[1];
Btree1Slot *slot;
DbStatus stat;

    key = fenceKey + keypre (fenceKey);
	keyLen = keylen(fenceKey);

	// remove prev child pageNo from interior keys

	if( lvl > 1 )
		keyLen -= Btree1_pagenobytes;

	memset(set, 0, sizeof(set));

	if ((stat = btree1LoadPage(idxMap, set, key, keyLen,  lvl, true, stopper, Btree1_lockWrite)))
		return stat;

	slot = slotptr(set->page, set->slotIdx);
	ptr = keyaddr(set->page, slot->off);
	dest = keystr(ptr);
	len = keylen(ptr);

	if( !stopper ) {
	  if( slot->type == Btree1_indexed )
		if (btree1KeyCmp (ptr, key, keyLen))
		  return DB_ERROR_invaliddeleterecord;

	  oldSlot->bits = get64(dest + len);

	  if( oldSuffix != prev )
		return DB_ERROR_invaliddeleterecord;
	}

	//	update pageAddr suffix 

	len -= Btree1_pagenobytes;
	sfxLen = store64(dest, len, suffix);
    dest[len + Btree1_pagenobytes - 1] = Btree1_pagenobytes - sfxLen;

	while( sfxLen < Btree1_pagenobytes - 1)
		dest[len + sfxLen++] = 0;

	// release write lock

	btree1UnlockPage (set->page, Btree1_lockWrite);
	return DB_OK;
}
//  compare two keys, return > 0, = 0, or < 0
//  =0: all key fields are same
//  -1: key2 > key1
//  +1: key2 < key1

int btree1KeyCmp(Btree1Slot *slot, Btree1Set *set)
{
int ans;

	ans = memcmp(slotkey(set->page, slot->off), set->keyVal, slot->length > set->keyLen ? len2 : len1);

	return ans;
}

//  find slot in page that is .ge. given search key

//	return zero if past end of all slots
//	return slot idx for key that is .ge. passed key.

uint32_t btree1FindSlot(Btree1Page *page, uint8_t *key, uint32_t keyLen)
{
	uint32_t diff, higher = page->cnt + 1, low = 1, slot;
	bool good;

	// virtual stopper key?

	if ((good = !page->right.bits))
		if (page->lvl)
			higher -= 1;

	//	low is a candidate.
	//  higher is already
	//	tested as .ge. the given key.
	//  loop ends when they meet

	while ((diff = higher - low)) {
		slot = low + diff / 2;

		if (btree1KeyCmp(keyptr(page, slot), key, keyLen) < 0)
			low = slot + 1;
		else
			higher = slot, good = true;
	}

	return good ? higher : 0;
}

//  find and load page at given level for given key
//	leave page rd or wr locked as requested

//	Librarian slots have the same key offset as their higher neighbor

DbStatus btree1LoadPage(DbMap * map, Btree1Set * set, bool findGood, bool stopper, uint8_t lvl) {
	Btree1Index *btree1 = btree1index(map);
	uint8_t drill = 0xff, *ptr;
	Btree1Page *prevPage = NULL;
	Btree1Lock mode, prevMode;
	Btree1Slot *slot;
	PageId prevPageNo;
	uint64_t bits;

	bits = btree1->root.bits;
	prevPageNo.bits = 0;

	//  start at the root level of the btree1 and drill down

	while ((set->pageNo.bits = bits)) {

		// determine lock mode of drill level

		mode = (drill == lvl) ? lock : Btree1_lockRead;
		set->page = getObj(map, set->pageNo);

		//	release parent or left sibling page

		if (prevPageNo.bits) {
			btree1UnlockPage(prevPage, prevMode);
			prevPageNo.bits = 0;
		}

		// obtain mode lock

		btree1LockPage(set->page, mode);

		if (set->page->free)
			return DB_BTREE_error;

		// re-read and re-lock root after determining actual level of root

		if (set->page->lvl != drill) {
			assert(drill == 0xff);
			drill = set->page->lvl;

			if (lock != Btree1_lockRead && drill == lvl) {
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

		if (set->page->kill) {
			bits = set->page->right.bits;
			continue;
		}

		//	if page is empty

		if (set->page->cnt == 0) {
			set->slotIdx = 0;
			return DB_OK;
		}

		// find slot on page

		if (stopper)
			set->slotIdx = set->page->cnt;
		else
			set->slotIdx = btree1FindSlot(set->page, key, keyLen);

		//  slide right into next page

		if (!set->slotIdx) {
			bits = set->page->right.bits;
			continue;
		}

		// find next higher non-dead slot

		if ((drill == lvl && findGood) || drill > lvl)
			while (set->slotIdx < set->page->cnt)
				if (slotptr(set->page, set->slotIdx)->dead)
					set->slotIdx++;
				else
					break;

		if (drill == lvl)
			return DB_OK;

		// continue on next page down

		slot = slotptr(set->page, set->slotIdx);
		ptr = keyaddr(set->page, slot->off);
		bits = zone64(keystr(ptr), keylen(ptr), Btree1_pagenobytes);

		assert(drill > 0);
		drill--;
	}

	// return error on end of right chain

	return DB_BTREE_error;
}
