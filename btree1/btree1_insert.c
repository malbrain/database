//	btree_insert.c

#include "btree1.h"


extern bool debug;

DbStatus btree1InsertKey(Handle *index, DbKeyDef *kv, uint8_t lvl, Btree1SlotType type) {
DbMap *idxMap = MapAddr(index);
uint32_t length;
Btree1Slot *slot;
Btree1Set set[1];
Btree1Page *page;
int32_t max, cnt;
int32_t idx, tst;
DbStatus stat;
uint8_t *ptr;

  length = kv->keyLen;

  while (true) {
	memset(set, 0, sizeof(set));
	set->keyLen = kv->keyLen;
	set->keyVal = kv->bytes;
	set->auxLen = kv->suffixLen;
	set->length = length;

	//  drill down to lvl page containing key

	if ((stat = btree1LoadPage(idxMap, set, Btree1_lockWrite, false, false, lvl)))
		return stat;

	// dest page overflow?
	// if so, split the page

	if (set->page->min < sizeof(Btree1Slot) * set->page->cnt + length + sizeof(Btree1Page))
	  if(( stat = btree1SplitPage(index, set)))
		return stat;
	  else
		continue;

	page = set->page;
	assert(set->slotIdx < page->cnt);
	slot = slotptr(set->page, set->slotIdx);

	if( set->slotIdx < set->page->cnt )
	  if(slot->type == Btree1_librarian)
		set->slotIdx++, slot++;

	//	check for duplicate key already on the page

	if( set->slotIdx <= set->page->cnt )
	  if( !btree1KeyCmp(set->page, set->slotIdx, kv->bytes, kv->keyLen) )
		return DB_ERROR_duplicatekey;

	//	slot now points to where the new 
	//	key would be inserted when open

	//	find nearest open `dead/librarian slot
	//	and bubble dead to slot[0]

	max = set->page->cnt;
	tst = set->slotIdx;
	idx = 0;

	do {
	  if( cnt = idx++ + tst < max ) {
		if( slot[idx].dead ) do {
		  slot[idx].bits[0] = slot[idx-1].bits[0];
		  slot[idx].bits[1] = slot[idx-1].bits[1];
		} while( --idx );
		break;
	  }
	  
	  if( idx < max && ++cnt ) {
        if( slot[-idx].dead ) do {
		  slot[-idx].bits[0] = slot[-idx + 1].bits[0];
		  slot[-idx].bits[1] = slot[-idx + 1].bits[1];
		} while( --idx );
		break;
	  }
	} while( cnt > 0 );

	assert((uint64_t)page->min < sizeof (Btree1Slot) * (uint64_t)(page->cnt + 1) + (uint64_t)set->length + sizeof(Btree1Page));

	slot->off = page->min -= set->length;

	// add the key with its suffix to the page

	ptr = keyaddr(page, page->min);
	page->act += 1;
	memcpy (ptr, kv->bytes, set->length);
	slot->type = type;
  }
  btree1UnlockPage (set->page, Btree1_lockWrite);
  return DB_OK;
}

//  compare two keys, return > 0, = 0, or < 0
//  =0: all key fields are same
//  -1: key2 > key1
//  +1: key2 < key1

int btree1KeyCmp(Btree1Page *page, uint32_t idx, uint8_t *keyVal, uint32_t keyLen)
{
Btree1Slot *slot = slotptr(page, idx);
int ans;

	ans = memcmp(keyptr(page, idx), keyVal, MIN(slot->length, keyLen));

	return ans;
}

//  find slot in page that is .ge. given search key

//	return zero if past end of all slots
//	return slot idx for key that is .ge. passed key.

uint32_t btree1FindSlot(Btree1Page *page, uint8_t *key, uint32_t keyLen)
{
uint32_t diff, higher = page->cnt + 1, low = 1, idx;
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
		idx = low + diff / 2;

		if (btree1KeyCmp(page, idx, key, keyLen) < 0)
			low = idx + 1;
		else
			higher = idx, good = true;
	}

	return good ? higher : 0;
}

//  lock and load page at given level for given key
//	Librarian slots have the same key offset as their higher neighbor

DbStatus btree1LoadPage(DbMap * map, Btree1Set * set, Btree1Lock lock, bool findGood, bool stopper, uint8_t lvl) {
Btree1Index *btree1 = btree1index(map);
uint8_t drill = 0xff;
Btree1Page *prevPage = NULL;
Btree1Lock mode, prevMode;
Btree1Slot *slot;
PageId prevPageId;
uint64_t bits;

	bits = btree1->root.bits;
	prevPageId.bits = 0;

	//  start at the root level of the btree1 and drill down

	while ((set->pageId.bits = bits)) {

	// determine lock mode of drill level

		mode = (drill == lvl) ? lock : Btree1_lockRead;
		set->pageAddr = fetchIdSlot(map, set->pageId);
		set->page = getObj(map, *set->pageAddr);

		//	release parent or left sibling page

		if (prevPageId.bits) {
			btree1UnlockPage(prevPage, prevMode);
			prevPageId.bits = 0;
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

		prevPageId.bits = set->page->self.bits;
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
			set->slotIdx = btree1FindSlot(set->page, set->keyVal, set->keyLen);

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
		assert(drill > 0);
		drill--;
	}

	// return error on end of right chain

	return DB_BTREE_error;
}
