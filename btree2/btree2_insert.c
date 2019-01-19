#include "btree2.h"
#include "btree2_slot.h"

DbStatus btree2InsertKey(Handle *index, void *key, uint32_t keyLen, uint8_t lvl, Btree2SlotState type) {
uint8_t height = btree2GenHeight(index);
uint32_t totKeyLen = keyLen;
Btree2Set set[1];
DbStatus stat;

	if (keyLen < 128)
		totKeyLen += 1;
	else
		totKeyLen += 2;

	while (true) {
	  if ((stat = btree2LoadPage(index->map, set, key, keyLen - (lvl ? sizeof(uint64_t) : 0), lvl)))
		return stat;

	  if ((stat = btree2CleanPage(index, set, totKeyLen, height))) {
		if (stat == DB_BTREE_needssplit) {
		  if ((stat = btree2SplitPage(index, set, height)))
			return stat;
		  else
			continue;
	    } else
			return stat;
	  }

	  // add the key to the page

	  return btree2InstallKey (set->page, key, keyLen, height);
	}

	return DB_OK;
}

//	check page for space available,
//	clean if necessary and return
//	>0 number of skip units needed
//	=0 split required

DbStatus btree2CleanPage(Handle *index, Btree2Set *set, uint32_t totKeyLen, uint8_t height) {
Btree2Index *btree2 = btree2index(index->map);
uint16_t skipUnit = 1 << set->page->skipBits;
uint32_t spaceReq, size = btree2->pageSize;
int type = set->page->pageType;
Btree2Page *newPage;
Btree2Page *clean;
Btree2Slot *slot;
uint8_t *key;
DbAddr addr;
ObjId *pageNoPtr;
uint16_t *tower, off;
DbStatus stat;


	if( !set->page->lvl )
		size <<= btree2->leafXtra;

	spaceReq = (sizeof(Btree2Slot) + set->height * sizeof(uint16_t) + totKeyLen + skipUnit - 1) / skipUnit;

	if( set->page->nxt + spaceReq <= size)
		return spaceReq;

	//	skip cleanup and proceed directly to split
	//	if there's not enough garbage
	//	to bother with.

	if( *set->page->garbage < size / 5 )
		return DB_BTREE_needssplit;

	if( (addr.bits = btree2NewPage(index, set->page->lvl, set->page->pageType)) )
		newPage = getObj(index->map, addr);
	else
		return DB_ERROR_outofmemory;

	//	copy over keys from old page into new page

	tower = set->page->skipHead;

	while( newPage->nxt < newPage->size / 2 )
		if( (off = tower[0]) ) {
			slot = slotptr(set->page,off);
			if( install8(slot->state, Btree2_slotactive, Btree2_slotmoved) == Btree2_slotactive )
				btree2InstallSlot(newPage, slot, height);
			tower = slot->tower;
		} else
			break;

	//	install new page addr into original pageNo slot

	pageNoPtr = fetchIdSlot(index->map, set->pageNo);
	pageNoPtr->bits = addr.bits;

	return DB_BTREE_needssplit;
}

//	update page's fence key in its parent

DbStatus btree2FixKey (Handle *index, uint8_t *fenceKey, uint8_t lvl) {
uint32_t keyLen = keylen(fenceKey);
Btree2Set set[1];
Btree2Slot *slot;
uint8_t *ptr;
DbStatus stat;

	if ((stat = btree2LoadPage(index->map, set, fenceKey + keypre(fenceKey), keyLen - sizeof(uint64_t), lvl)))
		return stat;

	ptr = slotkey(set->slot);

	// update child pageNo

	assert(!memcmp(ptr, fenceKey, keyLen + keypre(fenceKey) - sizeof(uint64_t)));
	assert(keylen(ptr) == keyLen);

	memcpy(ptr + keypre(ptr) + keylen(ptr) - sizeof(uint64_t), fenceKey + keypre(fenceKey) + keyLen - sizeof(uint64_t), sizeof(uint64_t));

	return DB_OK;
}

uint16_t btree2InstallSlot (Btree2Page *page, Btree2Slot *slot, uint8_t height) {
	return btree2InstallKey (page, slotkey(slot), keylen(slotkey(slot)), height);
}

//	install new key onto page
//	return page offset, or zero

uint16_t btree2InstallKey (Btree2Page *page, uint8_t *key, uint32_t keyLen, uint8_t height) {
uint32_t prefixLen, totalLen, slotSize;
Btree2Slot *newSlot;
uint16_t off;
uint8_t *ptr;

	//	calculate key length

	keyLen = keylen(key);
	prefixLen = keyLen < 128 ? 1 : 2;

	//	allocate space on page

	slotSize = prefixLen + keyLen + sizeof(*newSlot) + height * sizeof(uint16_t); 

	// copy slot onto page

	if( (off = btree2SlotAlloc(page, prefixLen + keyLen, height)) ) {
		newSlot = slotptr(page, off);
		ptr = slotkey(newSlot);

		if( keyLen < 128 )	
			*ptr++ = keyLen;
		else
			*ptr++ = keyLen/256 | 0x80, *ptr++ = keyLen;

	
		//	fill in new slot

		memcpy (ptr, key, keyLen);
		newSlot->slotState = Btree2_slotactive;
		newSlot->height = height;
	}

	return off;
}

