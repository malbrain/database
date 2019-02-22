#include "btree1.h"

DbStatus btree1InsertSlot (Btree1Set *set, uint8_t *key, uint32_t keyLen, Btree1SlotType type);

DbStatus btree1InsertKey(Handle *index, void *key, uint32_t keyLen, uint64_t suffixValue, uint8_t lvl, Btree1SlotType type) {
Btree1Index *btree1 = btree1index(index->map);
uint8_t keyBuff[MAX_key];
Btree1Set set[1];
DbStatus stat;

	if (keyLen > MAX_key)
		return DB_ERROR_keylength;

	memcpy(keyBuff, key, keyLen);
	keyLen += store64(keyBuff, keyLen, suffixValue, btree1->base->binaryFlds);

	memset(set, 0, sizeof(set));

	while (true) {
	  if ((stat = btree1LoadPage(index->map, set, keyBuff, keyLen, lvl, Btree1_lockWrite, false)))
		return stat;

	  if ((stat = btree1CleanPage(index, set, keyLen))) {
		if (stat == DB_BTREE_needssplit) {
		  if ((stat = btree1SplitPage(index, set)))
			return stat;
		  else
			continue;
	    } else
			return stat;
	  }

	  // add the key to the page

	  return btree1InsertSlot (set, keyBuff, keyLen, type);
	}

	return DB_OK;
}

//	update page's fence key in its parent

DbStatus btree1FixKey (Handle *index, uint8_t *fenceKey, uint8_t lvl, bool stopper) {
uint32_t keyLen = keylen(fenceKey);
Btree1Set set[1];
Btree1Slot *slot;
uint8_t *ptr;
DbStatus stat;

	if ((stat = btree1LoadPage(index->map, set, fenceKey + keypre(fenceKey), keyLen - sizeof(uint64_t), lvl, Btree1_lockWrite, stopper)))
		return stat;

	slot = slotptr(set->page, set->slotIdx);
	ptr = keyptr(set->page, set->slotIdx);

	// if librarian slot

	if (slot->type == Btree1_librarian) {
		slot = slotptr(set->page, ++set->slotIdx);
		ptr = keyptr(set->page, set->slotIdx);
	}

	// update child pageNo

	assert(!memcmp(ptr, fenceKey, keyLen + keypre(fenceKey) - sizeof(uint64_t)));
	assert(keylen(ptr) == keyLen);

	memcpy(ptr + keypre(ptr) + keylen(ptr) - sizeof(uint64_t), fenceKey + keypre(fenceKey) + keyLen - sizeof(uint64_t), sizeof(uint64_t));

	// release write lock

	btree1UnlockPage (set->page, Btree1_lockWrite);
	return DB_OK;
}

//	install new key onto page
//	page must already be checked for
//	adequate space

DbStatus btree1InsertSlot (Btree1Set *set, uint8_t *key, uint32_t keyLen, Btree1SlotType type) {
uint32_t idx, prefixLen;
Btree1Slot *slot;
uint8_t *ptr;

	//	if found slot > desired slot and previous slot
	//	is a librarian slot, use it

	if( set->slotIdx > 1 )
	  if( slotptr(set->page, set->slotIdx-1)->type == Btree1_librarian )
		set->slotIdx--;

	//	calculate key length

	prefixLen = keyLen < 128 ? 1 : 2;

	// copy key onto page

	set->page->min -= prefixLen + keyLen;
	ptr = keyaddr(set->page, set->page->min);

	if( keyLen < 128 )	
		*ptr++ = keyLen;
	else
		*ptr++ = keyLen/256 | 0x80, *ptr++ = keyLen;

	memcpy (ptr, key, keyLen);
	slot = slotptr(set->page, set->slotIdx);
	
	//	find first empty slot

	for( idx = set->slotIdx; idx < set->page->cnt; slot++, idx++ )
		if( slot->dead )
			break;

	if( idx == set->page->cnt )
		idx++, set->page->cnt++, slot++;

	set->page->act++;

	while( idx-- > set->slotIdx )
		slot->bits = slot[-1].bits, slot--;

	//	fill in new slot

	slot->bits = set->page->min;
	slot->type = type;

	btree1UnlockPage (set->page, Btree1_lockWrite);
	return DB_OK;
}

