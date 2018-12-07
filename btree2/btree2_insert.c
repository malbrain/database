#include "btree2.h"

DbStatus btree2InsertSlot (Btree2Set *set, uint8_t *key, uint32_t keyLen, Btree2SlotType type);

DbStatus btree2InsertKey(Handle *index, void *key, uint32_t keyLen, uint8_t lvl, Btree2SlotType type) {
uint32_t totKeyLen = keyLen;
Btree2Set set[1];
DbStatus stat;

	if (keyLen < 128)
		totKeyLen += 1;
	else
		totKeyLen += 2;

	while (true) {
	  if ((stat = btree2LoadPage(index->map, set, key, keyLen - (lvl ? sizeof(uint64_t) : 0), lvl, false)))
		return stat;

	  if ((stat = btree2CleanPage(index, set, totKeyLen))) {
		if (stat == DB_BTREE_needssplit) {
		  if ((stat = btree2SplitPage(index, set)))
			return stat;
		  else
			continue;
	    } else
			return stat;
	  }

	  // add the key to the page

	  return btree2InsertSlot (set, key, keyLen, type);
	}

	return DB_OK;
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

	slot = slotptr(set->page, set->slotIdx);
	ptr = keyptr(set->page, set->slotIdx);

	// update child pageNo

	assert(!memcmp(ptr, fenceKey, keyLen + keypre(fenceKey) - sizeof(uint64_t)));
	assert(keylen(ptr) == keyLen);

	memcpy(ptr + keypre(ptr) + keylen(ptr) - sizeof(uint64_t), fenceKey + keypre(fenceKey) + keyLen - sizeof(uint64_t), sizeof(uint64_t));

	return DB_OK;
}

//	install new key onto page
//	page must already be checked for
//	adequate space

DbStatus btree2InsertSlot (Btree2Set *set, uint8_t *key, uint32_t keyLen, Btree2SlotType type) {
uint32_t idx, prefixLen;
Btree2Slot *slot;
uint8_t *ptr;

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

	//	fill in new slot

	slot->bits = set->page->min;
	slot->type = type;

	return DB_OK;
}

