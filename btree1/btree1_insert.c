#include "btree1.h"

DbStatus btree1InsertSfxKey(Handle *index, uint8_t *key, uint32_t keyLen, uint64_t suffix, uint8_t lvl, Btree1SlotType type) {
uint8_t keyBuff[MAX_key];
uint32_t sfxLen;

    memcpy(keyBuff, key, keyLen);
	sfxLen = store64(keyBuff, keyLen, suffix, false);

	while( sfxLen < Btree1_pagenobytes )
		keyBuff[keyLen + sfxLen++] = 0;

	return btree1InsertKey(index, keyBuff, keyLen, sfxLen, lvl, type);
}

DbStatus btree1InsertKey(Handle *index, uint8_t *key, uint32_t keyLen, uint32_t sfxLen, uint8_t lvl, Btree1SlotType type) {
Btree1Index *btree1 = btree1index(index->map);
uint32_t totLen = keyLen + sfxLen;
uint32_t idx, prefixLen;
Btree1Slot *slot;
Btree1Set set[1];
DbStatus stat;
uint8_t *ptr;

	if (keyLen > MAX_key)
		return DB_ERROR_keylength;

	while (true) {
	  memset(set, 0, sizeof(set));

	  if ((stat = btree1LoadPage(index->map, set, key, totLen, lvl, Btree1_lockWrite)))
		return stat;

	  if ((stat = btree1CleanPage(index, set, totLen))) {
		if (stat == DB_BTREE_needssplit) {
		  if ((stat = btree1SplitPage(index, set)))
			return stat;
		  else
			continue;
	    } else
			return stat;
	  }

	  break;
	}

	if( set->page->cnt ) {
	  slot = slotptr(set->page, set->slotIdx);

	  ptr = keyaddr(set->page, slot->off);

	  if( !btree1KeyCmp(ptr, key, totLen) )
		return DB_ERROR_duplicatekey;

	} else {
		slot = slotptr(set->page, 1);
		set->slotIdx = 1;
		slot->dead = true;
	}

	//	if previous slot is a librarian slot, use it

	if( set->slotIdx > 1 && slot[-1].type == Btree1_librarian )
		set->slotIdx--, slot--;

	//	slot now points to where the new 
	//	key will be inserted

	// add the key to the page

	prefixLen = totLen < 128 ? 1 : 2;
	set->page->min -= prefixLen + totLen;
	ptr = keyaddr(set->page, set->page->min);

	if( totLen < 128 )	
		*ptr++ = totLen;
	else
		*ptr++ = totLen/256 | 0x80, *ptr++ = totLen;

	memcpy (ptr, key, totLen);
	
	//	find first empty slot

	for( idx = 0; idx + set->slotIdx <= set->page->cnt; idx++ )
	  if( slot[idx].dead )
		break;

	if( idx + set->slotIdx >= set->page->cnt )
		set->page->cnt++;

	set->page->act++;
	  
	//  move subsequent slots out of our way

	while( idx-- )
		slot[idx + 1].bits = slot[idx].bits;

	//	fill in new slot

	slot->bits = set->page->min;
	slot->type = type;

	btree1UnlockPage (set->page, Btree1_lockWrite);
	return DB_OK;
}

//	update page's fence key in its parent

DbStatus btree1FixKey (Handle *index, uint8_t *fenceKey, uint64_t prev, uint64_t suffix, uint8_t lvl, bool stopper) {
uint8_t keyBuff[MAX_key];
uint32_t keyLen, sfxLen;
Btree1Set set[1];
Btree1Slot *slot;
uint8_t *ptr, *key;
DbStatus stat;

    key = fenceKey + keypre (fenceKey);
	keyLen = keylen(fenceKey);

	memset(set, 0, sizeof(set));

	// add prev child pageNo to key

	if( lvl > 1 )
		keyLen -= Btree1_pagenobytes;

	memcpy(keyBuff, key, keyLen);
	sfxLen = store64(keyBuff, keyLen, prev, false);

	while( sfxLen < Btree1_pagenobytes )
		keyBuff[keyLen + sfxLen++] = 0;

	memset(set, 0, sizeof(set));

	if ((stat = btree1LoadPage(index->map, set, keyBuff, keyLen + sfxLen, lvl, Btree1_lockWrite)))
		return stat;

	slot = slotptr(set->page, set->slotIdx);

	// if librarian slot

	if (slot->type == Btree1_librarian)
		slot = slotptr(set->page, ++set->slotIdx);

	ptr = keyaddr(set->page, slot->off);

	if( slot->type == Btree1_indexed )
	  if( btree1KeyCmp(ptr, keyBuff, keyLen) )
		return DB_ERROR_invaliddeleterecord;

	sfxLen = store64(keystr(ptr), keylen(ptr), suffix, false);

	while( sfxLen < Btree1_pagenobytes )
		keyBuff[keylen(ptr) + sfxLen++] = 0;

	// release write lock

	btree1UnlockPage (set->page, Btree1_lockWrite);
	return DB_OK;
}
