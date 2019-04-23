#include "btree1.h"

DbStatus btree1LoadStopper(DbMap *map, Btree1Set *set, uint8_t lvl);

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
uint32_t idx, pfxLen;
Btree1Slot *slot;
Btree1Set set[1];
DbStatus stat;
uint8_t *ptr;

	pfxLen = totLen < 128 ? 1 : 2;

	if (keyLen > MAX_key)
		return DB_ERROR_keylength;

	while (true) {
	  memset(set, 0, sizeof(set));

	  if ((stat = btree1LoadPage(index->map, set, key, totLen, lvl, false, Btree1_lockWrite)))
		return stat;

	  if ((stat = btree1CleanPage(index, set, totLen + pfxLen))) {
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

	//	if previous slot is a librarian/dead slot, use it

	if( set->slotIdx > 1 && slot[-1].dead )
		set->slotIdx--, slot--;

	//	slot now points to where the new 
	//	key will be inserted

	// add the key to the page

	set->page->min -= pfxLen + totLen;
	ptr = keyaddr(set->page, set->page->min);

	if( totLen < 128 )	
		*ptr++ = totLen;
	else
		*ptr++ = totLen/256 | 0x80, *ptr++ = totLen;

	memcpy (ptr, key, totLen);
	
	//	find first dead/librarian slot

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
uint32_t keyLen, sfxLen, len;
uint8_t *ptr, *key, *dest;
uint64_t oldSuffix;
Btree1Set set[1];
Btree1Slot *slot;
DbStatus stat;

    key = fenceKey + keypre (fenceKey);
	keyLen = keylen(fenceKey);

	// remove prev child pageNo from interior keys

	if( lvl > 1 )
		keyLen -= Btree1_pagenobytes;

	memset(set, 0, sizeof(set));

	if( stopper ) {
	  if( (stat = btree1LoadStopper (index->map, set, lvl)) )
		return stat;
	} else {
	  if ((stat = btree1LoadPage(index->map, set, key, keyLen,  lvl, true, Btree1_lockWrite)))
		return stat;
	}

	slot = slotptr(set->page, set->slotIdx);
	ptr = keyaddr(set->page, slot->off);
	dest = keystr(ptr);
	len = keylen(ptr);

	if( !set->page->right.bits ) {
	  if( slot->type == Btree1_indexed )
		if (btree1KeyCmp (ptr, key, keyLen))
		  return DB_ERROR_invaliddeleterecord;

	  oldSuffix = get64(dest, len, false);

	  if( oldSuffix != prev )
		return DB_ERROR_invaliddeleterecord;
	}

	//	overwrite pageAddr 

	len -= Btree1_pagenobytes;
	sfxLen = store64(dest, len, suffix, false);

	while( sfxLen < Btree1_pagenobytes )
		dest[len + sfxLen++] = 0;

	// release write lock

	btree1UnlockPage (set->page, Btree1_lockWrite);
	return DB_OK;
}
