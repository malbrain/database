#include "btree1.h"

extern bool debug;

DbStatus btree1LoadStopper(DbMap *map, Btree1Set *set, uint8_t lvl);

DbStatus btree1InsertKey(Handle *index, uint8_t *key, uint32_t keyLen, int64_t *suffix, uint32_t suffixCnt, uint8_t lvl, Btree1SlotType type) {
DbMap *idxMap = MapAddr(index);
uint32_t slotIdx, slotOff, idx;
uint32_t idx, pfxLen, fence;
uint32_t fence, pfxLen;
Btree1Slot *slot;
Btree1Set set[1];
Btree1Page *page;
uint32_t avail;
DbStatus stat;
uint8_t *ptr;

	while (true) {
	  memset(set, 0, sizeof(set));

	  // find first node/page larger than our key

	  if ((stat = btree1LoadPage(idxMap, set, Btree1_lockWrite)))
		return stat;

	  if ((stat = btree1CleanPage(index, set))) {
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

	if( !set->page->cnt ) {
		slot = slotptr(set->page, 1);
		set->page->act = 1;
		set->page->cnt = 1;
		set->slotIdx = 1;
		goto fillSlot;
	}

	if(debug) {
		slot = slotptr(set->page, set->page->cnt);
		fence = slot->off;
	}

	slot = slotptr(set->page, set->slotIdx);

    if( set->slotIdx < set->page->cnt )
	  if(slot->type == Btree1_librarian)
	    set->slotIdx++, slot++;

	page = set->page;
	ptr = keyaddr(page, slot->off);

	//	check for duplicate key

	if( set->slotIdx <= page->cnt )
	  if( !btree1KeyCmp(ptr, key, totLen) )
		return DB_ERROR_duplicatekey;
	
	//	if previous slot is a librarian/dead slot, use it

	if( set->slotIdx > 1 && slot[-1].dead ) {
		set->slotIdx--, slot--;
		goto fillSlot;
	}

	//	slot now points to where the new 
	//	key will be inserted

	//	find first dead/librarian slot

	for( idx = 0; idx + set->slotIdx <= page->cnt; idx++ )
	  if( slot[idx].dead )
		break;

	//	slots 0 thru idx-1 get moved up one slot

	if( idx + set->slotIdx >= page->cnt )
		page->cnt++;

	//  move these slots out of our way

	while( idx-- )
		slot[idx + 1].bits = slot[idx].bits;

	//	fill new key in new slot
}

uint32_t btree1FillSlot(DbMap *idxMap, Btree1Set *set) {
Btree1Page *page = set->page;
uint8_t *ptr = keyaddr(page, page->min);
int32_t avail = page->min - slotmax(page);

	page->act++;

	// add the key and its suffix to the page, putting suffixbytes at top

	if(suffixCnt)
	  if( avail = append64(ptr, suffix, suffixCnt, slotmin(page)) )

	if( totLen < 128 )	
		*ptr++ = totLen;
	else
		*ptr++ = totLen/256 | 0x80, *ptr++ = totLen;

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
