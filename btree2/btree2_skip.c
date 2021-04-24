#include "btree2.h"
#include "btree2_slot.h"

//   implement skip list in btree page

//  compare two keys, return > 0, = 0, or < 0
//  =0: all key fields are same
//  -1: key2 > key1
//  +1: key2 < key1

int btree2KeyCmp (uint8_t *key1, uint8_t *key2, uint32_t len1, uint32_t len2) {
int ans;

	if((ans = memcmp (key1, key2, len1 > len2 ? len2 : len1)))
		return ans;

	if( len1 > len2 )
		return 1;
	if( len1 < len2 )
		return -1;

	return 0;
}

//  find and load page at given level for given key
//	returm slot with key ,ge. given key

uint16_t btree2LoadPage(DbMap *map, Btree2Set *set, uint8_t *key, uint32_t keyLen, uint8_t lvl) {
Btree2Index *btree2 = btree2index(map);
uint16_t *tower;
uint16_t towerOff;
ObjId *pageNoPtr;
Btree2Slot *slot = NULL;
int idx, result = 0;
bool targetLvl;

  //	Starting at the page head tower go down or right after each comparison
  //	build up previous path through the towers into prevOff with either
  //	the offset or zero to indicate the towerHead slot

  set->pageNo.bits = btree2->root.bits;

  //  start at the root level of the btree2 and drill down

  do {
	pageNoPtr = fetchIdSlot (map, set->pageNo);
	set->pageAddr.bits = pageNoPtr->bits;
	set->page = getObj(map, set->pageAddr);

	targetLvl = set->page->lvl == lvl;

	if( set->page->lvl > set->rootLvl )
		set->rootLvl = set->page->lvl;
	
	//	build vector of slots that are lexically
	//	before the key and whose towers point
	//	to slots past the key.  A slot at offset zero
	//	are referring to towerHead slots for the page.

	memset (set->prevOff, 0, sizeof set->prevOff);
	tower = set->page->towerHead;
	towerOff = TowerHeadSlot;
	idx = set->page->height;

	while( idx-- )
	  do {
		set->prevOff[idx] = towerOff;

		if(	(set->next = tower[idx]) )
			slot = slotptr (set->page, set->next);	// test right
		else
			break;

		result = btree2KeyCmp (slotkey(slot), key, slot->keyLen, keyLen); 

		if( targetLvl && result == 0 )
			set->found = towerOff;

		if( result >= 0 )   // new key is .le. next key, go down
			break;

		// to find a larger candidate, go right in tower

		towerOff = tower[idx];
		tower = slot->tower;
	  } while( true );

	if( targetLvl )
		return towerOff;

	//	The key is .lt. every key in the page towerHead vector

	if( towerOff == TowerHeadSlot ) {
	  if( set->page->left.bits ) {
		Btree2Slot	*lFenceSlot = slotptr (set->page, set->page->lFence);	// test left fence
		int fResult = btree2KeyCmp (slotkey(lFenceSlot), key, lFenceSlot->keyLen, keyLen);
		if( fResult >= 0 ) {
			set->pageNo.bits = set->page->left.bits;
			continue;
		}
	  }
	}

	if( set->next == 0 ) {
	  if( set->page->stopper.bits ) {
		set->pageNo.bits = set->page->stopper.bits;
		continue;
	  }
	  if( set->page->right.bits ) {
		set->pageNo.bits = set->page->right.bits;
		continue;
	  }
	}

	//	otherwise follow slot that is .ge. the search key

	set->pageNo.bits = btree2Get64 (slot);
	
  } while( set->pageNo.bits );

  return DB_BTREE_error;
}

// find next non-dead slot -- the fence key if nothing else
/*
bool btree2SkipDead (Btree2Set *set) {
Btree2Slot *slot = slotptr(set->page, set->prev);

  while( *slot->state == Btree2_slotdeleted ) {
	set->prevOff[0] = set->prev; 
	if( (set->prev = slot->tower[0]) )	// successor offset
	  slot = slotptr(set->page, set->prev);
	else
	  return 0;
  }

  return 1;
}*/
