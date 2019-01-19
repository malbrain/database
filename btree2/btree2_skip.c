#include "btree2.h"

//   implement skip list in btree page

//  compare two keys, return > 0, = 0, or < 0
//  =0: all key fields are same
//  -1: key2 > key1
//  +1: key2 < key1

int btree2KeyCmp (uint8_t *key1, uint8_t *key2, uint32_t len2)
{
uint32_t len1 = keylen(key1);
int ans;

	key1 += keypre(key1);

	if((ans = memcmp (key1, key2, len1 > len2 ? len2 : len1)))
		return ans;

	if( len1 > len2 )
		return 1;
	if( len1 < len2 )
		return -1;

	return 0;
}

//  find slot in page for given key
//	return true for exact match
//	return false otherwise

//	lazy build of node towers and
//	removal of dead nodes

bool btree2FindSlot (Btree2Set *set, uint8_t *key, uint32_t keyLen)
{
uint16_t height = set->page->height, off;
uint16_t *tower = set->page->skipHead;
Btree2Slot *slot = NULL;
int result = 0;

	//	Starting at the head tower go down or right after each comparison

	while( height-- ) {
	  off = 0;			// left skipHead tower

	  do {
		set->prevSlot[height] = off;

		if( (off = tower[height]) )
			slot = slotptr(set->page, off);
		else
			break;

		if( slot->height > height )
		  if( *slot->lazyFill == height )
			btree2FillTower (set, slot, off);

		result = btree2KeyCmp (slotkey(slot), key, keyLen); 

		// go right at same height if key > slot

		if( result < 0 )
			tower = slot->tower;

	  } while( result < 0 );
	}

	return !result;
}

// find next non-dead slot -- the fence key if nothing else

bool btree2SkipDead (Btree2Set *set) {

  while( set->slot->slotState == Btree2_slotdeleted ) {
	set->prevSlot[0] = set->off; 
	if( set->off = set->slot->tower[0] )	// successor offset
	  set->slot = slotptr(set->page, set->off);
	else
	  return 0;
  }

  return 1;
}
