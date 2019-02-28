#include "btree2.h"
#include "btree2_slot.h"

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

void btree2FindSlot (Btree2Set *set, uint8_t *key, uint32_t keyLen)
{
uint8_t idx = *set->page->height;
uint16_t *tower = set->page->towerHead;
Btree2Slot *slot;
int result = 0;

	//	Starting at the head tower go down or right after each comparison

	while( idx-- ) {
		while( (set->nextSlot[idx] = tower[idx]) ) {
			slot = slotptr(set->page, tower[idx]);	// test right
			result = btree2KeyCmp (slotkey(slot), key, keyLen); 

			if( result > 0 )		
				break;

			// go right

			set->prevSlot[idx] = set->off;
			set->found = !result;
			set->off = tower[idx];
			tower = slot->tower;
			set->slot = slot;
		}
	}
}

// find next non-dead slot -- the fence key if nothing else

bool btree2SkipDead (Btree2Set *set) {

  while( *set->slot->state == Btree2_slotdeleted ) {
	set->prevSlot[0] = set->off; 
	if( (set->off = set->slot->tower[0]) )	// successor offset
	  set->slot = slotptr(set->page, set->off);
	else
	  return 0;
  }

  return 1;
}
