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

	if((ans = memcmp (keystr(key1), key2, len1 > len2 ? len2 : len1)))
		return ans;

	if( len1 > len2 )
		return 1;
	if( len1 < len2 )
		return -1;

	return 0;
}

//  find prev slot in page for given key

void btree2FindSlot (Btree2Set *set, uint8_t *key, uint32_t keyLen)
{
uint16_t *tower = set->page->towerHead;
uint8_t idx = *set->page->height;
Btree2Slot *slot;
int result = 0;

	//	Starting at the page head tower go down or right after each comparison

	while( idx-- ) {
		while( (set->next = tower[idx]) ) {
			set->nextSlot[idx] = set->next;
			slot = slotptr(set->page, set->next);	// test right
			result = btree2KeyCmp (slotkey(slot), key, keyLen); 

			if( result > 0 )		
				break;

			// go right

			set->prevSlot[idx] = set->next;
			set->found = !result;
			set->prev = set->next;

			tower = slot->tower;
		}
	}
}

// find next non-dead slot -- the fence key if nothing else

bool btree2SkipDead (Btree2Set *set) {
Btree2Slot *slot = slotptr(set->page, set->next);

  while( *slot->state == Btree2_slotdeleted ) {
	set->prevSlot[0] = set->next; 
	if( (set->next = slot->tower[0]) )	// successor offset
	  slot = slotptr(set->page, set->next);
	else
	  return 0;
  }

  return 1;
}
