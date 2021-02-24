#include "btree2.h"
#include "btree2_slot.h"

bool btree2DeadTower(Btree2Set *set) {
	return true;
}

DbStatus btree2DeleteKey(Handle *index, uint8_t *key, uint32_t keyLen) {
DbMap *idxMap = MapAddr(index);
Btree2Index *btree2 = btree2index(idxMap);
Btree2Slot *slot;
Btree2Set set[1];
uint16_t next;

	memset(set, 0, sizeof(set));

	// find the level 0 page containing the key

	if ((next = btree2LoadPage(idxMap, set, key, keyLen, 0)))
		slot = slotptr (set->page, next);
	else
		return DB_ERROR_deletekey;

	if( set->found )
	  if( atomicCAS8(slot->state, Btree2_slotactive, Btree2_slotdeleted) )
		btree2DeadTower(set);

	return DB_OK;
}


