#include "btree2.h"
#include "btree2_slot.h"

DbStatus btree2DeleteKey(Handle *index, uint8_t *key, uint32_t keyLen) {
Btree2Index *btree2 = btree2index(index->map);
Btree2Set set[1];
DbStatus stat;

	// find the level 0 page containing the key

	if ((stat = btree2LoadPage(index->map, set, key, keyLen, 0)))
		return stat;

	if( set->found )
	  if( atomicCAS8(set->slot->state, Btree2_slotdeleted, Btree2_slotactive) )
		btree2DeadTower(set);

	return DB_OK;
}


