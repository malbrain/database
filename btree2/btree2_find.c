#include "btree2.h"
#include "btree2_slot.h"

DbStatus btree2FindKey( DbCursor *dbCursor, DbMap *map, uint8_t *key, uint32_t keyLen, bool onlyOne) {
Btree2Cursor *cursor = (Btree2Cursor *)dbCursor;
Btree2Index *btree2 = btree2index(map);
uint32_t pageSize;
uint8_t *foundKey;
Btree2Set set[1];
DbStatus stat;

	// find the level 0 page containing the key

	if ((stat = btree2LoadPage(map, set, key, keyLen, 0)))
		return stat;

	foundKey = slotkey(set->slot);
	cursor->base->state = CursorPosAt;

	if (onlyOne) {
		return DB_OK;
	}

	pageSize = 1 << (set->page->pageBits + set->page->leafXtra);
	memcpy(cursor->page, set->page, pageSize);

	cursor->listIdx = btree2FillFwd(cursor, cursor->page, set->off);
	return DB_OK;
}
