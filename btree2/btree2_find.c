#include "btree2.h"
#include "btree2_slot.h"

//	move cursor to first key >= given key

DbStatus btree2FindKey( DbCursor *dbCursor, DbMap *map, uint8_t *key, uint32_t keyLen, bool onlyOne) {
Btree2Cursor *cursor = (Btree2Cursor *)dbCursor;
uint32_t pageSize;
uint8_t *foundKey;
uint16_t next;
Btree2Set set[1];
Btree2Slot *slot;

	// find the level 0 page containing the key

    memset (set, 0, sizeof(set));

	if( (next = btree2LoadPage(map, set, key, keyLen, 0)))
		slot = slotptr (set->page, next);
	else
		return DB_ERROR_keynotfound;

	foundKey = slotkey(slot);
	cursor->base->state = CursorPosAt;

	if (onlyOne) {
		return DB_OK;
	}

	pageSize = 1 << (set->page->pageBits + set->page->leafXtra);

	if( cursor->pageSize < pageSize )
		return DB_ERROR_cursoroverflow;

	cursor->listIdx = btree2FillFwd(cursor, set->page, set->off, pageSize);
	return DB_OK;
}
