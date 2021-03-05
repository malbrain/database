// btree1_find.c

#include "btree1.h"

DbStatus btree1FindKey( DbCursor *dbCursor, DbMap *map, void *key, uint32_t keyLen, bool onlyOne) {
Btree1Cursor *cursor = (Btree1Cursor *)dbCursor;
Btree1Index *btree1 = btree1index(map);
uint8_t *foundKey;
Btree1Slot *slot;
Btree1Set set[1];
DbStatus stat;

	// find the level 0 page containing the key

	set->keyVal = key;
	set->keyLen = keyLen;

	if ((stat = btree1LoadPage(map, set, Btree1_lockRead, true, false, 0)))
		return stat;

	slot = slotptr(set->page, set->slotIdx);

	if (slot->type == Btree1_stopper) {
		btree1UnlockPage (set->page, Btree1_lockRead);
		return DB_CURSOR_eof;
	}

	foundKey = keyaddr(set->page, slot->off);
	cursor->base->state = CursorPosAt;

	if (onlyOne) {
		memset (cursor->page, 0, sizeof(Btree1Page));
		cursor->page->cnt = 2;
		cursor->page->act = 2;

		cursor->page->min = btree1->pageSize >> btree1->leafXtra;
		cursor->page->min -= slot->length;

		slotptr(cursor->page, 1)->bits[0] = cursor->page->min;
		slotptr(cursor->page, 2)->type = Btree1_stopper;

		memcpy (keyptr(cursor->page,1), foundKey, slot->length);

		btree1UnlockPage(set->page, Btree1_lockRead);
		cursor->base->key = keyptr(cursor->page, 1);
		cursor->base->keyLen = slot->length;
		cursor->slotIdx = 1;
		return DB_OK;
	}

	memcpy(cursor->page, set->page, btree1->pageSize);
	btree1UnlockPage(set->page, Btree1_lockRead);

	cursor->base->key = foundKey;
	cursor->base->keyLen = slot->length;
	cursor->slotIdx = set->slotIdx;
	return DB_OK;
}
