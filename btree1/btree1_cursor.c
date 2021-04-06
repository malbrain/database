// btree1_cursor.c

#include "btree1.h"

DbStatus btree1NewCursor(DbCursor *dbCursor, DbMap *idxMap) {
Btree1Cursor *cursor = (Btree1Cursor *)dbCursor;
Btree1Index *btree1 = btree1index(idxMap);

	cursor->leafSize = btree1->pageSize << btree1->leafXtra;
	cursor->slotIdx = 1;
	return DB_OK;
}

DbStatus btree1ReturnCursor(DbCursor *dbCursor, DbMap *map) {
	return DB_OK;
}

DbStatus btree1LeftKey(DbCursor *dbCursor, DbMap *map) {
Btree1Cursor *cursor = (Btree1Cursor *)dbCursor;
Btree1Index *btree1 = btree1index(map);
Btree1Page  *leftPage;
DbAddr *leftAddr;
PageId leftId;

	if (leftId.bits = cursor->page->left.bits)
		leftAddr = fetchIdSlot(map, leftId);
	else
		return DB_CURSOR_eof;

	leftPage = getObj(map, *leftAddr);
	btree1LockPage (leftPage, Btree1_lockRead);

	memcpy (cursor->page, leftPage, cursor->leafSize);
	btree1UnlockPage (leftPage, Btree1_lockRead);

	cursor->slotIdx = 1;
	return DB_OK;
}

DbStatus btree1RightKey(DbCursor *dbCursor, DbMap *map) {
Btree1Cursor *cursor = (Btree1Cursor *)dbCursor;
Btree1Index *btree1 = btree1index(map);
Btree1Page *rightPage;
DbAddr *rightAddr;
PageId rightId;

  if (rightId.bits = cursor->page->right.bits)
	  rightAddr = fetchIdSlot(map, rightId);
  else
	  return DB_CURSOR_eof;

  rightPage = getObj(map, *rightAddr);
  btree1LockPage(rightPage, Btree1_lockRead);
  memcpy(cursor->page, rightPage, cursor->leafSize);
  btree1UnlockPage(rightPage, Btree1_lockRead);
  cursor->slotIdx = cursor->page->cnt;
  return DB_OK;
}

DbStatus btree1NextKey (DbCursor *dbCursor, DbMap *map) {
Btree1Cursor *cursor = (Btree1Cursor *)dbCursor;
Btree1Page *rightPage;
DbAddr *rightAddr;
PageId rightId;
uint8_t *key;

	switch (dbCursor->state) {
	  case CursorNone:
		btree1LeftKey(dbCursor, map);
		break;

	  case CursorRightEof:
		return DB_CURSOR_eof;

	  default:
		break;
	}

	while (true) {
	  uint32_t max = cursor->page->cnt;

	  while (cursor->slotIdx <= max) {
		Btree1Slot *slot = slotptr(cursor->page, cursor->slotIdx++);

		if (slot->dead)
		  continue;

		key = keyaddr(cursor->page, slot->off);
		dbCursor->key = key;
		dbCursor->keyLen = slot->length;
		dbCursor->state = CursorPosAt;
		return DB_OK;
	  }

	  if (rightId.bits = cursor->page->right.bits)
		  rightAddr = fetchIdSlot(map, rightId);
	  else
		  break;

	  rightPage = getObj(map, *rightAddr);
      btree1LockPage(rightPage, Btree1_lockRead);
      memcpy(cursor->page, rightPage, cursor->leafSize);
      btree1UnlockPage(rightPage, Btree1_lockRead);
	  cursor->slotIdx = 1;
	}

	dbCursor->state = CursorRightEof;
	return DB_CURSOR_eof;
}

DbStatus btree1PrevKey (DbCursor *dbCursor, DbMap *map) {
Btree1Cursor *cursor = (Btree1Cursor *)dbCursor;
Btree1Page *leftPage;
DbAddr *leftAddr;
PageId leftId;
uint8_t *key;

	switch (dbCursor->state) {
	  case CursorNone:
		btree1RightKey(dbCursor, map);
		break;

	  case CursorLeftEof:
		return DB_CURSOR_eof;

	  default:
		break;
	}

	while (true) {
	  if (cursor->slotIdx ) {
		Btree1Slot *slot = slotptr(cursor->page, cursor->slotIdx--);

		if (slot->dead)
		  continue;

		key = keyaddr(cursor->page, slot->off);
		dbCursor->key = key;
		dbCursor->keyLen = slot->length;
		dbCursor->state = CursorPosAt;
		return DB_OK;
	  }

	  if (leftId.bits = cursor->page->left.bits)
	  	leftAddr = fetchIdSlot(map,leftId);
	  else
		break;

	  leftPage = getObj(map, *leftAddr);
      btree1LockPage(leftPage, Btree1_lockRead);
      memcpy(cursor->page, leftPage, cursor->leafSize);
      btree1UnlockPage(leftPage, Btree1_lockRead);
      cursor->slotIdx = cursor->page->cnt;
	}

	dbCursor->state = CursorLeftEof;
	return DB_CURSOR_eof;
}
