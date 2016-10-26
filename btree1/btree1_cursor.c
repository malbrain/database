#include "../db.h"
#include "../db_object.h"
#include "../db_arena.h"
#include "../db_index.h"
#include "../db_frame.h"
#include "../db_map.h"
#include "btree1.h"

DbStatus btree1NewCursor(Handle *index, Btree1Cursor *cursor) {
Btree1Index *btree1 = btree1index(index->map);
Btree1Page *first;

	cursor->pageAddr.bits = btree1NewPage(index, 0);
	cursor->page = getObj(index->map, cursor->pageAddr);
	cursor->slotIdx = 0;
	return DB_OK;
}

DbStatus btree1ReturnCursor(Handle *index, DbCursor *dbCursor) {
Btree1Cursor *cursor = (Btree1Cursor *)dbCursor;

	// return cursor page buffer

	addSlotToFrame(index->map, index->list[cursor->pageAddr.type].free, NULL, cursor->pageAddr.bits);
	return DB_OK;
}

DbStatus btree1LeftKey(DbCursor *dbCursor, DbMap *map) {
Btree1Cursor *cursor = (Btree1Cursor *)dbCursor;
Btree1Index *btree1 = btree1index(map);
Btree1Page *left;

	left = getObj(map, btree1->left);
	btree1LockPage (left, Btree1_lockRead);

	memcpy (cursor->page, left, btree1->pageSize);
	btree1UnlockPage (left, Btree1_lockRead);

	cursor->slotIdx = 0;
	return DB_OK;
}

DbStatus btree1RightKey(DbCursor *dbCursor, DbMap *map) {
Btree1Cursor *cursor = (Btree1Cursor *)dbCursor;
Btree1Index *btree1 = btree1index(map);
Btree1Page *right;

	right = getObj(map, btree1->right);
	btree1LockPage (right, Btree1_lockRead);

	memcpy (cursor->page, right, btree1->pageSize);
	btree1UnlockPage (right, Btree1_lockRead);

	cursor->slotIdx = cursor->page->cnt;
	return DB_OK;
}

DbStatus btree1NextKey (DbCursor *dbCursor, DbMap *map) {
Btree1Cursor *cursor = (Btree1Cursor *)dbCursor;
uint8_t *key;

	switch (cursor->base->state) {
	  case CursorNone:
		btree1LeftKey(dbCursor, map);
		break;

	  case CursorRightEof:
		return DB_CURSOR_eof;
	}

	while (true) {
	  uint32_t max = cursor->page->cnt;

	  if (!cursor->page->right.bits)
		max--;

	  while (++cursor->slotIdx <= max) {
		Btree1Slot *slot = slotptr(cursor->page, cursor->slotIdx);

		if (slot->dead)
		  continue;

		key = keyaddr(cursor->page, slot->off);
		cursor->base->key = key + keypre(key);
		cursor->base->keyLen = keylen(key);
		cursor->base->state = CursorPosAt;
		return DB_OK;
	  }

	  if (cursor->page->right.bits)
		cursor->page = getObj(map, cursor->page->right);
	  else
		break;

	  cursor->slotIdx = 0;
	}

	cursor->base->state = CursorRightEof;
	return DB_CURSOR_eof;
}

DbStatus btree1PrevKey (DbCursor *dbCursor, DbMap *map) {
Btree1Cursor *cursor = (Btree1Cursor *)dbCursor;
uint8_t *key;

	switch (cursor->base->state) {
	  case CursorNone:
		btree1RightKey(dbCursor, map);
		break;

	  case CursorLeftEof:
		return DB_CURSOR_eof;
	}

	while (true) {
	  if (cursor->slotIdx > 1) {
		Btree1Slot *slot = slotptr(cursor->page, --cursor->slotIdx);

		if (slot->dead)
		  continue;

		key = keyaddr(cursor->page, slot->off);
		cursor->base->key = key + keypre(key);
		cursor->base->keyLen = keylen(key);
		cursor->base->state = CursorPosAt;
		return DB_OK;
	  }

	  if (cursor->page->left.bits)
		cursor->page = getObj(map, cursor->page->left);
	  else
		break;

	  cursor->slotIdx = cursor->page->cnt + 1;
	}

	cursor->base->state = CursorLeftEof;
	return DB_CURSOR_eof;
}
