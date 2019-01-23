#include "btree2.h"
#include "btree2_slot.h"

uint16_t btree2FillFwd(Btree2Cursor *cursor, Btree2Page *page, uint16_t findOff) {
uint32_t pageSize = 1 << (page->pageBits + page->leafXtra);
uint16_t off = page->skipHead[0], foundIdx = 0;
Btree2Slot *slot;

	memcpy (cursor->page, page, pageSize);
	cursor->listMax = 0;

	while( off ) {
		slot = slotptr(page, off);

		if( slot->slotState == Btree2_slotactive)
			cursor->listFwd[++cursor->listMax] = off;

		if( off == findOff )
			foundIdx = cursor->listMax;

		off = slot->tower[0];
	}

	return foundIdx;
}

DbStatus btree2NewCursor(DbCursor *dbCursor, DbMap *map) {
Btree2Cursor *cursor = (Btree2Cursor *)((char *)dbCursor + dbCursor->xtra);
Btree2Index *btree2 = btree2index(map);
uint32_t size;

	//	allocate cursor page buffer

	size = btree2->pageSize << btree2->leafXtra;

	cursor->pageAddr.bits = db_rawAlloc(size, false);
	cursor->page = db_memObj(cursor->pageAddr.bits);
	cursor->listIdx = 0;
	return DB_OK;
}

DbStatus btree2ReturnCursor(DbCursor *dbCursor, DbMap *map) {
Btree2Cursor *cursor = (Btree2Cursor *)((char *)dbCursor + dbCursor->xtra);

	// return cursor page buffer

	db_memFree(cursor->pageAddr.bits);
	return DB_OK;
}

DbStatus btree2LeftKey(DbCursor *dbCursor, DbMap *map) {
Btree2Cursor *cursor = (Btree2Cursor *)((char *)dbCursor + dbCursor->xtra);
Btree2Index *btree2 = btree2index(map);
DbAddr *pageNoPtr;
Btree2Page *left;

	pageNoPtr = fetchIdSlot (map, btree2->left);
	left = getObj(map, *pageNoPtr);

	btree2FillFwd(cursor, left, 0);
	cursor->listIdx = 0;
	return DB_OK;
}

DbStatus btree2RightKey(DbCursor *dbCursor, DbMap *map) {
Btree2Cursor *cursor = (Btree2Cursor *)((char *)dbCursor + dbCursor->xtra);
Btree2Index *btree2 = btree2index(map);
DbAddr *pageNoPtr;
Btree2Page *right;

	pageNoPtr = fetchIdSlot (map, btree2->right);
	right = getObj(map, *pageNoPtr);

	btree2FillFwd(cursor, right, 0);
	cursor->listIdx = cursor->listMax;
	return DB_OK;
}

DbStatus btree2NextKey (DbCursor *dbCursor, DbMap *map) {
Btree2Cursor *cursor = (Btree2Cursor *)((char *)dbCursor + dbCursor->xtra);
DbAddr *pageNoPtr;
Btree2Page *right;
Btree2Slot *slot;
uint16_t off;
uint8_t *key;

	switch (dbCursor->state) {
	  case CursorNone:
		btree2LeftKey(dbCursor, map);
		break;

	  case CursorRightEof:
		return DB_CURSOR_eof;

	  default:
		break;
	}

	while (true) {
	  while( cursor->listIdx < cursor->listMax ) {
		off = cursor->listFwd[++cursor->listIdx];
		slot = slotptr(cursor->page, off);

		if( slot->slotState == Btree2_slotactive )
			key = slotkey(slot);
		else
			continue;

		dbCursor->key = key + keypre(key);
		dbCursor->keyLen = keylen(key);
		dbCursor->state = CursorPosAt;
		return DB_OK;
	  }

	  if (cursor->page->right.bits) {
		pageNoPtr = fetchIdSlot (map, cursor->page->right);
		right = getObj(map, *pageNoPtr);
	  } else
		break;

	  btree2FillFwd(cursor, right, 0);
	  cursor->listIdx = cursor->listMax;
	}

	dbCursor->state = CursorRightEof;
	return DB_CURSOR_eof;
}

DbStatus btree2PrevKey (DbCursor *dbCursor, DbMap *map) {
Btree2Cursor *cursor = (Btree2Cursor *)((char *)dbCursor + dbCursor->xtra);
DbAddr *pageNoPtr;
Btree2Page *left;
Btree2Slot *slot;
uint16_t off;
uint8_t *key;

	switch (dbCursor->state) {
	  case CursorNone:
		btree2RightKey(dbCursor, map);
		break;

	  case CursorLeftEof:
		return DB_CURSOR_eof;

	  default:
		break;
	}

	while (true) {
	  if (cursor->listIdx > 1) {
		off = cursor->listFwd[--cursor->listIdx];
		slot = slotptr(cursor->page, off);

		if( slot->slotState == Btree2_slotactive )
		  key = slotkey(slot);
		else
		  continue;

		dbCursor->key = key + keypre(key);
		dbCursor->keyLen = keylen(key);
		dbCursor->state = CursorPosAt;
		return DB_OK;
	  }

	  if (cursor->page->left.bits) {
		pageNoPtr = fetchIdSlot (map, cursor->page->left);
		left = getObj(map, *pageNoPtr);
	  } else
		break;

	  btree2FillFwd(cursor, left, 0);
	  cursor->listIdx = cursor->listMax;
	}

	dbCursor->state = CursorLeftEof;
	return DB_CURSOR_eof;
}