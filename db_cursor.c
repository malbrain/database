#include "db.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_index.h"
#include "db_map.h"
#include "db_txn.h"
#include "btree1/btree1.h"
#include "artree/artree.h"

//	position cursor

DbStatus dbFindKey(DbCursor *cursor, DbMap *map, uint8_t *key, uint32_t keyLen, bool onlyOne) {
DbStatus stat;

	switch (*map->arena->type) {
	  case ARTreeIndexType: {
		stat = artFindKey(cursor, map, key, keyLen);
		break;
	  }

	  case Btree1IndexType: {
		stat = btree1FindKey(cursor, map, key, keyLen, onlyOne);
		break;
	  }
	}

	if (stat)
		return stat;

	cursor->userLen = cursor->keyLen;

	if (cursor->noDocs)
		return DB_OK;

	if (cursor->useTxn)
		cursor->userLen = get64(cursor->key, cursor->userLen, &cursor->ver);

	cursor->userLen = get64(cursor->key, cursor->userLen, &cursor->docId.bits);
	return DB_OK;
}

//	position cursor before first key

DbStatus dbLeftKey(DbCursor *cursor, DbMap *map) {
DbStatus stat;

	switch (*map->arena->type) {
	  case ARTreeIndexType: {
		stat = artLeftKey(cursor, map);
		break;
	  }

	  case Btree1IndexType: {
		stat = btree1LeftKey(cursor, map);
		break;
	  }
	}

	if (stat)
		return stat;

	cursor->state = CursorLeftEof;
	return DB_OK;
}

//	position cursor after last key

DbStatus dbRightKey(DbCursor *cursor, DbMap *map) {
DbStatus stat;

	switch (*map->arena->type) {
	  case ARTreeIndexType: {
		stat = artRightKey(cursor, map);
		break;
	  }

	  case Btree1IndexType: {
		stat = btree1RightKey(cursor, map);
		break;
	  }
	}

	if (stat)
		return stat;

	cursor->state = CursorRightEof;
	return DB_OK;
}

//  position cursor at next doc visible under MVCC

DbStatus dbNextDoc(DbCursor *cursor, DbMap *map, uint8_t *maxKey, uint32_t maxLen) {
Txn *txn = NULL;
uint64_t *ver;
DbStatus stat;

  while (true) {
	if ((stat = dbNextKey(cursor, map, maxKey, maxLen)))
		return stat;

	if (!txn && cursor->txnId.bits)
		txn = fetchIdSlot(map->db, cursor->txnId);

	if (!(cursor->doc = findDocVer(map->parent, cursor->docId, txn)))
		continue;

	//  find version in verKeys skip list

	if ((ver = skipFind(map->parent, cursor->doc->verKeys, map->arenaDef->id)))
	  if (*ver == cursor->ver)
		return DB_OK;
  }
}

//  position cursor at prev doc visible under MVCC

DbStatus dbPrevDoc(DbCursor *cursor, DbMap *map, uint8_t *minKey, uint32_t minLen) {
Txn *txn = NULL;
uint64_t *ver;
DbStatus stat;

  while (true) {
	if ((stat = dbPrevKey(cursor, map, minKey, minLen)))
		return stat;

	if (minKey) {
		int len = cursor->userLen;

		if (len > minLen)
			len = minLen;

		if (memcmp(cursor->key, minKey, len) <= 0)
			return DB_CURSOR_eof;
	}

	if (!txn && cursor->txnId.bits)
		txn = fetchIdSlot(map->db, cursor->txnId);

	if (!(cursor->doc = findDocVer(map->parent, cursor->docId, txn)))
		continue;

	//  find version in verKeys skip list

	if ((ver = skipFind(map->parent, cursor->doc->verKeys, map->arenaDef->id)))
	  if (*ver == cursor->ver)
		return DB_OK;
  }
}

DbStatus dbNextKey(DbCursor *cursor, DbMap *map, uint8_t *maxKey, uint32_t maxLen) {
uint32_t len;
DbStatus stat;

	switch(*map->arena->type) {
	case ARTreeIndexType:
		stat = artNextKey (cursor, map);
		break;

	case Btree1IndexType:
		stat = btree1NextKey (cursor, map);
		break;

	default:
		stat = DB_ERROR_indextype;
		break;
	}

	if (stat)
		return stat;

	cursor->userLen = cursor->keyLen;

	if (!cursor->noDocs) {
	  if (cursor->useTxn)
		cursor->userLen = get64(cursor->key, cursor->userLen, &cursor->ver);

	  cursor->userLen = get64(cursor->key, cursor->userLen, &cursor->docId.bits);
	}

	if (maxKey) {
		int len = cursor->userLen;

		if (len > maxLen)
			len = maxLen;

		if (memcmp(cursor->key, maxKey, len) >= 0)
			return DB_CURSOR_eof;
	}

	return DB_OK;
}

DbStatus dbPrevKey(DbCursor *cursor, DbMap *map, uint8_t *minKey, uint32_t minLen) {
uint32_t len;
DbStatus stat;

	switch(*map->arena->type) {
	case ARTreeIndexType:
		stat = artPrevKey (cursor, map);
		break;

	case Btree1IndexType:
		stat = btree1PrevKey (cursor, map);
		break;

	default:
		stat = DB_ERROR_indextype;
		break;
	}

	if (stat)
		return stat;

	cursor->userLen = cursor->keyLen;

	if (!cursor->noDocs) {
	  if (cursor->useTxn)
		cursor->userLen = get64(cursor->key, cursor->userLen, &cursor->ver);

	  cursor->userLen = get64(cursor->key, cursor->userLen, &cursor->docId.bits);
	}

	if (minKey) {
		int len = cursor->userLen;

		if (len > minLen)
			len = minLen;

		if (memcmp(cursor->key, minKey, len) < 0)
			return DB_CURSOR_eof;
	}

	return DB_OK;
}
