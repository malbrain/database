#include "db.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_index.h"
#include "db_map.h"
#include "db_txn.h"
#include "btree1/btree1.h"
#include "artree/artree.h"

//	release cursor resources

DbStatus dbCloseCursor(DbCursor *cursor, DbMap *map) {
DbStatus stat = DB_ERROR_indextype;

	switch (*map->arena->type) {
	case Hndl_artIndex:
		stat = artReturnCursor(cursor, map);
		break;

	case Hndl_btree1Index:
		stat = btree1ReturnCursor(cursor, map);
		break;
	}

	return stat;
}

//	position cursor

DbStatus dbFindKey(DbCursor *cursor, DbMap *map, char *key, uint32_t keyLen, bool onlyOne) {
DbStatus stat;

	switch (*map->arena->type) {
	  case Hndl_artIndex: {
		stat = artFindKey(cursor, map, key, keyLen);
		break;
	  }

	  case Hndl_btree1Index: {
		stat = btree1FindKey(cursor, map, (uint8_t *)key, keyLen, onlyOne);
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
	  case Hndl_artIndex: {
		stat = artLeftKey(cursor, map);
		break;
	  }

	  case Hndl_btree1Index: {
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
	  case Hndl_artIndex: {
		stat = artRightKey(cursor, map);
		break;
	  }

	  case Hndl_btree1Index: {
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

DbStatus dbNextDoc(DbCursor *cursor, DbMap *map) {
Txn *txn = NULL;
uint64_t *ver;
DbStatus stat;

  while (true) {
	if ((stat = dbNextKey(cursor, map)))
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

DbStatus dbPrevDoc(DbCursor *cursor, DbMap *map) {
Txn *txn = NULL;
uint64_t *ver;
DbStatus stat;

  while (true) {
	if ((stat = dbPrevKey(cursor, map)))
		return stat;

	if (cursor->minKeyLen) {
		int len = cursor->userLen;

		if (len > cursor->minKeyLen)
			len = cursor->minKeyLen;

		if (memcmp(cursor->key, cursor->minKey, len) <= 0)
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

DbStatus dbNextKey(DbCursor *cursor, DbMap *map) {
uint32_t len;
DbStatus stat;

	switch(*map->arena->type) {
	case Hndl_artIndex:
		stat = artNextKey (cursor, map);
		break;

	case Hndl_btree1Index:
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

	if (cursor->maxKeyLen) {
		int len = cursor->userLen;

		if (len > cursor->maxKeyLen)
			len = cursor->maxKeyLen;

		if (memcmp(cursor->key, cursor->maxKey, len) >= 0)
			return DB_CURSOR_eof;
	}

	return DB_OK;
}

DbStatus dbPrevKey(DbCursor *cursor, DbMap *map) {
uint32_t len;
DbStatus stat;

	switch(*map->arena->type) {
	case Hndl_artIndex:
		stat = artPrevKey (cursor, map);
		break;

	case Hndl_btree1Index:
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

	if (cursor->minKeyLen) {
		int len = cursor->userLen;

		if (len > cursor->minKeyLen)
			len = cursor->minKeyLen;

		if (memcmp(cursor->key, cursor->minKey, len) < 0)
			return DB_CURSOR_eof;
	}

	return DB_OK;
}
