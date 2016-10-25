#include "db.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_index.h"
#include "db_map.h"
#include "db_txn.h"
#include "btree1/btree1.h"
#include "artree/artree.h"

//	position cursor

Status dbFindKey(DbCursor *cursor, DbMap *map, uint8_t *key, uint32_t keyLen, bool onlyOne) {
Status stat;

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
		return OK;

	if (cursor->useTxn)
		cursor->userLen = get64(cursor->key, cursor->userLen, &cursor->ver);

	cursor->userLen = get64(cursor->key, cursor->userLen, &cursor->docId.bits);
	return OK;
}

//	position cursor before first key

Status dbLeftKey(DbCursor *cursor, DbMap *map) {
Status stat;

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
	return OK;
}

//	position cursor after last key

Status dbRightKey(DbCursor *cursor, DbMap *map) {
Status stat;

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
	return OK;
}

//  position cursor at next doc visible under MVCC

Status dbNextDoc(DbCursor *cursor, DbMap *map, uint8_t *maxKey, uint32_t maxLen) {
Txn *txn = NULL;
uint64_t *ver;
Status stat;

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
		return OK;
  }
}

//  position cursor at prev doc visible under MVCC

Status dbPrevDoc(DbCursor *cursor, DbMap *map, uint8_t *minKey, uint32_t minLen) {
Txn *txn = NULL;
uint64_t *ver;
Status stat;

  while (true) {
	if ((stat = dbPrevKey(cursor, map, minKey, minLen)))
		return stat;

	if (minKey) {
		int len = cursor->userLen;

		if (len > minLen)
			len = minLen;

		if (memcmp(cursor->key, minKey, len) <= 0)
			return CURSOR_eof;
	}

	if (!txn && cursor->txnId.bits)
		txn = fetchIdSlot(map->db, cursor->txnId);

	if (!(cursor->doc = findDocVer(map->parent, cursor->docId, txn)))
		continue;

	//  find version in verKeys skip list

	if ((ver = skipFind(map->parent, cursor->doc->verKeys, map->arenaDef->id)))
	  if (*ver == cursor->ver)
		return OK;
  }
}

Status dbNextKey(DbCursor *cursor, DbMap *map, uint8_t *maxKey, uint32_t maxLen) {
uint32_t len;
Status stat;

	switch(*map->arena->type) {
	case ARTreeIndexType:
		stat = artNextKey (cursor, map);
		break;

	case Btree1IndexType:
		stat = btree1NextKey (cursor, map);
		break;

	default:
		stat = ERROR_indextype;
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
			return CURSOR_eof;
	}

	return OK;
}

Status dbPrevKey(DbCursor *cursor, DbMap *map, uint8_t *minKey, uint32_t minLen) {
uint32_t len;
Status stat;

	switch(*map->arena->type) {
	case ARTreeIndexType:
		stat = artPrevKey (cursor, map);
		break;

	case Btree1IndexType:
		stat = btree1PrevKey (cursor, map);
		break;

	default:
		stat = ERROR_indextype;
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
			return CURSOR_eof;
	}

	return OK;
}
