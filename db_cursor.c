#include "db.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_index.h"
#include "db_map.h"
#include "db_txn.h"
#include "btree1/btree1.h"
#include "artree/artree.h"

//	position cursor

Status dbPositionCursor(DbMap *index, DbCursor *cursor, uint8_t *key, uint32_t keyLen) {

	switch (*index->arena->type) {
	  case ARTreeIndexType: {
		cursor->foundKey = artFindKey(cursor, index, key, keyLen);
		break;
	  }

	  case Btree1IndexType: {
		cursor->foundKey = btree1FindKey(cursor, index, key, keyLen);
		break;
	  }
	}

	return OK;
}

Status dbNextDoc(DbMap *index, DbCursor *cursor, uint8_t *maxKey, uint32_t maxLen) {
ArrayEntry *array;
Txn *txn = NULL;
uint64_t *ver;
Status stat;

	while (true) {
	  if ((stat = dbNextKey(index, cursor, maxKey, maxLen)))
		break;

	  if (index->arenaDef->useTxn)
	  	cursor->keyLen = get64(cursor->key, cursor->keyLen, &cursor->ver);

	  cursor->keyLen = get64(cursor->key, cursor->keyLen, &cursor->docId.bits);

	  if (!txn && cursor->txnId.bits)
		txn = fetchIdSlot(index->db, cursor->txnId);

	  if (!(cursor->doc = findDocVer(index->parent, cursor->docId, txn)))
		continue;

	  array = getObj(index->parent, *cursor->doc->verKeys);

	  if ((ver = arrayFind(array, cursor->doc->verKeys->nslot, index->arenaDef->id)))
		if (*ver == cursor->ver)
		  break;
	}

	return stat;
}

Status dbPrevDoc(DbMap *index, DbCursor *cursor, uint8_t *maxKey, uint32_t maxLen) {
ArrayEntry *array;
Txn *txn = NULL;
uint64_t *ver;
Status stat;

	while (true) {
	  if ((stat = dbPrevKey(index, cursor, maxKey, maxLen)))
		break;

	  if (index->arenaDef->useTxn)
	  	cursor->keyLen = get64(cursor->key, cursor->keyLen, &cursor->ver);

	  cursor->keyLen = get64(cursor->key, cursor->keyLen, &cursor->docId.bits);

	  if (!txn && cursor->txnId.bits)
		txn = fetchIdSlot(index->db, cursor->txnId);

	  if (!(cursor->doc = findDocVer(index->parent, cursor->docId, txn)))
		continue;

	  array = getObj(index->parent, *cursor->doc->verKeys);

	  if ((ver = arrayFind(array, cursor->doc->verKeys->nslot, index->arenaDef->id)))
		if (*ver == cursor->ver)
		  break;
	}

	return stat;
}

Status dbNextKey(DbMap *index, DbCursor *cursor, uint8_t *maxKey, uint32_t maxLen) {
uint32_t len;
Status stat;

	switch(*index->arena->type) {
	case ARTreeIndexType:
		stat = artNextKey (cursor, index);
		break;

	case Btree1IndexType:
		stat = btree1NextKey (cursor, index);
		break;

	default:
		stat = ERROR_indextype;
		break;
	}

	if (stat)
		return stat;

	if (maxKey) {
		len = cursor->keyLen;

		if (len > maxLen)
			len = maxLen;

		if (memcmp (cursor->key, maxKey, len) >= 0)
			stat = ERROR_endoffile;
	}

	return stat;
}

Status dbPrevKey(DbMap *index, DbCursor *cursor, uint8_t *maxKey, uint32_t maxLen) {
uint32_t len;
Status stat;

	switch(*index->arena->type) {
	case ARTreeIndexType:
		stat = artPrevKey (cursor, index);
		break;

	case Btree1IndexType:
		stat = btree1PrevKey (cursor, index);
		break;

	default:
		stat = ERROR_indextype;
		break;
	}

	if (stat)
		return stat;

	if (maxKey) {
		len = cursor->keyLen;

		if (len > maxLen)
			len = maxLen;

		if (memcmp (cursor->key, maxKey, len) <= 0)
			stat = ERROR_endoffile;
	}

	return stat;
}
