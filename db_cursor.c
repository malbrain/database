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

DbStatus dbFindKey(DbCursor *cursor, DbMap *map, void *key, uint32_t keyLen, CursorOp op) {
DbStatus stat;

	switch (*map->arena->type) {
	  case Hndl_artIndex: {
		if ((stat = artFindKey(cursor, map, key, keyLen)))
			return stat;

		if (op == OpBefore) {
			if (cursor->state == CursorPosBefore)
			  return DB_OK;
			else
			  return artPrevKey(cursor, map);
		}

		return artNextKey(cursor, map);
	  }

	  case Hndl_btree1Index: {
		if ((stat = btree1FindKey(cursor, map, key, keyLen, op == OpOne)))
			return stat;

		if (op == OpAfter) {
		  if (memcmp (cursor->key, key, keyLen) <= 0)
			return btree1NextKey (cursor, map);
		  else
			return DB_OK;
		}

		if (op == OpBefore) {
		  if (memcmp (cursor->key, key, keyLen) >= 0)
			return btree1PrevKey (cursor, map);
		  else
			return DB_OK;
		}

		break;
	  }
	}

	return DB_OK;
}

//	position cursor before first key

DbStatus dbLeftKey(DbCursor *cursor, DbMap *map) {
DbStatus stat = DB_OK;

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

	cursor->state = CursorLeftEof;
	return stat;
}

//	position cursor after last key

DbStatus dbRightKey(DbCursor *cursor, DbMap *map) {
DbStatus stat = DB_OK;

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

DbStatus dbNextKey(DbCursor *cursor, DbMap *map) {
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

	return stat;
}

DbStatus dbPrevKey(DbCursor *cursor, DbMap *map) {
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

	return stat;
}
