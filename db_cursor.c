#include "btree1/btree1.h"
#include "artree/artree.h"

//	release cursor resources

DbStatus dbCloseCursor(DbCursor *dbCursor, DbMap *map) {
DbStatus stat = DB_ERROR_indextype;

	switch (*map->arena->type) {
	case Hndl_artIndex:
		stat = artReturnCursor(dbCursor, map);
		break;

	case Hndl_btree1Index:
		stat = btree1ReturnCursor(dbCursor, map);
		break;
	}

	return stat;
}

//	position cursor

DbStatus dbFindKey(DbCursor *dbCursor, DbMap *map, void *key, uint32_t keyLen, CursorOp op) {
DbStatus stat;

	switch (*map->arena->type) {
	  case Hndl_artIndex: {
		if ((stat = artFindKey(dbCursor, map, key, keyLen, 0)))
			return stat;

		if (op == OpBefore) {
			if (dbCursor->state == CursorPosBefore)
			  return DB_OK;
			else
			  return artPrevKey(dbCursor, map);
		}

		return artNextKey(dbCursor, map);
	  }

	  case Hndl_btree1Index: {
		if ((stat = btree1FindKey(dbCursor, map, key, keyLen, op == OpOne)))
			return stat;

		if (op == OpAfter) {
		  if (memcmp (dbCursor->key, key, keyLen) <= 0)
			return btree1NextKey (dbCursor, map);
		  else
			return DB_OK;
		}

		if (op == OpBefore) {
		  if (memcmp (dbCursor->key, key, keyLen) >= 0)
			return btree1PrevKey (dbCursor, map);
		  else
			return DB_OK;
		}

		break;
	  }
	}

	return DB_OK;
}

//	position cursor before first key

DbStatus dbLeftKey(DbCursor *dbCursor, DbMap *map) {
DbStatus stat = DB_OK;

	switch (*map->arena->type) {
	  case Hndl_artIndex: {
		stat = artLeftKey(dbCursor, map);
		break;
	  }

	  case Hndl_btree1Index: {
		stat = btree1LeftKey(dbCursor, map);
		break;
	  }
	}

	dbCursor->state = CursorLeftEof;
	return stat;
}

//	position cursor after last key

DbStatus dbRightKey(DbCursor *dbCursor, DbMap *map) {
DbStatus stat = DB_OK;

	switch (*map->arena->type) {
	  case Hndl_artIndex: {
		stat = artRightKey(dbCursor, map);
		break;
	  }

	  case Hndl_btree1Index: {
		stat = btree1RightKey(dbCursor, map);
		break;
	  }
	}

	if (stat)
		return stat;

	dbCursor->state = CursorRightEof;
	return DB_OK;
}

DbStatus dbNextKey(DbCursor *dbCursor, DbMap *map) {
DbStatus stat;

	switch(*map->arena->type) {
	case Hndl_artIndex:
		stat = artNextKey (dbCursor, map);
		break;

	case Hndl_btree1Index:
		stat = btree1NextKey (dbCursor, map);
		break;

	default:
		stat = DB_ERROR_indextype;
		break;
	}

	return stat;
}

DbStatus dbPrevKey(DbCursor *dbCursor, DbMap *map) {
DbStatus stat;

	switch(*map->arena->type) {
	case Hndl_artIndex:
		stat = artPrevKey (dbCursor, map);
		break;

	case Hndl_btree1Index:
		stat = btree1PrevKey (dbCursor, map);
		break;

	default:
		stat = DB_ERROR_indextype;
		break;
	}

	return stat;
}
