#pragma once

//	Index data structure after DbArena object

typedef struct {
	uint64_t numEntries[1];	// number of keys in index
	uint8_t noDocs;			// no document ID's on keys
} DbIndex;

// database index cursor

typedef enum {
	CursorNone,
	CursorLeftEof,
	CursorRightEof,
	CursorPosAt
} PosState;

typedef struct {
	uint64_t ver;		// cursor doc version
	uint64_t ts;		// cursor timestamp
	ObjId txnId;		// cursor transaction
	ObjId docId;		// current doc ID
	void *key;
	Doc *doc;			// current document
	uint32_t keyLen;	// raw key length
	uint32_t userLen;	// user's key length
	void *minKey;	// minimum key value
	void *maxKey;	// maximum key value
	uint32_t minKeyLen;
	uint32_t maxKeyLen;
	PosState state:8;	// cursor position state enum
	uint8_t foundKey;		// cursor position found the key
	uint8_t useTxn;		// txn being used
	uint8_t noDocs;		// no document ID's on keys
} DbCursor;

typedef struct {
	SkipHead indexes[1];	// index handles by Id
	uint64_t childId;		// highest child installed
	uint32_t idxCnt;		// number of indexes
} DocStore;

#define dbindex(map) ((DbIndex *)(map->arena + 1))

DbStatus installIndexes(Handle *docHndl);
DbStatus installIndexKeys(Handle *docHndl, Doc *doc);

DbStatus dbFindKey(DbCursor *cursor, DbMap *map, void *key, uint32_t keyLen, bool onlyOne);
DbStatus dbNextKey(DbCursor *cursor, DbMap *map);
DbStatus dbPrevKey(DbCursor *cursor, DbMap *map);

DbStatus dbNextDoc(DbCursor *cursor, DbMap *map);
DbStatus dbPrevDoc(DbCursor *cursor, DbMap *map);
DbStatus dbRightKey(DbCursor *cursor, DbMap *map);
DbStatus dbLeftKey(DbCursor *cursor, DbMap *map);
DbStatus dbCloseCursor(DbCursor *cursor, DbMap *map);
