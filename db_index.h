#pragma once

#define MAX_key		4096	// maximum key size in bytes

//	Index data structure after DbArena object

typedef struct {
	uint64_t numEntries[1];	// number of keys in index
	char noDocs;			// no document ID's on keys
} DbIndex;

// database index cursor

typedef enum {
	CursorNone,
	CursorLeftEof,
	CursorRightEof,
	CursorPosAt
} PosState;

typedef struct {
	uint64_t ver;			// cursor doc version
    uint64_t ts;            // cursor timestamp
    ObjId txnId;            // cursor transaction
    ObjId docId;            // current doc ID
	uint8_t *key;
    Document *doc;          // current document
    DbHandle idx[1];        // index handle
	uint32_t keyLen;		// raw key length
	uint32_t userLen;		// user's key length
	PosState state:8;		// cursor position state enum
	char foundKey;			// cursor position found the key
	char useTxn;			// txn being used
	char noDocs;			// no document ID's on keys
} DbCursor;

typedef struct {
	RWLock lock[1];		// index list r/w lock
	SkipHead indexes[1];	// index handles by Id
	uint64_t childId;		// highest child installed
	uint32_t idxCnt;		// number of indexes
} DocStore;

#define dbindex(map) ((DbIndex *)(map->arena + 1))

DbStatus storeDoc(Handle *docHndl, void *obj, uint32_t objSize, ObjId *result, ObjId txnId);
DbStatus installIndexes(Handle *docHndl);

DbStatus dbFindKey(DbCursor *cursor, DbMap *map, uint8_t *key, uint32_t keyLen, bool onlyOne);
DbStatus dbNextKey(DbCursor *cursor, DbMap *map, uint8_t *maxKey, uint32_t maxLen);
DbStatus dbPrevKey(DbCursor *cursor, DbMap *map, uint8_t *minKey, uint32_t minLen);

DbStatus dbNextDoc(DbCursor *cursor, DbMap *map, uint8_t *maxKey, uint32_t maxLen);
DbStatus dbPrevDoc(DbCursor *cursor, DbMap *map, uint8_t *maxKey, uint32_t maxLen);
DbStatus dbRightKey(DbCursor *cursor, DbMap *map);
DbStatus dbLeftKey(DbCursor *cursor, DbMap *map);
