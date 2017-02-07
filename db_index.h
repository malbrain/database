#pragma once

//	Index data structure after DbArena object

typedef struct {
	uint64_t numEntries[1];	// number of keys in index
	DbAddr keys;
} DbIndex;

// database cursor handle extension to index

typedef enum {
	CursorNone,
	CursorLeftEof,
	CursorRightEof,
	CursorPosBefore,	// cursor is before a key
	CursorPosAt			// cursor is at a key
} PosState;

typedef struct {
	uint64_t ts;		// cursor timestamp
	uint64_t version;	// cursor version #
	ObjId txnId;		// cursor transaction
	void *key;			// cursor key bytes
	uint32_t keyLen;	// cursor key length
	PosState state:8;	// cursor position state enum
	uint8_t foundKey;	// cursor position found the key
} DbCursor;

// database docStore handle extension to collection

typedef struct {
	SkipHead indexes[1];	// index DbHandles by docStore childId
	SkipHead txnVers[1];	// pending doc versions by ObjId
	uint64_t childId;		// highest child idx installed
	uint32_t idxCnt;		// number of indexes
} DocStore;

#define dbindex(map) ((DbIndex *)(map->arena + 1))

uint64_t dbAllocDocStore(Handle *docHndl, uint32_t amt, bool zeroit);
DbStatus dbCloseCursor(DbCursor *cursor, DbMap *map);
DbStatus dbInstallIndexes(Handle *docHndl);

DbStatus dbInsertKey (Handle *idxHndl, void *keyBytes, uint32_t keyLen);

DbStatus dbFindKey(DbCursor *cursor, DbMap *map, void *key, uint32_t keyLen, CursorOp op);
DbStatus dbNextKey(DbCursor *cursor, DbMap *map);
DbStatus dbPrevKey(DbCursor *cursor, DbMap *map);
DbStatus dbRightKey(DbCursor *cursor, DbMap *map);
DbStatus dbLeftKey(DbCursor *cursor, DbMap *map);
DbStatus dbCloseCursor(DbCursor *cursor, DbMap *map);
