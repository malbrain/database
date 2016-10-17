#pragma once

#define MAX_key		4096	// maximum key size in bytes

//	Index data structure after DbArena object

typedef struct {
	uint64_t numEntries[1];	// number of keys in index
	DbAddr keySpec;			// key construction document
} DbIndex;

// database index cursor

typedef struct {
	uint64_t ver;			// cursor doc version
    uint64_t ts;            // cursor timestamp
    ObjId txnId;            // cursor transaction
    ObjId docId;            // current doc ID
    Document *doc;          // current document
    DbHandle idx[1];        // index handle
	uint32_t keyLen;
	uint8_t *key;
	bool foundKey;			// cursor position found the key
} DbCursor;

typedef struct {
	RWLock2 lock[1];		// index list r/w lock
	SkipHead indexes[1];	// index handles by Id
	uint64_t childId;		// highest child installed
	uint32_t idxCnt;		// number of indexes
} DocStore;

#define dbindex(map) ((DbIndex *)(map->arena + 1))

Status storeDoc(Handle *docHndl, void *obj, uint32_t objSize, ObjId *result, ObjId txnId);
Status installIndexes(Handle *docHndl);

Status dbPositionCursor(DbMap *index, DbCursor *cursor, uint8_t *key, uint32_t keyLen);
Status dbNextKey(DbMap *index, DbCursor *cursor, uint8_t *maxKey, uint32_t maxLen);
Status dbPrevKey(DbMap *index, DbCursor *cursor, uint8_t *maxKey, uint32_t maxLen);
Status dbNextDoc(DbMap *index, DbCursor *cursor, uint8_t *maxKey, uint32_t maxLen);
Status dbPrevDoc(DbMap *index, DbCursor *cursor, uint8_t *maxKey, uint32_t maxLen);
