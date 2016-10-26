#pragma once

#define MAX_cursor 4096

// Artree interior nodes

enum ARTNodeType {
	UnusedSlot = 0,					// 0: slot is not yet in use
	Array4,							// 1: node contains 4 radix slots
	Array14,						// 2: node contains 14 radix slots
	Array64,						// 3: node contains 64 radix slots
	Array256,						// 4: node contains 256 radix slots
	KeyEnd,							// 5: node end of a key w/o another
	KeyPass,						// 6: node key end splice into longer key
	SpanNode,						// 7: node contains up to 8 key bytes
	MaxARTType = SpanNode + 16		//23: node spans up to 256 bytes
};

/**
 * key is a prefix of another longer key
 */

typedef struct {
	DbAddr next[1];
} ARTSplice;

/**
 * radix node with four slots and their key bytes
 */

typedef struct {
	uint64_t timestamp;
	uint8_t alloc;
	uint8_t keys[4];
	uint8_t filler[3];
	DbAddr radix[4];
} ARTNode4;

/**
 * radix node with fourteen slots and their key bytes
 */

typedef struct {
	uint64_t timestamp;
	uint16_t alloc;
	uint8_t keys[14];
	DbAddr radix[14];
} ARTNode14;

/**
 * radix node with sixty-four slots and a 256 key byte array
 */

typedef struct {
	uint64_t timestamp;
	uint64_t alloc;
	uint8_t keys[256];
	DbAddr radix[64];
} ARTNode64;

/**
 * radix node all 256 slots
 */

typedef struct {
	uint64_t timestamp;
	DbAddr radix[256];
} ARTNode256;

/**
 * span node containing up to 8 consecutive key bytes
 * span nodes are used to compress linear chains of key bytes
 */

typedef struct {
	uint64_t timestamp;
	DbAddr next[1];		// next node after span
	uint8_t bytes[8];
} ARTSpan;

/**
 * Index arena definition
 */

typedef struct {
	DbIndex base[1];	// basic db index
	DbAddr root[1];		// root of the arttree
	int64_t numEntries[1];
} ArtIndex;

typedef struct {
	volatile DbAddr *addr;	// tree addr of slot
	DbAddr slot[1];			// slot that points to node
	uint16_t off;			// offset within key
	int16_t ch;				// character of key
	bool dir;
} CursorStack;

typedef struct {
	DbCursor base[1];				// common cursor header (must be first)
	uint32_t depth;					// current depth of cursor
	uint8_t key[MAX_key];			// current cursor key
	CursorStack stack[MAX_cursor];	// cursor stack
} ArtCursor;

#define artIndexAddr(map)((ArtIndex *)(map->arena + 1))

DbStatus artNewCursor(Handle *index, ArtCursor *cursor);
DbStatus artReturnCursor(Handle *index, DbCursor *dbCursor);

DbStatus artLeftKey(DbCursor *cursor, DbMap *map);
DbStatus artRightKey(DbCursor *cursor, DbMap *map);

DbStatus artFindKey( DbCursor *dbCursor, DbMap *map, uint8_t *key, uint32_t keyLen);
DbStatus artNextKey(DbCursor *dbCursor, DbMap *map);
DbStatus artPrevKey(DbCursor *dbCursor, DbMap *map);

DbStatus artInit(Handle *hndl, Params *params);
DbStatus artInsertKey (Handle *hndl, uint8_t *key, uint32_t keyLen);

uint64_t artAllocateNode(Handle *index, int type, uint32_t size);

