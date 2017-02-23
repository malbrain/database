#pragma once
#include "../db.h"
#include "../db_arena.h"
#include "../db_map.h"
#include "../db_api.h"
#include "../db_object.h"
#include "../db_handle.h"
#include "../db_cursor.h"

#define MAX_cursor 4096

// Artree interior nodes

enum ARTNodeType {
	UnusedSlot = 0,				// 0: slot is not yet in use
	Array4,						// 1: node contains 4 radix slots
	Array14,					// 2: node contains 14 radix slots
	Array64,					// 3: node contains 64 radix slots
	Array256,					// 4: node contains 256 radix slots
	FldEnd,						// 5: node ends a binary string field
	KeyEnd,						// 6: node ends a complete key value
	KeyUniq,					// 7: node fans out duplicate keys
	SpanNode,					// 8: node contains up to 8 key bytes
	MaxARTType = SpanNode + 17	// 8-24: node spans up to 256 bytes
};

/**
 *	field value ends for binary strings option
 */

typedef struct {
	DbAddr sameFld[1];	// more bytes from the same field
	DbAddr nextFld[1];	// end this field and start next
} ARTFldEnd;
	
/**
 * node ends the uniqueness part of the key
 */

typedef struct {
	DbAddr dups[1];		// sub-tree of duplicate keys
	DbAddr next[1];		// keys that haven't ended yet
} ARTKeyUniq;

/**
 * key is a prefix of another longer key
 */

typedef struct {
	DbAddr next[1];
} ARTKeyEnd;

/**
 * radix node with four slots and their key bytes
 */

typedef struct {
	uint8_t alloc;
	uint8_t keys[4];
	uint8_t filler[3];
	DbAddr radix[4];
} ARTNode4;

/**
 * radix node with fourteen slots and their key bytes
 */

typedef struct {
	uint16_t alloc;
	uint8_t keys[14];
	DbAddr radix[14];
} ARTNode14;

/**
 * radix node with sixty-four slots and a 256 key byte array
 */

typedef struct {
	uint64_t alloc;
	uint8_t keys[256];
	DbAddr radix[64];
} ARTNode64;

/**
 * radix node all 256 slots
 */

typedef struct {
	DbAddr radix[256];
} ARTNode256;

/**
 * span node containing up to 8 consecutive key bytes
 * span nodes are used to compress linear chains of key bytes
 */

typedef struct {
	DbAddr next[1];		// next node after span
	uint8_t bytes[8];
} ARTSpan;

/**
 * Index arena definition
 */

typedef struct {
	DbAddr root[1];		// root of the arttree
} ArtIndex;

typedef struct {
	volatile DbAddr *addr;		// tree addr of slot
	volatile DbAddr slot[1];	// slot that points to node
	uint16_t lastFld;			// previous field start
	uint16_t off;				// offset within key
	int16_t ch;					// character of key
} CursorStack;

typedef struct {
	uint32_t depth;					// current depth of cursor
	uint16_t lastFld;				// previous field start
	uint16_t fldLen;				// length remaining in current field
	uint8_t key[MAX_key];			// current cursor key
	CursorStack stack[MAX_cursor];	// cursor stack
} ArtCursor;

typedef struct {
	volatile DbAddr *slot;
	volatile DbAddr *prev;
	DbAddr oldSlot[1];
	DbAddr newSlot[1];

	DbStatus stat;
	Handle *index;
	uint8_t *key;

	uint32_t keyLen;	// length of the key
	uint32_t off;	 	// progress down the key bytes
	uint16_t fldLen;	// remaining field length
	uint8_t ch;			// current key character
	uint8_t binaryFlds;	// string fields are binary
	uint8_t restart;	// restart insert from beginning
} InsertParam;

DbStatus artNewCursor(DbCursor *cursor, DbMap *map);
DbStatus artReturnCursor(DbCursor *dbCursor, DbMap *map);

DbStatus artLeftKey(DbCursor *cursor, DbMap *map);
DbStatus artRightKey(DbCursor *cursor, DbMap *map);

DbStatus artFindKey( DbCursor *dbCursor, DbMap *map, void *key, uint32_t keyLen);
DbStatus artNextKey(DbCursor *dbCursor, DbMap *map);
DbStatus artPrevKey(DbCursor *dbCursor, DbMap *map);

DbStatus artInit(Handle *hndl, Params *params);
DbStatus artInsertKey (Handle *hndl, void *key, uint32_t keyLen);
DbStatus artDeleteKey (Handle *hndl, void *key, uint32_t keyLen);

uint64_t artAllocateNode(Handle *index, int type, uint32_t size);

bool artInsertParam (InsertParam *p);
