#pragma once
#include "../db.h"
#include "../db_arena.h"
#include "../db_map.h"
#include "../db_api.h"
#include "../db_object.h"
#include "../db_handle.h"
#include "../db_cursor.h"
#include "../db_frame.h"
#include <stddef.h>

#define MAX_cursor 4096

// Artree interior nodes

enum ARTNodeType {
	UnusedSlot = 0,				// 0: slot is not yet in use
	Array4,						// 1: node contains 4 radix slots
	Array14,					// 2: node contains 14 radix slots
	Array64,					// 3: node contains 64 radix slots
	Array256,					// 4: node contains 256 radix slots
	FldEnd,						// 5: node ends a binary string field
	KeyEnd,						// 6: node ends the end of a key value
	SuffixEnd,					// 7: node ends a suffix string
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
 * key is a prefix of another longer key
 */

typedef struct {
	DbAddr next[1];
	DbAddr suffix[1];   // end key and continue w/ set of unique suffix strings
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
  DbIndex base[1];
  DbAddr root[1];  // root of the arttree
} ArtIndex;

typedef struct {
	volatile DbAddr *addr;	// tree addr of slot
	DbAddr slot[1];			// slot that points to node
	uint16_t off;			// offset within key
	uint16_t lastFld;		// offset of current field
	int16_t ch;				// character of key
	uint16_t startFld;		// flag to start field
} CursorStack;

typedef struct {
	DbCursor base[1];
	uint16_t depth;					// current depth of cursor
	uint16_t fldLen;	// length remaining in current field
	char binaryFlds;	// keys have binary fields
	char inSuffix;      //  binaryFlds in suffix string
	uint8_t key[MAX_key];			// current cursor key
	CursorStack stack[MAX_cursor];	// cursor stack
} ArtCursor;

typedef struct {
	volatile DbAddr *slot;
	volatile DbAddr *prev;
	DbAddr oldSlot[1];
	DbAddr newSlot[1];

	DbMap *idxMap;
	DbStatus stat;
	Handle *index;
	uint8_t *key;

	uint16_t keyLen;	// length of the key
	uint16_t off;	 	// progress down the key bytes
	uint16_t lastFld;	// previous field start
	uint16_t fldLen;	// length remaining in current field
	uint8_t ch;			// current key character
	char binaryFlds;	// keys have binary fields
	uint8_t restart;	// restart insert from beginning
} InsertParam;

#define artindex(map) ((ArtIndex *)(map->arena + 1))

DbStatus artNewCursor(DbCursor *cursor, DbMap *map);
DbStatus artReturnCursor(DbCursor *dbCursor, DbMap *map);

DbStatus artLeftKey(DbCursor *cursor, DbMap *map);
DbStatus artRightKey(DbCursor *cursor, DbMap *map);

DbStatus artFindKey( DbCursor *dbCursor, DbMap *map, uint8_t *key, uint16_t keyLen, uint16_t suffixLen);
DbStatus artNextKey(DbCursor *dbCursor, DbMap *map);
DbStatus artPrevKey(DbCursor *dbCursor, DbMap *map);

DbStatus artInit(Handle *hndl, Params *params);
DbStatus artDeleteKey (Handle *hndl, uint8_t *key, uint16_t keyLen, uint16_t suffixLen);
DbStatus artInsertKey (Handle *hndl, uint8_t *key, uint16_t keyLen, uint16_t suffixLen);
DbStatus artInsertUniq (Handle *hndl, uint8_t *key, uint16_t keyLen, uint16_t suffixLen, UniqCbFcn *fcn, bool *defer);
DbStatus artEvalUniq( DbMap *map, uint8_t *key, uint16_t keyLen, uint16_t suffixLen, UniqCbFcn *evalFcn);

uint64_t artAllocateNode(Handle *index, int type, uint32_t size);

bool artInsertParam (InsertParam *p);
