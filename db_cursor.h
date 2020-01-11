#pragma once

// database cursor handle extension to index

typedef enum {
	CursorNone,
	CursorLeftEof,
	CursorRightEof,
	CursorPosBefore,	// cursor is before a key
	CursorPosAt			// cursor is at a key
} PosState;

//	DbCursor handle extension

typedef struct {
	uint8_t *key;		// cursor key bytes
	uint32_t size;		// size of user data
	uint32_t keyLen;	// cursor key length
	uint32_t baseLen;   // cursor base length
	uint32_t suffixLen; // cursor suffix length
	PosState state;		// cursor position state enum
	uint8_t foundKey;	// cursor position found the key
	char binaryFlds;	// index keys have fields
	uint8_t deDup;		// cursor will deDuplicate result set
} DbCursor;

DbStatus dbCloseCursor(DbCursor *cursor, DbMap *map);
DbStatus dbFindKey(DbCursor *cursor, DbMap *map, void *key, uint32_t keyLen, CursorOp op);
DbStatus dbNextKey(DbCursor *cursor, DbMap *map);
DbStatus dbPrevKey(DbCursor *cursor, DbMap *map);
DbStatus dbRightKey(DbCursor *cursor, DbMap *map);
DbStatus dbLeftKey(DbCursor *cursor, DbMap *map);
