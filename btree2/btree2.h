#pragma once
#include "../db.h"
#include "../db_object.h"
#include "../db_handle.h"
#include "../db_arena.h"
#include "../db_map.h"
#include "../db_api.h"
#include "../db_cursor.h"
#include "../db_frame.h"
#include "../db_lock.h"

#define Btree2_maxkey		4096	// max key size
#define Btree2_maxskip		16	// height of skip list
#define Btree2_numslots		65536	// max skip entries
#define Btree2_maxbits		29	// maximum page size in bits
#define Btree2_minbits		9	// minimum page size in bits
#define Btree2_minpage		(1 << Btree2_minbits)	// minimum page size
#define Btree2_maxpage		(1 << Btree2_maxbits)	// maximum page size

//	types of btree pages/allocations

typedef enum{
	Btree2_rootPage = 3,
	Btree2_interior,
	Btree2_leafPage,
	MAXBtree2Type
} Btree2PageType;

//	Btree2Index global data on disk

typedef struct {
	DbIndex base[1];
	DbAddr freePage[MAXBtree2Type];
	DbAddr pageNos[1];
	uint32_t pageSize;
	uint32_t pageBits;
	uint32_t leafXtra;
	ObjId root;
	ObjId left;
	ObjId right;
} Btree2Index;

//	Btree2 page layout

//	This structure is immediately
//	followed by the key slots

typedef struct {
	uint32_t min;		// next key storage offset
	uint32_t garbage;	// page garbage in bytes
	uint16_t act, cnt;	// count of key slots in page, count of active keys
	uint8_t lvl:6;		// level of page
	uint8_t height[4];	// height of skip list
	uint8_t dead:1;		// page is replaced
	uint8_t kill:1;		// page is being split
	ObjId split[1];		// left page of replacement split
	ObjId right[1];		// page to right
	ObjId left[1];		// page to left
	uint16_t skipHead[Btree2_maxskip];	// skip list head indicies
} Btree2Page;

typedef struct {
	DbAddr pageNo;		// current page addr
	Btree2Page *page;	// current page address
	uint32_t slotIdx;	// slot on page
} Btree2Set;

//	Page key slot definition.

//	Keys are marked dead, but remain on the page until
//	it cleanup is called.

//	Slot types

//	In addition to the Unique keys that occupy slots
//	there are Librarian slots in the key slot array.

typedef enum {
	Btree2_indexed,		// key was indexed
	Btree2_deleted,		// key was deleted
} Btree2SlotType;

typedef struct {
	uint32_t off:Btree2_maxbits;	// page offset for key start
	uint32_t type:2;		// type of key slot
	uint32_t dead:1;		// unused slot
	uint16_t skipnext[Btree2_maxskip];	// skip list right indicies
} Btree2Slot;

typedef struct {
	DbCursor base[1];	// base object
	Btree2Page *page;	// cursor position page buffer
	DbAddr pageAddr;	// cursor page buffer address
	uint32_t slotIdx;	// cursor position index
} Btree2Cursor;

#define btree2index(map) ((Btree2Index *)(map->arena + 1))

DbStatus btree2NewCursor(DbCursor *cursor, DbMap *map);
DbStatus btree2ReturnCursor(DbCursor *dbCursor, DbMap *map);

DbStatus btree2LeftKey(DbCursor *cursor, DbMap *map);
DbStatus btree2RightKey(DbCursor *cursor, DbMap *map);

DbStatus btree2FindKey(DbCursor *dbCursor, DbMap *map, void *key, uint32_t keylen, bool onlyOne);
DbStatus btree2NextKey (DbCursor *cursor, DbMap *map);
DbStatus btree2PrevKey (DbCursor *cursor, DbMap *map);

#define slotptr(page, slot) (((Btree2Slot *)(page+1)) + (((int)slot)-1))

#define keyaddr(page, off) ((uint8_t *)((uint8_t *)(page) + off))
#define keyptr(page, slot) ((uint8_t *)((uint8_t *)(page) + slotptr(page, slot)->off))
#define keylen(key) ((key[0] & 0x80) ? ((key[0] & 0x7f) << 8 | key[1]) : key[0])
#define keystr(key) ((key[0] & 0x80) ? (key + 2) : (key + 1))
#define keypre(key) ((key[0] & 0x80) ? 2 : 1)

DbStatus btree2Init(Handle *hndl, Params *params);
DbStatus btree2InsertKey(Handle *hndl, void *key, uint32_t keyLen, uint8_t lvl, Btree2SlotType type);
DbStatus btree2DeleteKey(Handle *hndl, void *key, uint32_t keyLen);

DbStatus btree2LoadPage(DbMap *map, Btree2Set *set, void *key, uint32_t keyLen, uint8_t lvl, bool stopper);

uint64_t btree2NewPage (Handle *hndl, uint8_t lvl);

DbStatus btree2CleanPage(Handle *hndl, Btree2Set *set, uint32_t totKeyLen);
DbStatus btree2SplitPage (Handle *hndl, Btree2Set *set);
DbStatus btree2FixKey (Handle *hndl, uint8_t *fenceKey, uint8_t lvl, bool stopper);

void btree2PutPageNo(uint8_t *key, uint32_t len, uint64_t bits);
uint64_t btree2GetPageNo(uint8_t *key, uint32_t len);
