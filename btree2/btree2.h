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
#define Btree2_maxslots		65536	// max skip entries
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
	uint32_t skipUnits;	// unit size for skip list entries
	ObjId root;
	ObjId left;
	ObjId right;
} Btree2Index;

//	Btree2 page layout

//	Page key slot definition.

//	Slot types

typedef enum {
	unused,		// slot unused
	active,		// slot active
	moved,		// slot copied into new page version
	deleted,	// slot deleted
} SlotState;

typedef struct {
	ObjId value;
	union {
		SlotState slotstate : 8;
		uint8_t state[1];
	};
	uint8_t height;			// tower height
	uint16_t skipTower[0];	// skip list tower
} Btree2Slot;

#define slotptr(page, off) (Btree2Slot *)((uint8_t *)page + (off << page->skipUnits)) 
#define keyaddr(page, slot) ((uint8_t *)(slot + 1) + slot->height * sizeof(uint16_t))
#define keylen(key) ((key[0] & 0x80) ? ((key[0] & 0x7f) << 8 | key[1]) : key[0])
#define keystr(key) ((key[0] & 0x80) ? (key + 2) : (key + 1))
#define keypre(key) ((key[0] & 0x80) ? 2 : 1)

typedef enum {
	empty,
	live,
	locked
} PageState;

//	This structure is immediately
//	followed by the key slots

typedef struct {
	union {
		struct {
			uint16_t nxt;	// next skip list storage unit
			PageState state : 16;
		};
		uint32_t alloc;
	};
	uint16_t garbage;	// page garbage in skip units
	uint16_t size, cnt;	// page size in skip units, count of active keys
	uint8_t lvl:4;		// level of page
	uint8_t height:4;	// height of skip list
	uint8_t skipBits;	// bits for skip list units
	uint8_t dead:1;		// page is replaced
	uint8_t kill:1;		// page is being split
	ObjId pageNo[1];		// left page of replacement split
	ObjId split[1];		// left page of replacement split
	ObjId right[1];		// page to right
	ObjId left[1];		// page to left
	uint16_t skipHead[Btree2_maxskip];
} Btree2Page;

typedef struct {
	ObjId pageNo;		// current page Number
	Btree2Page *page;	// current page address
	uint32_t slotUnit;	// slot on page
	uint16_t skipTower[Btree2_maxskip];	// skip list descent indicies
} Btree2Set;

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

DbStatus btree2Init(Handle *hndl, Params *params);
DbStatus btree2InsertKey(Handle *hndl, void *key, uint32_t keyLen, uint8_t lvl, SlotState state);
DbStatus btree2DeleteKey(Handle *hndl, void *key, uint32_t keyLen);

DbStatus btree2LoadPage(DbMap *map, Btree2Set *set, void *key, uint32_t keyLen, uint8_t lvl);

uint64_t btree2NewPage (Handle *hndl, uint8_t lvl);

DbStatus btree2CleanPage(Handle *hndl, Btree2Set *set, uint32_t totKeyLen);
DbStatus btree2SplitPage (Handle *hndl, Btree2Set *set);
DbStatus btree2FixKey (Handle *hndl, uint8_t *fenceKey, uint8_t lvl, bool stopper);

void btree2PutPageNo(uint8_t *key, uint32_t len, uint64_t bits);
uint64_t btree2GetPageNo(uint8_t *key, uint32_t len);

Btree2Slot *btree2InstallKey(Btree2Page *newRoot, void *key, uint32_t keyLen, ObjId pageNo);
