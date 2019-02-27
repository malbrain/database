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
#define Btree2_maxskip		16		// max height of skip tower
#define Btree2_maxslots		65536	// max skip entries
#define Btree2_maxbits		29	// maximum page size in bits
#define Btree2_minbits		9	// minimum page size in bits
#define Btree2_minpage		(1 << Btree2_minbits)	// minimum page size
#define Btree2_maxpage		(1 << Btree2_maxbits)	// maximum page size

//	types of btree pages/allocations

typedef enum {
	Btree2_pageNo   = 1,
	Btree2_leafPage = 2,
	Btree2_interior = 3,
	MAXBtree2Type,
} Btree2PageType;

//	page Attributes

typedef enum {
	Btree2_rootPage = 0x10,
} Btree2PageAttribute;

//	Btree2Index global data on disk

typedef struct {
	DbIndex base[1];
	uint32_t pageSize;
	uint8_t pageBits;
	uint8_t leafXtra;
	uint8_t skipBits;	// unit size for skip list entries
	ObjId root;
	ObjId left;
	ObjId right;
} Btree2Index;

//	Btree2 page layout

typedef enum {
	Btree2_pageempty = 0,
	Btree2_pageactive,	// page is live
	Btree2_pageclean,	// page being cleaned
	Btree2_pagesplit,	// page being split
} Btree2PageState;

//	This structure is immediately
//	followed by the key slots

typedef struct {
	union Btree2Alloc {
		struct {
			uint8_t state;
			uint8_t filler;
			uint16_t nxt;	// next skip list storage unit
		};
        Btree2PageState disp:8;
        uint8_t bytes[4];
		uint32_t word[1];
	} alloc[1];
	uint32_t size;		// page size
	uint16_t garbage[1];	// page garbage in skip units
	uint16_t fence;		// fence slot offset in skip units
	uint8_t attributes;	// page attributes
	uint8_t height[1];	// height of skip list
	uint8_t lvl;		// level of page
	uint8_t pageBits;
	uint8_t leafXtra;
	uint8_t skipBits;	// unit size for skip list allocations
	uint8_t pageType;	// allocation type
	DbAddr newPage;		// replacement page
	ObjId stopper;		// go here when right page is zero
	ObjId pageNo;		// page number
	ObjId right;		// page to right
	ObjId left;			// page to left
	uint8_t bitLatch[Btree2_maxskip / 8];
	uint16_t towerHead[Btree2_maxskip];
} Btree2Page;

//	Slot types

typedef enum {
	Btree2_slotunused,	// slot unused
	Btree2_slotactive,	// slot active
	Btree2_slotmoved,	// slot copied into new page version
	Btree2_slotdeleted,	// slot deleted
} Btree2SlotState;

//	Page key slot definition.

typedef struct {
	uint8_t height;		// final tower height 
	uint8_t state[1];
	uint8_t bitLatch[Btree2_maxskip / 8];
	uint16_t tower[];	// skip list tower
} Btree2Slot;

typedef struct {
	uint8_t rootLvl;
	uint8_t height;		// tower height
	bool found;			// key found as prefix of entry
	ObjId pageNo;		// current page Number
	DbAddr pageAddr;	// current page address
	Btree2Page *page;	// current page
	Btree2Slot *slot;	// height zero slot
	uint16_t off;		// offset of current slot
	uint16_t prevSlot[Btree2_maxskip];
	uint16_t nextSlot[Btree2_maxskip];
} Btree2Set;

typedef struct {
	DbCursor base[1];	// base object
	Btree2Page *page;	// cursor position page buffer
	DbAddr pageAddr;	// current page address
	uint32_t pageSize;	// size of cursor page buffer
	uint16_t listIdx;	// cursor position idx
	uint16_t listMax;	// cursor position max
	uint16_t listFwd[Btree2_maxslots];
} Btree2Cursor;

#define btree2index(map) ((Btree2Index *)(map + 1))

DbStatus btree2NewCursor(DbCursor *cursor, DbMap *map);
DbStatus btree2ReturnCursor(DbCursor *dbCursor, DbMap *map);

DbStatus btree2LeftKey(DbCursor *cursor, DbMap *map);
DbStatus btree2RightKey(DbCursor *cursor, DbMap *map);

DbStatus btree2FindKey(DbCursor *cursor, DbMap *map, uint8_t *key, uint32_t keylen, bool onlyOne);
DbStatus btree2NextKey (DbCursor *cursor, DbMap *map);
DbStatus btree2PrevKey (DbCursor *cursor, DbMap *map);

DbStatus btree2Init(Handle *hndl, Params *params);
DbStatus btree2InsertKey(Handle *hndl, uint8_t *key, uint32_t keyLen, uint64_t suffixValue, uint8_t lvl, Btree2SlotState state);
DbStatus btree2DeleteKey(Handle *hndl, uint8_t *key, uint32_t keyLen);
DbStatus btree2LoadPage(DbMap *map, Btree2Set *set, uint8_t *key, uint32_t keyLen, uint8_t lvl);

uint64_t btree2NewPage (Handle *hndl, uint8_t lvl);

DbStatus btree2CleanPage(Handle *hndl, Btree2Set *set);
DbStatus btree2SplitPage (Handle *hndl, Btree2Set *set);
DbStatus btree2FixKey (Handle *hndl, uint8_t *fenceKey, uint8_t lvl);
DbStatus btree2InstallKey(Handle *index, Btree2Set *set, uint16_t off, uint8_t *key, uint32_t keyLen, uint8_t height);

int btree2KeyCmp(uint8_t *key1, uint8_t *key2, uint32_t len2);
void btree2FindSlot(Btree2Set *set, uint8_t *key, uint32_t keyLen);
uint64_t btree2AllocPageNo(Handle *index);

uint16_t btree2AllocSlot(Btree2Page *page, uint16_t size);
uint16_t btree2FillFwd(Btree2Cursor *cursor, Btree2Page *page, uint16_t findOff, uint32_t pageSize);
uint16_t btree2SizeSlot(uint8_t , uint32_t totKeySize, uint8_t height);
uint16_t btree2InstallSlot(Handle *index, Btree2Page *page, Btree2Slot *slot, uint16_t *fwd);
uint16_t btree2SlotSize(Btree2Slot *slot, uint8_t skipBits, uint8_t height);
uint32_t btree2GenHeight(Handle *index);
bool btree2RecyclePage(Handle *index, int type, DbAddr page);
bool btree2RecyclePageNo(Handle *ind6x, ObjId pageNo);
bool btree2SkipDead(Btree2Set *set);
bool btree2DeadTower(Btree2Set *set);
