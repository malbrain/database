#pragma once
#include "../base64.h"
#include "../db.h"
#include "../db_object.h"
#include "../db_handle.h"
#include "../db_arena.h"
#include "../db_map.h"
#include "../db_api.h"
#include "../db_cursor.h"
#include "../db_frame.h"
#include "../rwlock/readerwriter.h"

#define Btree2_pagenobytes  (2 + 7 + 1)
#define Btree2_maxkey		4096	// max key size
#define Btree2_maxtower		16		// max height of skip tower
#define Btree2_maxslots		65536	// max skip entries
#define Btree2_maxbits		29	// maximum page size in bits
#define Btree2_minbits		9	// minimum page size in bits
#define Btree2_minpage		(1 << Btree2_minbits)	// minimum page size
#define Btree2_maxpage		(1 << Btree2_maxbits)	// maximum page size

//	tower slot status values

typedef enum {
	TowerSlotEmpty = 0,
	TowerHeadSlot,
	TowerSlotOff
} Btree2TowerSlot;

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

//	Btree2Index global data on disk after Arena

typedef struct {
	DbIndex dbIndex[1];
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
	Btree2_pageactive= 1,	// page is live
	Btree2_pageclean = 2,	// page being redone or split
	Btree2_pageleft  = 4,	// page is leftmost
	Btree2_pageright = 8,	// page is rightmost
} Btree2PageState;

//	This structure is immediately
//	followed by the key slots

typedef struct {
	volatile union Btree2Alloc {
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
	uint16_t lFence, rFence;// fence slot offsets in skip units
	uint16_t garbage[1];// page garbage in skip units
	uint8_t attributes;	// page attributes
	uint8_t height; 	// height of skip list
	uint8_t lvl;		// level of page
	uint8_t pageBits;
	uint8_t leafXtra;
	uint8_t skipBits;	// unit size for skip list allocations
	uint8_t pageType;	// allocation type
	DbAddr newPage;		// replacement page
	ObjId stopper;      // page down chain of right-most pages
	ObjId pageNo;		// page number
	ObjId right;		// page number to right
	ObjId left;			// page numberto left
	uint8_t bitLatch[Btree2_maxtower / 8];
	uint16_t towerHead[Btree2_maxtower];
} Btree2Page;

//	Slot types

typedef enum {
	Btree2_slotunused,	// slot unused
	Btree2_slotactive,	// slot active
	Btree2_slotmoved,	// slot copied into new page version
	Btree2_slotdeleted,	// slot deleted
} Btree2SlotState;

//	Page key slot definition.
//	length of key (one or two bytes), 
//	and key bytes follow these fields

typedef volatile struct {
	uint8_t height;		// final tower height 
	uint8_t state[1];
	uint8_t bitLatch[Btree2_maxtower / 8];
	uint16_t volatile tower[];	// skip list tower
} Btree2Slot;

typedef struct {
	uint8_t rootLvl;    // last discovered root  level
	ObjId pageNo;		// current page Number
	DbAddr pageAddr;	// current page address
	Btree2Page *page;	// current page content
	uint16_t found, off, next;  // offset of new and next slot
	uint16_t prevOff[Btree2_maxtower];
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

typedef struct {
  uint32_t lcgState[1];    // Lehmer's RNG state
  uint16_t nrandState[3];  // random number generator state
} Btree2HandleXtra;

#define btree2index(map) ((Btree2Index *)(map->arena + 1))
#define btree2HandleXtra(handle) ((Btree2HandleXtra *)(handle + 1))

DbStatus btree2NewCursor(DbCursor *cursor, DbMap *map);
DbStatus btree2ReturnCursor(DbCursor *dbCursor, DbMap *map);

DbStatus btree2LeftKey(DbCursor *cursor, DbMap *map);
DbStatus btree2RightKey(DbCursor *cursor, DbMap *map);

DbStatus btree2FindKey(DbCursor *cursor, DbMap *map, uint8_t *key, uint32_t keylen, bool onlyOne);
DbStatus btree2NextKey (DbCursor *cursor, DbMap *map);
DbStatus btree2PrevKey (DbCursor *cursor, DbMap *map);

DbStatus btree2Init(Handle *hndl, Params *params);
DbStatus btree2InsertKey(Handle *hndl, DbKeyBase *kv, uint8_t lvl, Btree2SlotState state);
DbStatus btree2DeleteKey(Handle *hndl, uint8_t *key, uint32_t keyLen);

uint16_t btree2LoadPage(DbMap *map, Btree2Set *set, uint8_t *key, uint32_t keyLen, uint8_t lvl);
uint64_t btree2NewPage (Handle *hndl, uint8_t lvl);

DbStatus btree2CleanPage(Handle *hndl, Btree2Set *set);
DbStatus btree2SplitPage (Handle *hndl, Btree2Set *set);
DbStatus btree2InstallKey(Btree2Set *set, uint8_t *key, uint32_t keyLen, uint8_t height);

int btree2KeyCmp(uint8_t *key1, uint8_t *key2, uint32_t len2);
void btree2FindSlot(Btree2Set *set, uint8_t *key, uint32_t keyLen);
uint64_t btree2AllocPageNo(Handle *index);
uint64_t btree2Get64 (Btree2Slot *slot);
uint32_t btree2Store64(Btree2Slot *slot, uint64_t value);
uint16_t btree2AllocSlot(Btree2Page *page, uint32_t bytes);
uint16_t btree2FillFwd(Btree2Cursor *cursor, Btree2Page *page, uint16_t findOff, uint32_t pageSize);
uint32_t btree2SlotSize(Btree2Slot *slot, uint8_t skipBits, uint8_t height);
uint32_t btree2SizeSlot(uint32_t keyLen, uint8_t height);
uint32_t btree2GenHeight(Handle *index);
bool btree2RecyclePage(Handle *index, int type, DbAddr page);
bool btree2RecyclePageNo(Handle *ind6x, ObjId pageNo);
bool btree2SkipDead(Btree2Set *set);
bool btree2DeadTower(Btree2Set *set);
