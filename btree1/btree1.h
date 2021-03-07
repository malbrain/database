#pragma once
#include "../base64.h"
#include "../db.h"
#include "../db_object.h"
#include "../db_handle.h"
#include "../db_arena.h"
#include "../db_map.h"
#include "../db_api.h"
#include "../db_index.h"
#include "../db_cursor.h"
#include "../db_frame.h"
#include "../rwlock/readerwriter.h"

//	BTree configuration and options

// #define *+ (2 + 7 + 1)
#define Btree1_maxbits		29					// maximum page size in bits
#define Btree1_minbits		9					// minimum page size in bits
#define Btree1_minpage		(1 << Btree1_minbits)	// minimum page size
#define Btree1_maxpage		(1 << Btree1_maxbits)	// maximum page size
#define Btree1_keylenbits	(15)
#define Btree1_maxkey		(1 << Btree1_keybits)	// maximum key length

//	There are four lock types for each node in three independent sets: 
//	1. (set 1) ReadLock: Sharable. Read the node. Incompatible with WriteLock. 
//	2. (set 1) WriteLock: Exclusive. Modify the node. Incompatible with ReadLock and other WriteLocks. 
//	3. (set 2) ParentModification: Exclusive. Change the node's parent keys. Incompatible with another ParentModification. 
//	4. (set 3) LinkModification: Exclusive. Update of a node's left link is underway. Incompatible with another LinkModification. 

typedef enum {
	Btree1_lockRead   = 1,
	Btree1_lockWrite  = 2,
	Btree1_lockParent = 4,
	Btree1_lockLink   = 8
} Btree1Lock;

typedef ObjId PageId;

//	types of btree pages/allocations

typedef enum{
	Btree1_rootPage = 3,
	Btree1_interior,
	Btree1_leafPage,
	MAXBtree1Type
} Btree1PageType;

//	page address

//	Btree1Index global data on disk after Arena

typedef struct {
	DbIndex dbIndex[1];
	uint32_t pageSize;
	uint32_t pageBits;
	uint32_t leafXtra;
	uint32_t librarianDensity;// 2 == every other key
	PageId root;
	PageId left;					// leftmost page level 0
	PageId right;					//	rightmost page lvl 0
} Btree1Index;

//	Btree page      layout

typedef struct {
	RWLock readwr[1];	// read/write access lock
	RWLock parent[1];	// posting of fence key
	RWLock link[1];	// left link update
} LatchSet;

//	The page structure is immediately
//	followed by an array of the key slots
//	and key strings on this page, allocated top-down

typedef struct {
	LatchSet latch[1];	// latches for this page
	uint32_t cnt;		// count of keys in page
	uint32_t act;		// count of active keys
	uint32_t min;		// next page key end offset
	uint32_t garbage;	// page garbage in bytes
	Btree1PageType type:4;
	uint8_t lvl:4;		// level of page in btree
	uint8_t free:1;		// page is unused on free chain
	uint8_t kill:1;		// page is being deleted
	PageId right;		// page to right
	PageId left;		// page to left
	PageId self;		// current page no
} Btree1Page;

typedef struct {
	uint8_t *keyVal;
	uint32_t keyLen;
	int64_t *suffix;
	uint32_t suffixCnt;
	Btree1Page *page;	// current page Addr
	uint32_t slotIdx;	// slot on page
} Btree1Set;

//	Page key slot definition.

//	Keys are marked dead, but remain on the page until
//	it cleanup is called.

//	Slot types

//	In addition to the Unique keys that occupy slots
//	there are Librarian slots in the key slot array.

//	The Librarian slots are dead keys that
//	serve as filler, available to add new keys.

typedef enum {
	Btree1_indexed,		// key was indexed
	Btree1_deleted,		// key was deleted
	Btree1_librarian,	// librarian slot
	Btree1_fenceKey,	// fence key for page
	Btree1_stopper		// stopper slot
} Btree1SlotType;

typedef union {
  uint64_t bits[2];

  struct {
	uint32_t off : 29;	// key bytes offset
	uint32_t type : 2;	// type of key slot
	uint32_t dead : 1;	// dead/librarian slot
	uint32_t length;	// key length
  };
  union {
	  PageId childId;		// page Id of next level to leaf
	  ObjId payLoad;
  };
} Btree1Slot;

typedef struct {
  DbCursor base[1];	  // base object
  uint32_t leafSize;
  uint32_t slotIdx;   // cursor position index
  Btree1Page page[];  // cursor position page buffer
} Btree1Cursor;

//	access macros

#define slotptr(page, slot) (((Btree1Slot *)(page+1)) + (((int)slot)-1))

#define btree1index(map) ((Btree1Index *)(map->arena + 1))

#define keyaddr(page, off) ((uint8_t *)((uint8_t *)(page) + off))
#define keyptr(page, slot) ((uint8_t *)((uint8_t *)(page) + slotptr(page, slot)->off))

//	btree1 implementation

DbStatus btree1NewCursor(DbCursor *cursor, DbMap *map);
DbStatus btree1ReturnCursor(DbCursor *dbCursor, DbMap *map);

DbStatus btree1LeftKey(DbCursor *cursor, DbMap *map);
DbStatus btree1RightKey(DbCursor *cursor, DbMap *map);

DbStatus btree1FindKey(DbCursor *dbCursor, DbMap *map, void *key, uint32_t keylen, bool onlyOne);
DbStatus btree1NextKey (DbCursor *cursor, DbMap *map);
DbStatus btree1PrevKey (DbCursor *cursor, DbMap *map);

DbStatus btree1StoreSlot (Handle *hndl, uint8_t *key, uint32_t keyLen, int64_t *values, uint32_t valueCnt);
DbStatus btree1Init(Handle *hndl, Params *params);
DbStatus btree1InsertKey(Handle *hndl, uint8_t *key, uint32_t keyLen, uint32_t sfxLen, uint8_t lvl, Btree1SlotType type);
DbStatus btree1DeleteKey(Handle *hndl, void *key, uint32_t keyLen);

DbStatus btree1LoadPage(DbMap *map, Btree1Set *set, Btree1Lock lockMode);

DbStatus btree1CleanPage(Handle *hndl, Btree1Set *set);
DbStatus btree1SplitPage (Handle *hndl, Btree1Set *set);
DbStatus btree1FixKey (Handle *index, uint8_t *fenceKey, uint64_t prev, uint64_t suffix, uint8_t lvl, bool stopper);
DbStatus btree1InsertSfxKey(Handle *hndl, uint8_t *key, uint32_t keyLen, uint64_t suffix, uint8_t lvl, Btree1SlotType type);

Btree1Page *btree1NewPage(Handle *index, uint8_t lvl, Btree1PageType type);
void btree1LockPage(Btree1Page *page, Btree1Lock mode);
void btree1UnlockPage(Btree1Page *page, Btree1Lock mode);
int btree1KeyCmp (uint8_t *key1, uint8_t *key2, uint32_t len2);
