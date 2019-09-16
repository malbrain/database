#pragma once
#include "../db.h"
#include "../db_object.h"
#include "../db_handle.h"
#include "../db_arena.h"
#include "../db_map.h"
#include "../db_api.h"
#include "../db_cursor.h"
#include "../db_frame.h"
#include "../rwlock/readerwriter.h"

#define Btree1_pagenobytes  (1 + 7 + 1)
#define Btree1_maxkey		4096
#define Btree1_maxbits		29					// maximum page size in bits
#define Btree1_minbits		9					// minimum page size in bits
#define Btree1_minpage		(1 << Btree1_minbits)	// minimum page size
#define Btree1_maxpage		(1 << Btree1_maxbits)	// maximum page size

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

//	types of btree pages/allocations

typedef enum{
	Btree1_rootPage = 3,
	Btree1_interior,
	Btree1_leafPage,
	MAXBtree1Type
} Btree1PageType;

//	Btree1Index global data on disk

typedef struct {
	DbIndex base[1];
	uint32_t pageSize;
	uint32_t pageBits;
	uint32_t leafXtra;
	DbAddr root;
	DbAddr left;
	DbAddr right;
} Btree1Index;

//	Btree page layout

//	This structure is immediately
//	followed by the key slots

typedef struct {
	RWLock readwr[1];	// read/write access lock
	RWLock parent[1];	// posting of fence key
	RWLock link[1];	// left link update
} LatchSet;

typedef struct {
	LatchSet latch[1];	// page latches
	uint32_t cnt;		// count of keys in page
	uint32_t act;		// count of active keys
	uint32_t min;		// next page key offset
	uint32_t garbage;	// page garbage in bytes
	uint8_t lvl:6;		// level of page
	uint8_t free:1;		// page is on free chain
	uint8_t kill:1;		// page is being deleted
	DbAddr right;		// page to right
	DbAddr left;		// page to left
} Btree1Page;

typedef struct {
	DbAddr pageNo;		// current page addr
	Btree1Page *page;	// current page
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
	Btree1_stopper		// stopper slot
} Btree1SlotType;

typedef union {
	struct {
		uint32_t off:Btree1_maxbits;	// page offset for key start
		uint32_t type:2;			// type of key slot
		uint32_t dead:1;			// dead/librarian slot
	};
	uint32_t bits;
} Btree1Slot;

typedef struct {
	DbCursor base[1];	// base object
	Btree1Page *page;	// cursor position page buffer
	DbAddr addr, pageAddr;	// cursor page buffer address
	uint32_t slotIdx;	// cursor position index
} Btree1Cursor;

#define btree1index(map) ((Btree1Index *)(map->arena + 1))

DbStatus btree1NewCursor(DbCursor *cursor, DbMap *map);
DbStatus btree1ReturnCursor(DbCursor *dbCursor, DbMap *map);

DbStatus btree1LeftKey(DbCursor *cursor, DbMap *map);
DbStatus btree1RightKey(DbCursor *cursor, DbMap *map);

DbStatus btree1FindKey(DbCursor *dbCursor, DbMap *map, void *key, uint32_t keylen, bool onlyOne);
DbStatus btree1NextKey (DbCursor *cursor, DbMap *map);
DbStatus btree1PrevKey (DbCursor *cursor, DbMap *map);

#define slotptr(page, slot) (((Btree1Slot *)(page+1)) + (((int)slot)-1))

#define keyaddr(page, off) ((uint8_t *)((uint8_t *)(page) + off))
#define keyptr(page, slot) ((uint8_t *)((uint8_t *)(page) + slotptr(page, slot)->off))
#define keylen(key) ((key[0] & 0x80) ? ((key[0] & 0x7f) << 8 | key[1]) : key[0])
#define keystr(key) ((key[0] & 0x80) ? (key + 2) : (key + 1))
#define keypre(key) ((key[0] & 0x80) ? 2 : 1)

DbStatus btree1Init(Handle *hndl, Params *params);
DbStatus btree1InsertKey(Handle *hndl, uint8_t *key, uint32_t keyLen, uint32_t sfxLen, uint8_t lvl, Btree1SlotType type);
DbStatus btree1DeleteKey(Handle *hndl, void *key, uint32_t keyLen);

DbStatus btree1LoadPage(DbMap *map, Btree1Set *set, void *key, uint32_t keyLen, uint8_t lvl, bool goodOnly, bool stopper, Btree1Lock lock);

uint64_t btree1NewPage (Handle *hndl, uint8_t lvl);

DbStatus btree1CleanPage(Handle *hndl, Btree1Set *set, uint32_t totKeyLen);
DbStatus btree1SplitPage (Handle *hndl, Btree1Set *set);
DbStatus btree1FixKey (Handle *index, uint8_t *fenceKey, uint64_t prev, uint64_t suffix, uint8_t lvl, bool stopper);
DbStatus btree1InsertSfxKey(Handle *hndl, uint8_t *key, uint32_t keyLen, uint64_t suffix, uint8_t lvl, Btree1SlotType type);

void btree1LockPage(Btree1Page *page, Btree1Lock mode);
void btree1UnlockPage(Btree1Page *page, Btree1Lock mode);
int btree1KeyCmp (uint8_t *key1, uint8_t *key2, uint32_t len2);
