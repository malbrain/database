#pragma once

#include "db_lock.h"

enum ObjType {
	FrameType,
	ObjIdType,			// ObjId value
	MinObjType = 3,		// minimum object size in bits
	MaxObjType = 49		// each half power of two, 3 - 24
};

typedef struct {
	DbAddr tail[1];		// location of newest frame to be recycled
	DbAddr head[1];		// oldest frame waiting to be recycled
	DbAddr free[1];		// frames of available free objects
} FreeList;

//  arena api entry counts
//	for array list of handles

typedef struct {
	uint64_t entryCnt[1];	// count of running api calls
	uint64_t entryTs;	// time stamp on first api entry
} HandleCalls;

//	Local Handle for an arena

struct Handle_ {
	DbMap *map;			// pointer to map, zeroed on close
#ifdef ENFORCE_CLONING
	DbHandle *addr;		// user's DbHandle location address
#endif
	FreeList *list;		// list of free blocks
	int32_t status[1];	// active entry in use count
	uint16_t arenaIdx;	// arena handle table entry index
	uint16_t listIdx;	// arena handle table entry index
	uint16_t xtraSize;	// size of following structure
	uint8_t hndlType;	// type of handle
	uint8_t maxType;	// number of arena list entries
};

//	types of handles/arenas

typedef enum {
	NotSetYet = 0,
	DatabaseType,
	DocStoreType,
	Btree1IndexType,
	Btree2IndexType,
	ARTreeIndexType,
	IteratorType,
	CursorType
} HandleType;

/**
 * even =>  reader timestamp
 * odd  =>  writer timestamp
 */

enum ReaderWriterEnum {
	en_reader,
	en_writer,
	en_current
};

//	skip list head

typedef struct {
	DbAddr head[1];		// list head
	RWLock2 lock[1];	// reader/writer lock
} SkipHead;

//	Skip list entry

typedef struct {
	uint64_t key[1];	// entry key
	uint64_t val[1];	// entry value
} SkipEntry;

//	size of skip list entry array

typedef struct {
	DbAddr next[1];		// next block of keys
	SkipEntry array[0];	// array of key/value pairs
} SkipNode;

#define skipSize(addr) (((1ULL << addr->type) - sizeof(SkipNode)) / sizeof(SkipEntry))
#define SKIP_first 15
#define SKIP_max 10

//	Child Id bits

#define CHILDID_DROP 0x1
#define CHILDID_INCR 0x2

bool isReader(uint64_t ts);
bool isWriter(uint64_t ts);
bool isCommitted(uint64_t ts);

uint32_t get64(uint8_t *key, uint32_t len, uint64_t *result);
uint32_t store64(uint8_t *key, uint32_t keylen, uint64_t what);
uint64_t makeHandle(DbMap *map, uint32_t xtraSize, uint32_t listMax, HandleType type);
void closeHandle(Handle  *hndl);

Status bindHandle(DbHandle *dbHndl, Handle **hndl);
void releaseHandle(Handle *hndl);

void *arrayElement(DbMap *map, DbAddr *array, uint16_t idx, size_t size);
uint16_t arrayAlloc(DbMap *map, DbAddr *array, size_t size);

SkipEntry *skipSearch(SkipEntry *array, int high, uint64_t key);
uint64_t skipDel(DbMap *map, DbAddr *skip, uint64_t key);
void *skipFind(DbMap *map, DbAddr *skip, uint64_t key);
void *skipPush(DbMap *map, DbAddr *skip, uint64_t key);
void *skipAdd(DbMap *map, DbAddr *skip, uint64_t key);
uint64_t skipInit(DbMap *map, int numEntries);
