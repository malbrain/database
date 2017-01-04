#pragma once

#include "db_lock.h"

//  number of elements in an array node
//	max element idx = ARRAY_size * ARRAY_lvl1

#define ARRAY_size	256
#define ARRAY_lvl1	256
#define ARRAY_inuse	((ARRAY_size + 64 - 1) / 64)
#define ARRAY_first(objsize) ((ARRAY_inuse * sizeof(uint64_t) + (objsize) - 1) / (objsize))

enum ObjType {
	FrameType,
	ObjIdType,			// ObjId value
	MinObjType = 3,		// minimum object size in bits
	MaxObjType = 49		// each half power of two, 3 - 24
};

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
	RWLock lock[1];		// reader/writer lock
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

//  arena creation specification

typedef struct {
	uint64_t id;				// our id in parent children
	uint64_t ver;				// current arena version
	int64_t childId;			// highest child Id we've issued
	uint32_t localSize;			// extra space after DbMap
	uint32_t baseSize;			// extra space after DbArena
	uint32_t objSize;			// size of ObjectId array slot
	uint32_t mapIdx;			// index in openMap array
	uint8_t arenaType;			// type of the arena
	uint8_t numTypes;			// number of node types
	char dead[1];				// arena being deleted
	DbAddr parentAddr;			// address of parent's red-black entry
	DbAddr nameTree[1];			// child arena name red/black tree
	DbAddr hndlCalls[1];		// array of bound handle counts
	SkipHead idList[1];			// child skiplist of names by id
	Params params[MaxParam];	// parameter array for rest of object
} ArenaDef;

#define skipSize(addr) (((1ULL << addr->type) - sizeof(SkipNode)) / sizeof(SkipEntry))
#define SKIP_node 15

//	timestamp bits

bool isReader(uint64_t ts);
bool isWriter(uint64_t ts);
bool isCommitted(uint64_t ts);

uint64_t allocateTimestamp(DbMap *map, enum ReaderWriterEnum e);

void *arrayElement(DbMap *map, DbAddr *array, uint16_t idx, size_t size);
void *arrayEntry(DbMap *map, DbAddr array, uint16_t idx, size_t size);

uint16_t arrayAlloc(DbMap *map, DbAddr *array, size_t size);
uint64_t *arrayBlk(DbMap *map, DbAddr *array, uint16_t idx);
uint64_t arrayAddr(DbMap *map, DbAddr *array, uint16_t idx);

SkipEntry *skipSearch(SkipEntry *array, int high, uint64_t key);
uint64_t skipDel(DbMap *map, DbAddr *skip, uint64_t key);
uint64_t *skipFind(DbMap *map, DbAddr *skip, uint64_t key);
uint64_t *skipPush(DbMap *map, DbAddr *skip, uint64_t key);
uint64_t *skipAdd(DbMap *map, DbAddr *skip, uint64_t key);
uint64_t skipInit(DbMap *map, int numEntries);

