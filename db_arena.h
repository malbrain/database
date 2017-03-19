#pragma once

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

#define MAX_segs  1000
#define MIN_segbits	 17
#define MIN_segsize  (1ULL << MIN_segbits)
#define MAX_segsize  (1ULL << (32 + 3))  // 32 bit offset and 3 bit multiplier

#define MAX_path  4096
#define MAX_blk		49	// max arena blk size in half bits

//  disk arena segment

typedef struct {
	uint64_t off;		// file offset of segment
	uint64_t size;		// size of the segment
	DbAddr nextObject;	// next Object address
	ObjId nextId;		// highest object ID in use
} DbSeg;

//	skip list head

struct SkipHead_ {
	DbAddr head[1];		// list head
	RWLock lock[1];		// reader/writer lock
};

//	Skip list entry

typedef struct SkipEntry_ {
	uint64_t key[1];	// entry key
	uint64_t val[1];	// entry value
} SkipEntry;

//	size of skip list entry array

typedef struct {
	DbAddr next[1];		// next block of keys
	SkipEntry array[0];	// array of key/value pairs
} SkipNode;

//  arena creation specifications
//	data is permanent in database arena

typedef struct {
	uint64_t id;				// our id in parent's child list
	uint64_t nxtVer;			// next arena version when creating
	uint64_t childId;			// highest child Id we've issued
	uint64_t creation;			// milliseconds since 1/1/70
	uint32_t localSize;			// extra space after DbMap
	uint32_t baseSize;			// extra space after DbArena
	uint32_t objSize;			// size of ObjectId array slot
	uint32_t mapIdx;			// index in openMap array
	uint16_t storeId;			// global docStore ID
	uint8_t arenaType;			// type of the arena
	uint8_t numTypes;			// number of node types
	char dead[1];				// arena file killed/deleted
	DbAddr nameTree[1];			// child arena name red/black tree
	SkipHead idList[1];			// child skiplist of names by id
	DbAddr hndlArray[1];		// array of handle ids for this arena
	Params params[MaxParam];	// parameter array for rest of object
} ArenaDef;

//  arena at beginning of seg zero

typedef struct {
	DbSeg segs[MAX_segs]; 			// segment meta-data
	uint64_t lowTs, delTs, nxtTs;	// low hndl ts, Incr on delete
	DbAddr freeBlk[MAX_blk];		// free blocks in frames
	DbAddr freeFrame[1];			// free frames in frames
	DbAddr listArray[1];			// free lists array for handles
	DbAddr rbAddr[1];				// address of r/b entry
	uint64_t objCount;				// overall number of objects
	uint64_t objSpace;				// overall size of objects
	uint16_t currSeg;				// index of highest segment
	uint16_t objSeg;				// current segment index for ObjIds
	char mutex[1];					// arena allocation lock/drop flag
	char type[1];					// arena type
	uint8_t filler[128];
} DbArena;

//	per instance arena structure

struct DbMap_ {
	char *base[MAX_segs];	// pointers to mapped segment memory
#ifndef _WIN32
	int hndl;				// OS file handle
#else
	HANDLE hndl;
	HANDLE maphndl[MAX_segs];
#endif
	DbArena *arena;			// ptr to mapped seg zero
	char *arenaPath;		// file database path
	DbMap *parent, *db;		// ptr to parent and database
	ArenaDef *arenaDef;		// pointer to database object
	SkipHead childMaps[1];	// skipList of child DbMaps
	uint32_t openCnt[1];	// count of open children
	uint32_t objSize;		// size of ObjectId array slot
	uint16_t pathLen;		// length of arena path
	uint16_t numSeg;		// number of mapped segments
	char mapMutex[1];		// segment mapping mutex
	char drop[1];			// arena map being killed
};

#define skipSize(addr) (((1ULL << addr->type) - sizeof(SkipNode)) / sizeof(SkipEntry))

#define SKIP_node 15

//	catalog structure

typedef union {
  struct {
	DbAddr openMap[1];		// process openMap array index assignments
	DbAddr storeId[1];		// global array of document store ids
  };
  char filler[256];
} Catalog;

/**
 * open/create arenas
 */

DbMap *openMap(DbMap *parent, char *name, uint32_t nameLen, ArenaDef *arena, RedBlack *entry);
DbMap *arenaRbMap(DbMap *parent, RedBlack *entry);

RedBlack *procParam(DbMap *parent, char *name, int nameLen, Params *params);
DbMap *initArena (DbMap *map, ArenaDef *arenaDef, char *name, uint32_t nameLen, RedBlack *rbEntry);

/**
 *  memory mapping
 */

void* mapMemory(DbMap *map, uint64_t offset, uint64_t size, uint32_t segNo);
void unmapSeg(DbMap *map, uint32_t segNo);
bool mapSeg(DbMap *map, uint32_t segNo);

bool newSeg(DbMap *map, uint32_t minSize);
void mapSegs(DbMap *map);

DbStatus dropMap(DbMap *db, bool dropDefs);
void getPath(DbMap *map, char *name, uint32_t nameLen, uint64_t ver);
uint32_t addPath(char *path, uint32_t len, char *name, uint32_t nameLen, uint64_t ver);
