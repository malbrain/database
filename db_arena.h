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

//  arena at beginning of seg zero

struct DbArena_ {
	DbSeg segs[MAX_segs]; 			// segment meta-data
	int64_t lowTs, delTs, nxtTs;	// low hndl ts, Incr on delete
	DbAddr freeBlk[MAX_blk];		// free blocks in frames
	DbAddr freeFrame[1];			// free frames in frames
	DbAddr redblack[1];				// our redblack entry addr
	uint64_t objCount;				// overall number of objects
	uint64_t objSpace;				// overall size of objects
	uint32_t baseSize;				// size of object following arena
	uint32_t objSize;				// size of object array element
	uint16_t currSeg;				// index of highest segment
	uint16_t objSeg;				// current segment index for ObjIds
	char mutex[1];					// arena allocation lock/drop flag
	char type[1];					// arena type

	char filler[256];
};

//	skip list head

struct SkipHead_ {
	DbAddr head[1];		// list head
	RWLock lock[1];		// reader/writer lock
};

//	Skip list entry

struct SkipEntry_ {
	uint64_t key[1];	// entry key
	uint64_t val[1];	// entry value
};

//	size of skip list entry array

typedef struct {
	DbAddr next[1];		// next block of keys
	SkipEntry array[0];	// array of key/value pairs
} SkipNode;

//  arena creation specifications
//	data is permanent in database arena

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
	DbAddr hndlIds[1];			// array of handleID for this arena
	DbAddr parentAddr;			// address of parent's red-black entry
	DbAddr nameTree[1];			// child arena name red/black tree
	SkipHead idList[1];			// child skiplist of names by id
	Params params[MaxParam + 1];	// parameter array for rest of object
} ArenaDef;

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
	SkipHead childMaps[1];	// skipList of child DbMaps
	ArenaDef *arenaDef;		// our arena definition
	DbAddr listArray[1];	// free lists array for handles
	int32_t openCnt[1];		// count of open children
	uint16_t pathLen;		// length of arena path
	uint16_t numSeg;		// number of mapped segments
	char mapMutex[1];		// segment mapping mutex
};

#define skipSize(addr) (((1ULL << addr->type) - sizeof(SkipNode)) / sizeof(SkipEntry))

#define SKIP_node 15

//	database variables

typedef struct {
	int64_t timestamp[1];	// database txn timestamp
	DbAddr txnIdx[1];		// array of active idx for txn entries
} DataBase;

#define database(db) ((DataBase *)(db->arena + 1))

//	catalog structure

typedef struct {
	DbAddr openMap[1];		// process openMap array index assignments
	DbAddr dbList[1];		// red/black tree of database names & versions
	char filler[256];
} Catalog;

//	docarena variables

typedef struct {
	uint16_t docIdx;		// our map index for txn
	uint8_t init;			// set on init
	char filler[256];
} DocArena;

#define docarena(map) ((DocArena *)(map->arena + 1))

/**
 * open/create arenas
 */

DbMap *openMap(DbMap *parent, char *name, uint32_t nameLen, ArenaDef *arena, DbAddr *rbAddr);
DbMap *arenaRbMap(DbMap *parent, RedBlack *entry);

RedBlack *procParam(DbMap *parent, char *name, int nameLen, Params *params);
DbMap *initArena (DbMap *map, ArenaDef *arenaDef, char *name, uint32_t nameLen, DbAddr *rbAddr);

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

#ifdef _WIN32
HANDLE openPath(char *name);
#else
int openPath(char *name);
#endif

