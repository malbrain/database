#pragma once

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

#include "db_redblack.h"

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

//  arena creation specification

typedef struct {
	uint64_t id;				// child arena id in parent
	uint64_t childId;			// highest child Id issued
	uint64_t initSize;			// initial arena size
	uint64_t specAddr;			// database addr of spec object
	uint64_t partialAddr;		// database addr of partial object
	uint32_t localSize;			// extra space after DbMap
	uint32_t baseSize;			// extra space after DbArena
	uint32_t objSize;			// size of ObjectId array slot
	uint8_t onDisk;				// arena onDisk/inMemory
	uint8_t useTxn;				// transactions are used
	uint8_t arenaType;			// type of the arena
	DbAddr nameTree[1];			// child arena name red/black tree
	SkipHead idList[1];			// child skiplist of names by id
} ArenaDef;

//  arena at beginning of seg zero

struct DbArena_ {
	DbSeg segs[MAX_segs]; 			// segment meta-data
	uint64_t lowTs, delTs, nxtTs;	// low hndl ts, Incr on delete
	DbAddr freeBlk[MAX_blk];		// free blocks in frames
	DbAddr hndlCalls[1];			// array of open handle call cnts
	DbAddr listArray[1];			// free lists array for handles
	DbAddr freeFrame[1];			// free frames in frames
	uint64_t objCount;				// overall number of objects
	uint64_t objSpace;				// overall size of objects
	uint32_t objSize;				// size of object array element
	uint16_t currSeg;				// index of highest segment
	uint16_t objSeg;				// current segment index for ObjIds
	char UseTxn[1];					// Transactions are used for arena
	char mutex[1];					// arena allocation lock/drop flag
	char type[1];					// arena type
};

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
	DbMap *parent, *db;		// ptr to parent and database
	SkipHead childMaps[1];	// skipList of child DbMaps
	char path[MAX_path];	// file database path
	ArenaDef *arenaDef;		// our arena definition
	uint16_t pathLen;		// length of path in buffer
	uint16_t maxSeg;		// maximum mapped segment array index
	char mapMutex[1];		// segment mapping mutex
};

//	database variables

typedef struct {
	uint64_t timestamp[1];	// database txn timestamp
	ArenaDef arenaDef[1];	// database variables and root of children
	DbAddr txnIdx[1];		// array of active idx for txn entries
} DataBase;

#define database(db) ((DataBase *)(db->arena + 1))

//	docarena variables

typedef struct {
	uint16_t docIdx;		// our map index for txn
	uint8_t init;			// set on init
} DocArena;

#define docarena(map) ((DocArena *)(map->arena + 1))

DbMap *openMap(DbMap *parent, char *name, uint32_t nameLen, ArenaDef *arena);
DbMap *arenaRbMap(DbMap *parent, RedBlack *entry);
DbMap *initArena (DbMap *map, ArenaDef *arenaDef);


/**
 *  memory mapping
 */

void* mapMemory(DbMap *map, uint64_t offset, uint64_t size, uint32_t segNo);
void unmapSeg(DbMap *map, uint32_t segNo);
bool mapSeg(DbMap *map, uint32_t segNo);

bool newSeg(DbMap *map, uint32_t minSize);
void mapSegs(DbMap *map);

int getPath(char *path, int max, char *name, int len, DbMap *parent);
#ifdef _WIN32
HANDLE openPath(char *name);
#else
int openPath(char *name);
#endif

