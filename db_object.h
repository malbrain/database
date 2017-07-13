#pragma once

//  number of elements in an array node
//	max element idx = ARRAY_size * ARRAY_lvl1

//	note that each level zero block reserves first few
//	indexes (ARRAY_first) for the inUse bit map

#define ARRAY_size	256			// level zero slot count
#define ARRAY_lvl1	(256 - 2)	// adjust to power of two sizeround

//	define the number of inUse slots per level zero block

#define ARRAY_inuse	((ARRAY_size + 64 - 1) / 64)

//  calculate the number of sluffed indexes because of inUse bit maps

#define ARRAY_first(objsize) ((objsize) ? (ARRAY_inuse * sizeof(uint64_t) + (objsize) - 1) / (objsize) : 0)

//	Arrays

typedef struct {
	uint16_t level0;			// level0 slot to allocate
	uint16_t maxLvl0;			// number of level one blocks
	uint32_t objSize;			// size of each array element
	DbAddr addr[ARRAY_lvl1];	// level one block addresses
} ArrayHdr;

void *arrayElement(DbMap *map, DbAddr *array, uint16_t idx, uint32_t size);
void *arrayEntry(DbMap *map, DbAddr *array, uint16_t idx);

uint16_t arrayAlloc(DbMap *map, DbAddr *array, uint32_t size);
uint16_t arrayFirst(uint32_t objSize);

void arrayRelease(DbMap *map, DbAddr *array, uint16_t idx);

enum ObjType {
	FrameType,
	ObjIdType,			// ObjId value
	MinObjType = 4,		// minimum object size in bits
	MaxObjType = 50		// each half power of two, 4 - 24
};

//  set-membership control

typedef struct {
	DbAddr next;
	uint16_t cnt;		// count of entries in this table
	uint16_t max;		// number of hash table entries
	uint16_t sizeIdx;	// hash table size vector slot
	uint64_t table[0];	// the hash table entries
} DbMmbr;

DbMmbr *xtnMmbr(DbMap *map, DbAddr *addr, DbMmbr *first);
DbMmbr *iniMmbr(DbMap *map, DbAddr *addr, int minSize);

//  mmbr table enumerators

void *getMmbr(DbMmbr *mmbr, uint64_t item);
void *nxtMmbr(DbMmbr *mmbr, uint64_t *entry);
void *allMmbr(DbMmbr *mmbr, uint64_t *entry);
void *revMmbr(DbMmbr *mmbr, uint64_t *entry);

//	mmbr-set functions

uint64_t *setMmbr(DbMap *map, DbAddr *addr, uint64_t keyVal, bool add);
uint64_t *newMmbr(DbMap *map, DbAddr *addr, uint64_t keyVal);
