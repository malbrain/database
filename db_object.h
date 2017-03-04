#pragma once

//  number of elements in an array node
//	max element idx = ARRAY_size * ARRAY_lvl1

//	note that each level zero block reserves first few
//	indexes (ARRAY_first) for the inUse bit map

#define ARRAY_size	256
#define ARRAY_lvl1	(256 - 2)	// adjust to power of two sizeround

//	define the number of inUse slots per level zero block

#define ARRAY_inuse	((ARRAY_size + 64 - 1) / 64)

//  define the number of sluffed indexes because of inUse bit maps

#define ARRAY_first(objsize) (objsize ? (ARRAY_inuse * sizeof(uint64_t) + (objsize) - 1) / (objsize) : 0)

//	Arrays

typedef struct {
	uint32_t nxtIdx;			// next new index to allocate
	uint32_t objSize;
	DbAddr availIdx[1];			// frames of available indexes
	DbAddr addr[ARRAY_lvl1];	// level one block addresses
} ArrayHdr;

void *arrayElement(DbMap *map, DbAddr *array, uint16_t idx, size_t size);
void *arrayEntry(DbMap *map, DbAddr *array, uint16_t idx);

uint16_t arrayAlloc(DbMap *map, DbAddr *array, size_t size);
void arrayActivate(DbMap *map, DbAddr *array, uint16_t idx);
void arrayRelease(DbMap *map, DbAddr *array, uint16_t idx);

enum ObjType {
	FrameType,
	ObjIdType,			// ObjId value
	MinObjType = 3,		// minimum object size in bits
	MaxObjType = 49		// each half power of two, 3 - 24
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
