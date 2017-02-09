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
	uint32_t objSize;
	uint16_t nxtIdx;			// next new index to allocate
	uint16_t maxIdx;			// maximum index range
	DbAddr availIdx[1];			// frames of available indexes
	DbAddr addr[ARRAY_lvl1];	// level zero block addresses
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

/**
 * even =>  reader timestamp
 * odd  =>  writer timestamp
 */

enum ReaderWriterEnum {
	en_reader,
	en_writer,
	en_current
};

//	timestamp bits

bool isReader(uint64_t ts);
bool isWriter(uint64_t ts);
bool isCommitted(uint64_t ts);

uint64_t allocateTimestamp(DbMap *map, enum ReaderWriterEnum e);
