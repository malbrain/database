#pragma once

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

//	timestamp bits

bool isReader(uint64_t ts);
bool isWriter(uint64_t ts);
bool isCommitted(uint64_t ts);

uint64_t allocateTimestamp(DbMap *map, enum ReaderWriterEnum e);

void *arrayElement(DbMap *map, DbAddr *array, uint16_t idx, size_t size);
void *arrayEntry(DbMap *map, DbAddr *array, uint16_t idx, size_t size);

uint16_t arrayAlloc(DbMap *map, DbAddr *array, size_t size);
uint64_t *arrayBlk(DbMap *map, DbAddr *array, uint16_t idx);
DbAddr* arrayAddr(DbMap *map, DbAddr *array, uint16_t idx);
