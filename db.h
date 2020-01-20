#pragma once
#define _GNU_SOURCE 1

#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <inttypes.h>
#include <time.h>
#include <memory.h>
#include <limits.h>
#include <string.h>
#include <assert.h>

#ifndef _WIN32
#include <unistd.h>
#include <pthread.h>
#endif

#include "db_error.h"

extern bool stats, debug;

typedef enum {
	prngRandom,		// pure randomness all streams unique
	prngProcess,	// all streams are the same
	prngThread		// each thread has own stream, repeats across processes
} PRNG;

void mynrand48seed(uint16_t* nrandState, PRNG prng, int init);
int createB64(uint8_t* key, int size, unsigned short next[3]);
uint64_t get64(uint8_t* key, uint32_t len);

#define MAX_key		4096	// maximum key size in bytes

//	general object pointer

typedef union {
	struct {
		uint32_t offset;	// offset in the segment
		uint16_t segment;	// arena segment number
		union {
			struct {
				uint8_t mutex :1;	// mutex bit
				uint8_t kill  :1;	// kill entry
				uint8_t type  :6;	// object type
			};
			volatile uint8_t latch[1];
		};
		union {
			uint8_t nbyte;		// number of bytes in a span node
			uint8_t nslot;		// number of frame slots in use
			uint8_t maxidx;		// maximum slot index in use
			uint8_t firstx;		// first array inUse to chk
			uint8_t ttype;		// index transaction type
            uint8_t docIdx;     // document key index no
            int8_t rbcmp;       // red/black comparison
		};
	};
	uint64_t addr:48;			// segment/offset
	uint64_t bits;
} DbAddr;

#define TYPE_SHIFT (6*8 + 2)	// number of bits to shift type left and zero all bits
#define BYTE_SHIFT (2)			// number of bits to shift type left and zero latch
#define MUTEX_BIT  0x01
#define KILL_BIT   0x02
#define TYPE_BITS  0xFC

#define ADDR_MUTEX_SET	0x0001000000000000ULL
#define ADDR_KILL_SET	0x0002000000000000ULL
#define ADDR_BITS		0x0000ffffffffffffULL

typedef union {
	struct {
		uint32_t idx;	// record ID in the segment
		uint16_t seg;	// arena segment number
		uint16_t xtra;	// xtra bits (available for txn)
	};
	uint64_t addr:48;	// address part of struct above
	uint64_t bits;
} ObjId;

// string content

typedef struct {
	uint16_t len;
	uint8_t str[];
} DbString;

typedef struct SkipHead_ SkipHead;
typedef struct RedBlack_ RedBlack;
typedef struct DbMap_ DbMap;

//	param slots

typedef enum {
	Size = 0,		// total Params structure size	(int)
	OnDisk,			// Arena resides on disk	(bool)
	InitSize,		// initial arena size	(int)
	ObjIdSize,		// size of arena ObjId array element	(int)
	ClntSize,		// Handle client area size (DbCursor, Iterator)  (int)
	ClntXtra,		// Handle client extra storage (leaf page buffer) (int)
	ArenaXtra,		// extra bytes in arena	(DbIndex, DocStore) (int)
	ResetVersion,	// reset arena version

	IdxKeyUnique = 10,	// index keys uniqueness constraint	(bool)
	IdxKeyDeferred,		// uniqueness constraints deferred to commit	(bool)
	IdxKeyAddr,			// index key definition address
	IdxKeySparse,
	IdxKeyPartial,		// offset of partial document
	IdxKeyFlds,			// store field lengths in keys	(bool)
	IdxType,			// 0 for artree, 1 & 2 for btree	(int)
	IdxNoDocs,			// stand-alone index file	(bool)

	Btree1Bits = 20,	// Btree1 page size in bits	(int)
	Btree1Xtra,			// leaf page extra bits	(int)

	Btree2Bits = 23,	// Btree2 page size in bits	(int)
	Btree2Xtra,			// leaf page extra bits	(int)

	CursorDeDup = 25,	// de-duplicate cursor results	(bool)

	UserParams = 30,
	MaxParam = 64		// count of param slots defined
} ParamSlot;

typedef union {
	uint64_t intVal;
	uint32_t offset;
	double dblVal;
	char charVal;
	bool boolVal;
	DbAddr addr;
	void *obj;
} Params;

// cursor move/positioning operations

typedef enum {
	OpLeft		= 'l',
	OpRight 	= 'r',
	OpNext		= 'n',
	OpPrev		= 'p',
	OpFind		= 'f',
	OpOne		= 'o',
	OpBefore	= 'b',
	OpAfter		= 'a'
} CursorOp;

// DbHandle

typedef struct {
  ObjId hndlId;
} DbHandle;

// DbVector definition

typedef struct {
	volatile uint8_t latch[1];
	uint8_t type;
	uint16_t vecLen;
	uint16_t vecMax;
	DbAddr next, vector[1];
} DbVector;
  
uint32_t vectorPush(DbMap*, DbVector *, DbAddr);
DbAddr *vectorFind(DbMap*, DbVector *, uint32_t);

uint32_t store64(uint8_t *key, uint32_t keylen, int64_t what);
uint64_t get64(uint8_t *key, uint32_t len);
uint32_t size64(uint8_t *key, uint32_t len);
uint64_t zone64(uint8_t* key, uint32_t len, uint32_t zone);
uint32_t calc64(int64_t value);

#include "db_malloc.h"
