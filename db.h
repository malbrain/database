#pragma once

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <limits.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>

#include "db_error.h"
#include "db_malloc.h"
#include "db_lock.h"

#define MAX_key		4096	// maximum key size in bytes

//	types of handles/arenas

typedef enum {
	Hndl_newarena = 0,
	Hndl_catalog,
	Hndl_database,
	Hndl_docStore,
	Hndl_artIndex,
	Hndl_btree1Index,
	Hndl_btree2Index,
	Hndl_colIndex,
	Hndl_iterator,
	Hndl_cursor,
	Hndl_txn
} HandleType;

//	general object pointer

typedef union {
	struct {
		uint32_t offset;	// offset in the segment
		uint16_t segment;	// arena segment number
		union {
			struct {
				uint8_t type:6;		// object type
				uint8_t kill:1;		// kill entry
				uint8_t mutex:1;	// mutex bit
			};
			volatile char latch[1];
		};
		union {
			uint8_t nbyte;		// number of bytes in a span node
			uint8_t nslot;		// number of frame slots in use
			uint8_t maxidx;		// maximum slot index in use
			uint8_t keyEnd;		// node continues the key
			uint8_t ttype;		// index transaction type
			int8_t rbcmp;		// red/black comparison
		};
	};
	uint64_t addr:48;			// segment/offset
	uint64_t bits;
} DbAddr;

#define TYPE_SHIFT (6*8)		// number of bits to shift type left
#define MUTEX_BIT  0x80
#define KILL_BIT   0x40
#define TYPE_BITS  0x3f

#define ADDR_MUTEX_SET 0x80000000000000ULL
#define ADDR_KILL_SET  0x40000000000000ULL

typedef union {
	struct {
		uint32_t index;		// record ID in the segment
		uint16_t seg:12;	// arena segment number
		uint16_t cmd:4;		// for use in txn
		uint16_t store;		// document store idx from catalog
	};
	uint64_t addr:42;		// address part of struct above
	uint64_t bits;
} ObjId;

typedef struct SkipHead_ SkipHead;
typedef struct RedBlack_ RedBlack;
typedef struct DbArena_ DbArena;
typedef union Handle_ Handle;
typedef struct DbMap_ DbMap;

//	param slots

typedef enum {
	Size = 0,		// total Params structure size
	OnDisk,			// Arena resides on disk
	InitSize,		// initial arena size
	UseTxn,			// transactions used
	NoDocs,			// no documents, just indexes
	NoIdx,			// no indexes, just documents
	DropDb,			// drop the database
	DocStoreId,		// document store ID

	IdxKeyUnique = 10,
	IdxKeySparse,
	IdxKeyPartial,		// offset of partial document
	IdxBinary,			// treat string fields as binary
	IdxType,			// 0 for artree, 1 & 2 for btree

	Btree1Bits = 20,	// Btree1 set
	Btree1Xtra,

	CursorTxn = 25,

	IteratorEnd = 30,  // position iterator at end?

	MaxParam = 40	// maximum param array in use
} ParamSlot;

typedef union {
	uint64_t intVal;
	uint32_t offset;
	double dblVal;
	bool boolVal;
	void *obj;
} Params;

typedef enum {
	DocUnused = 0,
	DocActive,
	DocInsert,
	DocDelete,
	DocDeleted
} DocState;

typedef struct Ver_ {
	uint64_t version;	// document version
	uint32_t offset;	// offset from origin
	uint32_t size;		// version size
	ObjId docId;		// document ObjId
	ObjId txnId;		// insert/version txn ID
} Ver;

typedef struct {
	uint32_t refCnt[1];	// active references to the document
	DbAddr prevDoc[1];	// previous versions of doc
	DbAddr addr;		// doc arena address
	ObjId delId;		// delete txn ID
	uint32_t verCnt;	// number of versions
	uint32_t lastVer;	// offset of last version
	DocState state;		// document state
	Ver ver[1];			// base version
} Doc;

// user's DbHandle
//	contains the Handle ObjId bits

typedef struct {
	uint64_t hndlBits;
} DbHandle;

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

uint32_t get64(char *key, uint32_t len, uint64_t *result);
uint32_t store64(char *key, uint32_t keylen, uint64_t what);
void *getParamOff(Params *params, uint32_t off);
void *getParamIdx(Params *params, uint32_t idx);
