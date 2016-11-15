#pragma once

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <limits.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>

#include "db_error.h"

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
	Hndl_docVersion
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
			uint8_t ttype;		// index transaction type
			int8_t rbcmp;		// red/black comparison
		};
	};
	uint64_t bits;
	struct {
		uint64_t addr:48;
		uint64_t fill:16;
	};
} DbAddr;

#define MUTEX_BIT  0x80
#define KILL_BIT   0x40
#define TYPE_BITS  0x3f

#define ADDR_MUTEX_SET 0x80000000000000ULL
#define ADDR_KILL_SET  0x40000000000000ULL

typedef union {
	struct {
		uint32_t index;		// record ID in the segment
		uint16_t seg:10;	// arena segment number
		uint16_t cmd:6;		// for use in txn
		uint16_t idx;		// document store arena idx
	};
	uint64_t bits;
} ObjId;

typedef struct DbArena_ DbArena;
typedef struct Handle_ Handle;
typedef struct DbMap_ DbMap;

//	param slots

typedef enum {
	OnDisk = 0,		// base set
	InitSize,		// arena size
	UseTxn,			// txn used
	NoDocs,			// no documents
	DropDb,			// drop the database

	IdxKeySpec = 10,	// index key spec document
	IdxKeySpecLen,		// this must immediately follow
	IdxKeyUnique,
	IdxKeySparse,
	IdxKeyPartial,
	IdxKeyPartialLen,	// this must immediately follow

	Btree1Bits = 20,	// Btree1 set
	Btree1Xtra,

	MaxParam = 30	// param array size
} ParamSlot;

typedef union {
	uint64_t intVal;
	uint32_t offset;
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

typedef struct {
	DbAddr verKeys[1];	// skiplist of versions with Id key
	DbAddr prevDoc[1];	// previous version of doc
	uint64_t version;	// version of the document
	DbAddr addr;		// docStore arena address
	ObjId docId;		// ObjId of the document
	ObjId txnId;		// insert/update txn ID
	ObjId delId;		// delete txn ID
	uint32_t size;		// document size
	DocState state;		// document state
} Doc;

// user's DbHandle
//	contains the HandleId ObjId bits

typedef struct {
	uint64_t hndlBits;
} DbHandle;

// cursor positioning operations

typedef enum {
	OpLeft	= 'l',
	OpRight = 'r',
	OpNext	= 'n',
	OpPrev	= 'p',
	OpFind	= 'f',
	OpOne	= 'o'
} CursorOp;

uint32_t get64(char *key, uint32_t len, uint64_t *result);
uint32_t store64(char *key, uint32_t keylen, uint64_t what);
