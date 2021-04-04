#pragma once

#ifndef _WIN32
#include <pthread.h>
#else
#define WIN32_LEAN_AND_MEAN
#include <Windows.h>
#endif


 bool Btree1_stats, debug;

//	general object pointer

typedef union {
  uint64_t bits;
  uint64_t addr:48;		// address part of struct below
  uint64_t verNo:48;	// document version number

  struct {
	  uint32_t off;	// 16 byte offset in segment
	  uint16_t seg;	// slot index in arena segment array
		union {
		  uint8_t step:8;
		  uint16_t xtra[1];	// xtra bits 
		  volatile uint8_t latch[1];
		  struct {
		    uint8_t mutex :1;	// mutex bit
		    uint8_t kill  :1;	// kill entry
		    uint8_t type  :6;	// object type
		    union {
			    uint8_t nbyte;	// number of bytes in a span node
			    uint8_t nslot;		// number of frame slots in use
					uint8_t maxidx;		// maximum slot index in use
					uint8_t firstx;		// first array inUse to chk
					uint8_t ttype;		// index transaction type
					uint8_t docIdx;     // document key index no
		  	  int8_t rbcmp;       // red/black comparison
			  };
			};
		};
	};
} DbAddr, ObjId;

// typedef DbAddr ObjId;

#define TYPE_SHIFT (6*8 + 2)	// number of bits to shift type left and zero all bits
#define BYTE_SHIFT (2)			// number of bits to shift type left and zero latch
#define MUTEX_BIT  0x01
#define KILL_BIT   0x02
#define TYPE_BITS  0xFC

#define ADDR_MUTEX_SET	0x0001000000000000ULL
#define ADDR_KILL_SET	0x0002000000000000ULL
#define ADDR_BITS		0x0000ffffffffffffULL
/*
typedef union {
	struct {
		uint32_t idx;		// record ID in the segment
		uint16_t seg;		// arena segment number
		union {
			uint8_t step :8;
			uint16_t xtra[1];	// xtra bits 
		};
	};
	uint64_t addr:48;		// address part of struct above
	uint64_t bits;
} ObjId;
*/
#define MAX_key	65536

// string /./content

typedef struct {
	uint16_t len;
	uint8_t str[];
} DbString;

typedef struct SkipHead_ SkipHead;
typedef struct DbMap_ DbMap;

//	param slots

typedef enum {
	Size = 0,		// total Params structure size	(int)
	OnDisk,			// Arena resides on disk	(bool)
	InitSize,		// initial arena size	(int)
	ObjIdSize,		// size of arena ObjId array element	(int)
	ClntSize,		// Handle client area size (DbCursor, Iterator)  (int)
	XtraSize,		// Handle client extra storage (leaf page buffer) (int)
	ArenaXtra,		// extra bytes in arena	(DbIndex, DocStore) (int)

	RecordType = 10,// arena document record type: 0=raw, 1=mvcc
	MvccBlkSize,    // initial mvcc document size

	IdxKeyUnique = 15,	// index keys uniqueness constraint	(bool)
	IdxKeyDeferred,		// uniqueness constraints deferred to commit	(bool)
	IdxKeyAddr,			// index key definition address
	IdxKeySparse,
	IdxKeyPartial,		// offset of partial document
	IdxKeyFlds,			// store field lengths in keys	(bool)
	IdxType,			// 0 for artree, 1 & 2 for btree	(int)
	IdxNoDocs,			// stand-alone index file	(bool)

	Btree1Bits = 25,	// Btree1 page size in bits	(int)
	Btree1Xtra,			// leaf page extra bits	(int)

	Btree2Bits = 28,	// Btree2 page size in bits	(int)
	Btree2Xtra,			// leaf page extra bits	(int)

	CursorDeDup = 30,	// de-duplicate cursor results	(bool)
	Concurrency,
	ResultSetSize,	// # cursor keys or # iterator docs returned (int)

	UserParams = 40,
	MaxParam = 64		// count of param slots defined
} ParamSlot;

typedef union {
	uint64_t intVal;
	uint32_t offset;
	double dblVal;
	uint32_t wordVal;
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

// user's DbHandle
//	contains the Handle ObjId bits

typedef union {
	ObjId hndlId;
	uint64_t hndlBits;
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

#define HandleAddr(dbHndl) fetchIdSlot(hndlMap, dbHndl->hndlId)
#define MapAddr(handle) (DbMap *)(db_memObj(handle->mapAddr))
#define ClntAddr(handle) getObj(MapAddr(handle), handle->clientAddr)

DbMap *hndlMap;

// document header in docStore
// next hdrs in set follow, up to docMin

typedef enum {
    VerRaw,
    VerMvcc
} DocType;

typedef struct {
  union {
    uint32_t refCnt[1];
    uint8_t base[4];
  };
  uint32_t mapId;     // db child id
  DocType docType;
  DbAddr ourAddr;
  ObjId docId;
} DbDoc;


#include "db_arena.h"
#include "db_index.h"
#include "db_cursor.h"
#include "db_map.h"
#include "db_error.h"
#include "db_frame.h"
#include "db_api.h"
#include "db_malloc.h"
#include "db_object.h"
#include "db_handle.h"
