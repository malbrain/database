#pragma once

//  arena api entry counts
//	for array list of handles

typedef struct {
	uint64_t entryTs;		// time stamp on first api entry
	uint32_t entryCnt[1];	// count of running api calls
	uint16_t entryIdx;		// entry Array index
} HndlCall;

//	Local Handle for an arena
//	entries live in red/black entries.

struct Handle_ {
	DbMap *map;			// pointer to map, zeroed on close
	FreeList *list;		// list of objects waiting to be recycled in frames
	HndlCall *calls;	// in-use & garbage collection counters
	DbAddr next;		// next entry in red/black handle tree
	uint16_t arenaIdx;	// arena handle table entry index
	uint16_t listIdx;	// arena handle table entry index
	uint16_t xtraSize;	// size of following structure
	uint8_t hndlType;	// type of handle
	uint8_t maxType;	// number of arena list entries
};

typedef struct {
	DbAddr addr;
	uint32_t refCnt[1];
	char latch[1];
} HandleId;

uint64_t scanHandleTs(DbMap *map);

uint64_t makeHandle(DbMap *map, uint32_t xtraSize, uint32_t listMax, HandleType type);
DbStatus bindHandle(DbHandle *dbHndl, Handle **hndl);
void releaseHandle(Handle *hndl);
void closeHandle(DbHandle *hndl);
Handle *getHandle(DbHandle *hndl);
HandleId *slotHandle(uint64_t hndlBits);

void destroyHandle(HandleId *slot);

