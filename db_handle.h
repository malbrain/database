#pragma once

//  arena api entry counts
//	for array list of handles

typedef struct {
	uint64_t entryTs;	// time stamp on first api entry
	ObjId hndlId;		// object Id entry in catalog
} HndlCall;

//	Local Handle for an arena
//	entries live in red/black entries.

struct Handle_ {
	DbMap *map;			// pointer to map, zeroed on close
	DbAddr calls;		// current handle timestamp
	ObjId hndlId;		// object Id entry in catalog
	FreeList *list;		// obj frames waiting to be recycled
	uint16_t arenaIdx;	// arena handle table entry index
	uint16_t xtraSize;	// size of following structure
	uint16_t callIdx;	// arena call table entry index
	uint16_t listIdx;	// arena list table entry index
	uint8_t hndlType;	// type of handle
	uint8_t maxType;	// number of arena list entries
};

typedef struct {
	DbAddr addr[1];
	uint32_t bindCnt[1];	// count of outstanding handle binds
} HandleId;

uint64_t scanHandleTs(DbMap *map);

uint64_t makeHandle(DbMap *map, uint32_t xtraSize, uint32_t listMax, HandleType type);
Handle *bindHandle(DbHandle *dbHndl);
void releaseHandle(Handle *hndl);
Handle *getHandle(DbHandle *hndl);
HandleId *slotHandle(uint64_t hndlBits);

void destroyHandle(DbMap *map, DbAddr *addr);
void initHndlMap(char *path, int pathLen, char *name, int nameLen, bool onDisk);

