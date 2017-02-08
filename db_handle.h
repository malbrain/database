#pragma once

//  listHead:		frame of newest nodes waiting to be recycled
//	listWait:		frame of oldest nodes waiting to be recycled
//	listFree;		frames of available free objects

#define listHead(hndl, type) (&hndl->frames[type])
#define listWait(hndl, type) (&hndl->frames[type + hndl->maxType])
#define listFree(hndl, type) (&hndl->frames[type + 2 * hndl->maxType])

//	Local Handle for an arena
//	these live in red/black entries.

union Handle_ {
  struct {
	DbMap *map;				// pointer to map, zeroed on close
	ObjId hndlId;			// object Id entry in catalog
	DbAddr *frames;			// frames ready and waiting to be recycled
	uint64_t entryTs;		// time stamp of first api call
	int32_t bindCnt[1];		// count of open api calls (handle binds)
	int32_t lockedDocs[1];	// count of open api calls (handle binds)
	uint16_t arenaIdx;		// arena handle table entry index
	uint16_t xtraSize;		// size of following structure
	uint16_t listIdx;		// arena free frames entry index
	uint8_t hndlType;		// type of handle
	uint8_t maxType;		// number of arena list entries
	uint8_t relaxTs;
  };
  char filler[64];	// fill cache line
};

void disableHndls(DbMap *db, DbAddr *hndlCalls);
uint64_t scanHandleTs(DbMap *map);

uint64_t makeHandle(DbMap *map, uint32_t xtraSize, HandleType type);
void releaseHandle(Handle *hndl, DbHandle *dbHndl);
Handle *bindHandle(DbHandle *dbHndl);
Handle *getHandle(DbHandle *hndl);
DbAddr *slotHandle(ObjId hndlId);

void destroyHandle(Handle *hndl, DbAddr *slot);
void initHndlMap(char *path, int pathLen, char *name, int nameLen, bool onDisk);

