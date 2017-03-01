#pragma once

//  listHead:		frame of newest nodes waiting to be recycled
//	listWait:		frame of oldest nodes waiting to be recycled
//	listFree;		frames of available free objects

#define listHead(hndl, type) (&hndl->frames[type])
#define listWait(hndl, type) (&hndl->frames[type + hndl->maxType[0]])
#define listFree(hndl, type) (&hndl->frames[type + 2 * hndl->maxType[0]])

//	Handle for an arena
//	these live in red/black entries.

union Handle_ {
  struct {
	DbMap *map;				// pointer to map, zeroed on close
	DbAddr addr;			// location of this handle in hndlMaps
	DbAddr *frames;			// frames ready and waiting to be recycled
	ObjId hndlId;			// Handle Id
	uint64_t entryTs;		// time stamp of first api call
	uint32_t bindCnt[1];		// count of open api calls (handle binds)
	uint32_t lockedDocs[1];	// count of open api calls (handle binds)
	uint32_t xtraSize;		// size of following structure
	int8_t maxType[1];		// number of arena list entries
	int8_t status[1];		// current status of the handle
	uint8_t hndlType;		// type of handle
	uint8_t relaxTs;
	uint16_t listIdx;		// arena free frames entry index
	uint16_t arrayIdx;		// arena handle array index
  };
  char filler[64];	// fill cache line
};

void disableHndls(DbMap *db, DbAddr *hndlCalls);
uint64_t scanHandleTs(DbMap *map);

Handle *makeHandle(DbMap *map, uint32_t xtraSize, HandleType type);
void releaseHandle(Handle *hndl, DbHandle *dbHndl);
bool enterHandle(Handle *handle, DbAddr *slot);
Handle *bindHandle(DbHandle *dbHndl);
DbAddr *slotHandle(ObjId hndlId);

void destroyHandle(Handle *hndl, DbAddr *slot);
void initHndlMap(char *path, int pathLen, char *name, int nameLen, bool onDisk);
