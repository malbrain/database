#pragma once

//  listHead:		frame of newest nodes waiting to be recycled
//	listWait:		frame of oldest nodes waiting to be recycled
//	listFree;		frames of available free objects

#define listHead(hndl, type) (&hndl->frames[type])
#define listWait(hndl, type) (&hndl->frames[type + hndl->maxType[0]])
#define listFree(hndl, type) (&hndl->frames[type + 2 * hndl->maxType[0]])

//	Handle for an arena
//	these live in HndlMap (Catalog)

//	** marks fields that are
//	specific to the base arena

union Handle_ {
  struct {
	DbMap *map;				// pointer to map, zeroed on close **
	ObjId hndlId;			// Handle Id in HndlMap (Catalog)
	DbAddr addr;			// location of this handle in hndlMaps
	DbAddr *frames;			// recycle frames ready or waiting **
	uint64_t entryTs;		// time stamp of first api call
	uint32_t bindCnt[1];	// count of open api calls (handle binds)
	uint16_t nrandState[3];	// random number generator state
	uint16_t xtraSize;		// size of following structure
	uint16_t listIdx;		// arena free frames entry index
	uint16_t arrayIdx;		// arena handle array index
	uint8_t maxType[1];		// number of arena list entries
	uint8_t status[1];		// current status of the handle
	uint8_t hndlType;		// type of handle
	uint8_t relaxTs;
  };
  char filler[64];	// fill cache line
};

uint32_t disableHndls(DbAddr *hndlCalls);
uint64_t scanHandleTs(DbMap *map);

Handle *makeHandle(DbMap *map, uint32_t xtraSize, HandleType type);
void releaseHandle(Handle *hndl, DbHandle *dbHndl);
bool enterHandle(Handle *handle, DbAddr *slot);
Handle *bindHandle(DbHandle *dbHndl);
DbAddr *slotHandle(ObjId hndlId);

void destroyHandle(Handle *hndl, DbAddr *slot);
void *initHndlMap(char *path, int pathLen, char *name, int nameLen, bool onDisk, uint32_t arenaXtra);
