#pragma once

//	Handle for an opened arena
//	these instances live in hndlMap (Catalog)

//  the map instances opened for a process live in memMap
//  in the objId segmented array
// 
//	** marks fields that are
//	specific to the base arena

typedef struct {
    union {
      DbHandle hndl[1];
      ObjId hndlId;       // Handle Id in HndlMap (Catalog)
    };
    DbAddr mapAddr;       // addr for this map in memMaps
    DbAddr clientAddr;    // addr for client area in hndlMap
    uint64_t entryTs;     // time stamp of first api call
    uint32_t hndlIdx;     // catalog docstore index for this handle
    uint32_t clntSize;    // size of client area (iterator/cursor)
    uint32_t xtraSize;    // size of user work area after this Handle
    uint32_t bindCnt[1];  // count of open api calls (handle binds)
    uint16_t frameIdx;     // arena avail/empty frames entry index assigned
    uint16_t arrayIdx;    // arena open handle array index
    uint8_t maxType[1];   // number of arena free array frame list entries
    uint8_t status[1];    // current status of the handle
    uint8_t hndlType;     // type of handle
    uint8_t relaxTs;
    TriadQueue queue[32]; // working blks transferred back to areena
} Handle;

//  Handle status codes

typedef enum {
    HndlIdle = 0,
    HndlKill = 1,
} HndlCodes;

//	types of handles/arenas

typedef enum {
  Hndl_any = 0,
  Hndl_anyIdx,
  Hndl_catalog,
  Hndl_database,
  Hndl_docStore,
  Hndl_artIndex,
  Hndl_btree1Index,
  Hndl_btree2Index,
  Hndl_colIndex,
  Hndl_iterator,
  Hndl_cursor,
  Hndl_txns,
  Hndl_max
} HandleType;

uint32_t disableHndls(DbAddr *hndlCalls);
uint64_t scanHandleTs(DbMap *map);

Handle *makeHandle(DbMap *map, uint32_t clntSize, uint32_t cursorSize, HandleType type);
void releaseHandle(Handle *handle);
bool enterHandle(Handle *handle);
Handle *bindHandle(DbHandle dbHndl, HandleType type);

void destroyHandle(Handle *handle);
