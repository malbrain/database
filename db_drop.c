#include "db.h"
#include "db_arena.h"
#include "db_map.h"
#include "db_object.h"
#include "db_handle.h"
#include "db_redblack.h"

extern DbAddr openMaps[1];
extern DbMap memMap[1];
extern DbMap *hndlMap;

// drop an arena given its r/b entry
//	and recursively its children

void dropArenaDef(DbMap *db, ArenaDef *parentDef, bool dropDefs, char *path, uint32_t pathLen) {
PathStk pathStk[1];
RedBlack *entry;
uint32_t len;
DbMap **map;
DbAddr addr;

	//	drop our children

	lockLatch (parentDef->nameTree->latch);

    if ((entry = rbStart (db, pathStk, parentDef->nameTree))) do {
      ArenaDef *arenaDef = (ArenaDef *)(entry + 1);
	  len = addPath(path, pathLen, rbkey(entry), entry->keyLen, arenaDef->nxtVer);

	  //  delete our name from parent's nameList

	  if (dropDefs) {
		atomicOr8(arenaDef->dead, KILL_BIT);
		rbDel(db, parentDef->nameTree, entry); 
	  }

	  dropArenaDef(db, arenaDef, dropDefs, path, pathLen + len);
    } while ((entry = rbNext(db, pathStk)));

    unlockLatch(parentDef->nameTree->latch);

	//  wait for handles to exit

	path[pathLen] = 0;
	disableHndls(db, parentDef->hndlArray);

	//	close map if we are open in this process
	//	otherwise we can try to delete it

	map = arrayElement(memMap, openMaps, parentDef->mapIdx, sizeof(void *));

	//	if arena file is not opened in this process
	//	we can unmap it now, and close handles in
	//	other processes.

	if (!*map) {
		deleteMap(path);
		return;
	}

	//	when all of our children are unmapped
	//	we can unmap ourselves

	atomicOr8((volatile char *)(*map)->arena->mutex, KILL_BIT);

	if (!(*map)->openCnt[0])
		closeMap(*map), *map = NULL;
}

//  drop an arena and all of its children
//	optionally, remove arenadef from parent childlist

DbStatus dropMap(DbMap *map, bool dropDefs) {
uint64_t id = map->arena->arenaDef->id;
SkipEntry *skipPayLoad;
char path[MAX_path];
RedBlack *entry;
DbMap *ourDb;
DbAddr addr;

	//  are we deleting a db from the catalog?

	if (*map->arena->type == Hndl_database)
		ourDb = map->parent;
	else
		ourDb = map->db;

	//	are we already dropped?

	if (dropDefs)
	  if (atomicOr8(map->arena->arenaDef->dead, KILL_BIT) & KILL_BIT)
		return DB_ERROR_arenadropped;

	atomicOr8((volatile char *)map->arena->mutex, KILL_BIT);

	//	remove id from parent's idList

	writeLock(map->parent->arena->arenaDef->idList->lock);
	addr.bits = skipDel(ourDb, map->parent->arena->arenaDef->idList->head, id); 
	entry = getObj(ourDb, addr);
	writeUnlock(map->parent->arena->arenaDef->idList->lock);

	//  delete our r/b entry from parent's child nameList

	if (dropDefs) {
		lockLatch (map->arena->arenaDef->nameTree->latch);
		atomicOr8(map->arena->arenaDef->dead, KILL_BIT);
		rbDel(ourDb, map->parent->arena->arenaDef->nameTree, entry); 
		unlockLatch (map->arena->arenaDef->nameTree->latch);
	}

	if (*map->arena->type == Hndl_database)
		dropDefs = false;

	memcpy (path, map->arenaPath, map->pathLen);

	dropArenaDef(map->db, map->arena->arenaDef, dropDefs, path, map->pathLen);
	return DB_OK;
}
