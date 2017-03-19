#include "db.h"
#include "db_arena.h"
#include "db_map.h"
#include "db_object.h"
#include "db_handle.h"
#include "db_redblack.h"

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

	  atomicOr8(arenaDef->dead, KILL_BIT);

	  //  delete our name from parent's nameList

	  if (dropDefs)
		rbDel(db, parentDef->nameTree, entry); 

	  dropArenaDef(db, arenaDef, dropDefs, path, pathLen + len);
    } while ((entry = rbNext(db, pathStk)));

	path[pathLen] = 0;

    unlockLatch(parentDef->nameTree->latch);

	//  see if all handles have unbound

	lockLatch(parentDef->hndlArray->latch);

	if (!disableHndls(parentDef->hndlArray))
		deleteMap(path);

	unlockLatch(parentDef->hndlArray->latch);
}

//  drop an arena and all of its children
//	optionally, remove arenadef from parent childlist

DbStatus dropMap(DbMap *map, bool dropDefs) {
uint64_t id = map->arenaDef->id;
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

	if (atomicOr8(map->drop, KILL_BIT) & KILL_BIT)
		return DB_ERROR_arenadropped;

	if (dropDefs)
	  atomicOr8(map->arenaDef->dead, DEAD_BIT);

	atomicOr8((volatile char *)map->arena->mutex, KILL_BIT);

	//	remove id from parent's idList

	writeLock(map->parent->arenaDef->idList->lock);
	addr.bits = skipDel(ourDb, map->parent->arenaDef->idList->head, id); 
	entry = getObj(ourDb, addr);
	writeUnlock(map->parent->arenaDef->idList->lock);

	//  delete our r/b entry from parent's child nameList

	if (dropDefs) {
		lockLatch (map->arenaDef->nameTree->latch);
		atomicOr8(map->arenaDef->dead, KILL_BIT);
		rbDel(ourDb, map->parent->arenaDef->nameTree, entry); 
		unlockLatch (map->arenaDef->nameTree->latch);
	}

	if (*map->arena->type == Hndl_database)
		dropDefs = false;

	memcpy (path, map->arenaPath, map->pathLen);

	dropArenaDef(map->db, map->arenaDef, dropDefs, path, map->pathLen);

	//	when all of our children are unmapped
	//	we can unmap ourselves

	if (!map->openCnt[0])
		closeMap(map);

	return DB_OK;
}
