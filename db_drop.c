#include "db.h"
#include "db_arena.h"
#include "db_map.h"
#include "db_api.h"
#include "db_object.h"
#include "db_handle.h"
#include "db_redblack.h"
#include "db_skiplist.h"

DbMap memMap[1];

// drop an arena given its r/b entry
//	and recursively its children

void dropArenaDef(DbMap *db, ArenaDef *arenaDef, bool dropDefs, char *path, uint32_t pathLen) {
uint32_t len, count;
PathStk pathStk[1];
RedBlack *entry;

	//	drop our children

	lockLatch (arenaDef->nameTree->latch);

	//	enumerate child nameTree

    if ((entry = rbStart (db, pathStk, arenaDef->nameTree))) do {
      ArenaDef *childDef = (ArenaDef *)(entry + 1);
	  len = addPath(path, pathLen, rbkey(entry), entry->keyLen, childDef->nxtVer);

	  atomicOr8(childDef->dead, KILL_BIT);

	  //  delete our name from parent's nameList

	  if (dropDefs)
		rbDel(db, arenaDef->nameTree, entry); 

	  dropArenaDef(db, childDef, dropDefs, path, pathLen + len);
    } while ((entry = rbNext(db, pathStk)));

	path[pathLen] = 0;

    unlockLatch(arenaDef->nameTree->latch);

	//  see if all handles have unbound

	lockLatch(arenaDef->hndlArray->latch);
	count = disableHndls(arenaDef->hndlArray);
	unlockLatch(arenaDef->hndlArray->latch);

	if (!count)
		deleteMap(path);
}

//  drop an arena and all of its children
//	optionally, remove arenadef from parent childlist

DbStatus dropMap(DbMap *map, bool dropDefs) {
uint64_t id = map->arenaDef->id;
char path[MAX_path];
DbMap *ourDb;

	//  are we deleting a db from the catalog?

	if (*map->arena->type == Hndl_database) {
		ourDb = map->parent;
		dropDefs = false;
	} else
		ourDb = map->db;

	//	are we already dropped?

	if (atomicOr8(map->drop, KILL_BIT) & KILL_BIT)
		return DB_ERROR_arenadropped;

	atomicOr8((volatile uint8_t *)map->arena->mutex, KILL_BIT);

	//	remove id from parent's childMap list

	lockLatch(map->parent->childMaps->latch);
	skipDel(memMap, map->parent->childMaps, id);
	unlockLatch(map->parent->childMaps->latch);

	//  delete our r/b entry from parent's child nameList
	//	or kill our name tree from our surviving arenaDef

	if (dropDefs) {
		lockLatch (map->parent->arenaDef->nameTree->latch);
		atomicOr8(map->arenaDef->dead, KILL_BIT);
		rbDel(ourDb, map->parent->arenaDef->nameTree, map->rbEntry); 
		unlockLatch (map->parent->arenaDef->nameTree->latch);
	}

	memcpy (path, map->arenaPath, map->pathLen);

	dropArenaDef(map->db, map->arenaDef, dropDefs, path, map->pathLen);

	//	when all of our children are unmapped
	//	we can unmap ourselves

	if (!*map->openCnt)
		closeMap(map);

	return DB_OK;
}
