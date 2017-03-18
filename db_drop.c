#include "db.h"
#include "db_arena.h"
#include "db_map.h"
#include "db_object.h"
#include "db_handle.h"
#include "db_redblack.h"

extern DbAddr openMaps[1];
extern DbMap memMap[1];
extern DbMap *hndlMap;

// drop an arena, and recursively our children

void dropArenaDef(DbMap *db, RedBlack *entry, bool dropDefs, char *path, uint32_t pathLen) {
ArenaDef *arenaDef = (ArenaDef *)(entry + 1);
DbAddr *next, addr;
uint32_t len;
DbMap **map;
uint64_t id;

	len = addPath(path, pathLen, rbkey(entry), entry->keyLen, arenaDef->nxtVer);

	//  delete our id from db's child name & id list

	if (dropDefs) {
		writeLock(db->arenaDef->idList->lock);
		atomicOr8(arenaDef->dead, KILL_BIT);
		rbDel(db, db->arenaDef->nameTree, entry); 
		writeUnlock(db->arenaDef->idList->lock);
	}

	//	drop our children

	readLock(arenaDef->idList->lock);
	next = arenaDef->idList->head;

	while (next->addr) {
	  SkipNode *node = getObj(db, *next);
	  int idx;

	  for (idx = 0; idx < next->nslot; idx++) {
	  	addr.bits = *node->array[idx].val;
		entry = getObj(db, addr);

		dropArenaDef(db, entry, dropDefs, path, pathLen + len);
	  }

	  next = node->next;
	}

	//  wait for handles to exit

	readUnlock(arenaDef->idList->lock);
	disableHndls(db, arenaDef->hndlArray);

	//	close map if we are open in this process
	//	otherwise we can try to delete it

	map = arrayElement(memMap, openMaps, arenaDef->mapIdx, sizeof(void *));

	//	if arena file is not opened in this process
	//	we can unmap it now, and close handles in
	//	other processes.

	if (!*map) {
		deleteMap(path);
		path[pathLen] = 0;
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
uint64_t id = map->arenaDef->id;
SkipEntry *skipPayLoad;
char path[MAX_path];
RedBlack *entry;
DbAddr addr;

	//	are we already dropped?

	if (dropDefs)
	  if (atomicOr8(map->arenaDef->dead, KILL_BIT) & KILL_BIT)
		return DB_ERROR_arenadropped;

	atomicOr8((volatile char *)map->arena->mutex, KILL_BIT);

	//	remove from arenaDef from parent's idList

	writeLock(map->parent->arenaDef->idList->lock);
	addr.bits = skipDel(map->db, map->parent->arenaDef->idList->head, id); 
	entry = getObj(map->db, addr);

	//  delete our id from parent's child name & id list

	if (dropDefs) {
		atomicOr8(map->arenaDef->dead, KILL_BIT);
		rbDel(map->db, map->parent->arenaDef->nameTree, entry); 
	}

	if (*map->arena->type == Hndl_database)
		dropDefs = false;

	writeUnlock(map->parent->arenaDef->idList->lock);
	memcpy (path, map->arenaPath, map->pathLen);

	dropArenaDef(map->db, entry, dropDefs, path, map->pathLen);
	return DB_OK;
}
