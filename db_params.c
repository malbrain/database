#include "db.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_map.h"

extern DbAddr openMaps[1];
extern DbMap memMap[1];
extern DbMap *hndlMap;

uint32_t sizeParam(Params *params) {
uint32_t size = 0;

	size += params[IdxKeySpecLen].intVal;
	size += params[IdxKeyPartialLen].intVal;
	return size;
}

//	save the param object in the database

void saveParam(DbMap *db, Params *arenaParams, Params *params) {
uint8_t *save = (uint8_t *)(arenaParams + 1);
uint32_t off = sizeof(ArenaDef);
int idx;

	for (idx = 0; idx < MaxParam; idx++)
	  switch (idx) {
		case IdxKeySpec:
		case IdxKeyPartial:
		  if (params[idx].obj) {
			uint32_t size = params[idx+1].intVal;
			memcpy(save + off, params[idx].obj, size);
			arenaParams[idx].offset = off;
			off += size;
		  }

		  continue;

		default:
		  arenaParams[idx] = params[idx];
		  continue;
	  }
}

//	if this is a new map file, process param
//	structure to a new ArenaDef in parent
//	otherwise, return existing arenaDef

RedBlack *procParam(DbMap *parent, char *name, int nameLen, Params *params) {
uint64_t *skipPayLoad;
ArenaDef *arenaDef;
PathStk pathStk[1];
RedBlack *rbEntry;
Catalog *catalog;
uint32_t xtra;

	lockLatch(parent->arenaDef->nameTree->latch);

	//	see if ArenaDef already exists as a child in the parent

	if ((rbEntry = rbFind(parent->db, parent->arenaDef->nameTree, name, nameLen, pathStk))) {
		unlockLatch(parent->arenaDef->nameTree->latch);
		return rbEntry;
	}

	// otherwise, create new rbEntry in parent
	// with an arenaDef payload

	xtra = sizeParam(params);

	if ((rbEntry = rbNew(parent->db, name, nameLen, sizeof(ArenaDef) + xtra)))
		arenaDef = (ArenaDef *)(rbEntry + 1);
	else {
		unlockLatch(parent->arenaDef->nameTree->latch);
		return NULL;
	}

	saveParam(parent->db, arenaDef->params, params);

	catalog = (Catalog *)(hndlMap->arena + 1);

	arenaDef->id = atomicAdd64(&parent->arenaDef->childId, 1);
	arenaDef->mapIdx = arrayAlloc(hndlMap, catalog->openMap, sizeof(void *));
	arenaDef->parentAddr.bits = parent->arena->redblack->bits;

	initLock(arenaDef->idList->lock);

	//	add arenaDef to parent's child arenaDef tree

	rbAdd(parent->db, parent->arenaDef->nameTree, rbEntry, pathStk);

	//	add new rbEntry to parent's child id array

	writeLock(parent->arenaDef->idList->lock);
	skipPayLoad = skipAdd (parent->db, parent->arenaDef->idList->head, arenaDef->id);
	*skipPayLoad = rbEntry->addr.bits;
	writeUnlock(parent->arenaDef->idList->lock);

	unlockLatch(parent->arenaDef->nameTree->latch);
	return rbEntry;
}

uint8_t *getObjParam(ArenaDef *arena, uint32_t idx) {
uint32_t off;

	if ((off = arena->params[idx].offset))
		return (uint8_t *)arena + off;

	return NULL;
}
