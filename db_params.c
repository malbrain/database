#include "db.h"
#include "db_object.h"
#include "db_redblack.h"
#include "db_arena.h"
#include "db_map.h"

extern DbAddr openMaps[1];
extern DbMap memMap[1];
extern DbMap *hndlMap;

//	if this is a new map file, copy param
//	structure to a new ArenaDef in parent
//	otherwise, return existing arenaDef
//	from the parent.

RedBlack *procParam(DbMap *parent, char *name, int nameLen, Params *params) {
SkipEntry *skipPayLoad;
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

	xtra = params[Size].intVal;

	if (!xtra)
		xtra = sizeof(Params) * (MaxParam + 1);

	if ((rbEntry = rbNew(parent->db, name, nameLen, sizeof(ArenaDef) + xtra)))
		arenaDef = (ArenaDef *)(rbEntry + 1);
	else {
		unlockLatch(parent->arenaDef->nameTree->latch);
		return NULL;
	}

	memcpy (arenaDef->params, params, xtra);

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
	*skipPayLoad->val = rbEntry->addr.bits;
	writeUnlock(parent->arenaDef->idList->lock);

	unlockLatch(parent->arenaDef->nameTree->latch);
	return rbEntry;
}

void *getParamOff(Params *params, uint32_t off) {
	return (char *)params + off;
}

void *getParamIdx(Params *params, uint32_t idx) {
uint32_t off;

	if ((off = params[idx].offset))
		return (char *)params + off;

	return NULL;
}
