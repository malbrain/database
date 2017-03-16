#include "db.h"
#include "db_object.h"
#include "db_redblack.h"
#include "db_arena.h"
#include "db_map.h"

extern DbAddr openMaps[1];
extern DbMap *hndlMap;

//	if this is a new map file, copy param
//	structure to a new ArenaDef in parent
//	otherwise, return existing arenaDef
//	from the parent.

RedBlack *procParam(DbMap *parent, char *name, int nameLen, Params *params) {
ArenaDef *arenaDef = NULL;
SkipEntry *skipPayLoad;
PathStk pathStk[1];
RedBlack *rbEntry;
Catalog *catalog;

	//	see if ArenaDef already exists as a child in the parent

	while (true) {
	  lockLatch (parent->arenaDef->nameTree->latch);

	  if ((rbEntry = rbFind(parent->db, parent->arenaDef->nameTree, name, nameLen, pathStk))) {
		arenaDef = (ArenaDef *)(rbEntry + 1);

		if (*arenaDef->dead & KILL_BIT) {
		  unlockLatch (parent->arenaDef->nameTree->latch);
		  yield ();
		  continue;
		}

		unlockLatch (parent->arenaDef->nameTree->latch);
		return rbEntry;
	  }

	  break;
	}

	// create new rbEntry in parent?
	// with an arenaDef payload

	if (!arenaDef)
	  if ((rbEntry = rbNew(parent->db, name, nameLen, sizeof(ArenaDef))))
		arenaDef = (ArenaDef *)(rbEntry + 1);
	  else {
		unlockLatch(parent->arenaDef->nameTree->latch);
		return NULL;
	}

	memcpy (arenaDef->params, params, sizeof(arenaDef->params));

	catalog = (Catalog *)(hndlMap->arena + 1);

	//	the openMap assignment is for an open Map pointer in memMap

	arenaDef->mapIdx = arrayAlloc(hndlMap, catalog->openMap, sizeof(void *));
	arenaDef->id = atomicAdd64(&parent->arenaDef->childId, 1);
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

