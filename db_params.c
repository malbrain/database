#include "db.h"
#include "db_object.h"
#include "db_redblack.h"
#include "db_arena.h"
#include "db_map.h"

extern DbMap *hndlMap;

//	if this is a new map file, copy param
//	structure to a new ArenaDef in parent
//	otherwise, return existing arenaDef
//	from the parent.

RedBlack *procParam(DbMap *parent, char *name, int nameLen, Params *params) {
SkipEntry *skipPayLoad;
PathStk pathStk[1];
ArenaDef *arenaDef;
RedBlack *rbEntry;
Catalog *catalog;
DbAddr *slot;

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

	// create new rbEntry in parent
	// with an arenaDef payload

	catalog = (Catalog *)(hndlMap->arena + 1);

	if ((rbEntry = rbNew(parent->db, name, nameLen, sizeof(ArenaDef)))) {
		arenaDef = (ArenaDef *)(rbEntry + 1);
	} else {
		unlockLatch(parent->arenaDef->nameTree->latch);
		return NULL;
	}

	//	fill in new arenaDef r/b entry

	time (&arenaDef->creation);
	memcpy (arenaDef->params, params, sizeof(arenaDef->params));

	//	allocate slot in parent's openMap array

	arenaDef->id = arrayAlloc(parent->db, parent->arenaDef->childList, sizeof(DbAddr));
	slot = arrayEntry(parent->db, parent->arenaDef->childList, arenaDef->id);
	slot->bits = rbEntry->addr.bits;

	//	add arenaDef to parent's child arenaDef by name tree

	rbAdd(parent->db, parent->arenaDef->nameTree, rbEntry, pathStk);
	unlockLatch(parent->arenaDef->nameTree->latch);
	return rbEntry;
}

