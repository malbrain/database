#include "artree.h"

#ifdef __linux__
#define offsetof(type,member) __builtin_offsetof(type,member)
#endif

DbStatus artInsertUniq( Handle *index, void *key, uint32_t keyLen, uint32_t keySuffix, UniqCbFcn *evalUniq) {
uint8_t area[sizeof(ArtCursor) + sizeof(DbCursor)];
volatile DbAddr *uniq, slot;
ARTKeyUniq *keyUniqNode;
DbCursor *dbCursor;
CursorStack* stack;
ArtCursor *cursor;
bool pass = false;
InsertParam p[1];
ArtIndex *artIdx;
DbIndex *dbIdx;

	dbIdx = (DbIndex *)(index->map->arena + 1);
	artIdx = (ArtIndex *)((uint8_t *)(dbIdx + 1) + dbIdx->xtra);

	memset(p, 0, sizeof(p));
	p->binaryFlds = index->map->arenaDef->params[IdxKeyFlds].boolVal;

	do {
		p->slot = artIdx->root;
		p->restart = false;
		p->keyLen = keyLen;
		p->index = index;
		p->fldLen = 0;
		p->key = key;
		p->off = 0;
	
		//  we encountered a dead node

		if (pass) {
			pass = false;
			yield();
		}

		if (!artInsertParam(p))
			continue;

		lockLatch(p->slot->latch);

		//	end the uniq portion of the path with a KeyUniq

		if (p->slot->type == KeyUniq) {
			keyUniqNode = getObj(index->map, *p->slot);
			slot.bits = p->slot->bits;
			break;
		}

		if ((slot.bits = artAllocateNode(index, KeyUniq, sizeof(ARTKeyUniq)))) {
			keyUniqNode = getObj(index->map, slot);
			keyUniqNode->next->bits = p->slot->bits & ~ADDR_MUTEX_SET;
			p->slot->bits = slot.bits | ADDR_MUTEX_SET;
			break;
		}

		unlockLatch(p->slot->latch);
		return DB_ERROR_outofmemory;
	} while (!p->stat && (pass = p->restart));

	if (p->stat)
		return p->stat;

	//	remember our locked KeyUniq node

	uniq = p->slot;

	//  prepare cursor to enumerate uniq keys

	dbCursor = (DbCursor *)(area);
	memset(dbCursor, 0, sizeof(DbCursor));

	dbCursor->xtra = sizeof(ArtCursor);
	dbCursor->state = CursorPosAt;

	cursor = (ArtCursor *)((char *)dbCursor + dbCursor->xtra);
	memset(cursor, 0, offsetof(ArtCursor, key));
	dbCursor->key = cursor->key;

	stack = &cursor->stack[cursor->depth++];
	stack->slot->bits = keyUniqNode->dups->bits;
	stack->addr = keyUniqNode->dups;
	stack->lastFld = 0;
	stack->off = 0;
	stack->ch = -1;

	if (keyUniqNode->dups->bits)
	 while (artNextKey(dbCursor, index->map) == DB_OK) {
	  if ((*evalUniq)(index, dbCursor)) {
		unlockLatch(p->slot->latch);
		return DB_ERROR_unique_key_violation;
	  }
	 }

	//  install the suffix key bytes

	p->keyLen += keySuffix;
	pass = false;

	do {
		p->slot = keyUniqNode->dups;
		p->off = keyLen;

		if (pass) {
			pass = false;
			yield();
		}

		if (!artInsertParam(p))
			continue;

		//	duplicate key?

		if (p->slot->type == KeyEnd)
		  break;

		//  if not, splice in a KeyEnd node to end the key

		lockLatch(p->slot->latch);

		//	check duplicate again after getting lock

		if (p->slot->type == KeyEnd) {
		  unlockLatch(p->slot->latch);
		  break;
		}

		//	end the key path with a zero-addr KeyEnd

		if (p->slot->type == UnusedSlot) {
		  p->slot->bits = (uint64_t)KeyEnd << TYPE_SHIFT;
		  break;
		}

		//	splice in a new KeyEnd node

		if ((slot.bits = artAllocateNode(index, KeyEnd, sizeof(ARTKeyEnd)))) {
		  ARTKeyEnd *keyEndNode = getObj(index->map, slot);
		  keyEndNode->next->bits = p->slot->bits & ~ADDR_MUTEX_SET;

		  p->slot->bits = slot.bits;
		  break;
		}

		unlockLatch(p->slot->latch);
		return DB_ERROR_outofmemory;
	} while (!p->stat && (pass = p->restart));

	unlockLatch(uniq->latch);
	return DB_OK;
}

