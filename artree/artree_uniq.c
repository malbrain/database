#include "artree.h"

#ifdef __linux__
#define offsetof(type,member) __builtin_offsetof(type,member)
#endif

bool evalUniq(DbMap *map, ARTKeyUniq *keyUniqNode, UniqCbFcn *evalFcn);

//  insert unique key
//	clear defer if unique

DbStatus artInsertUniq( Handle *index, void *key, uint32_t keyLen, uint32_t uniqueLen, UniqCbFcn *evalFcn, uint8_t *defer) {
volatile DbAddr *uniq, slot;
ARTKeyUniq *keyUniqNode;
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
		p->keyLen = uniqueLen;
		p->restart = false;
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

		//  latch the terminal node

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

	//	evaluate uniqueness violation

	if (!keyUniqNode->dups->bits)
	  *defer = false;				// no other keys
	else if (evalUniq(index->map, keyUniqNode, evalFcn))
	  *defer = false;				// no conflicting keys
	else if (!*defer)
	  return DB_ERROR_unique_key_constraint;

	//  install the suffix key bytes

	p->keyLen = keyLen;
	pass = false;

	do {
		p->slot = keyUniqNode->dups;
		p->off = uniqueLen;

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

DbStatus artEvalUniq( DbMap *map, void *key, uint32_t keyLen, uint32_t uniqueLen, UniqCbFcn *evalFcn) {
uint8_t area[sizeof(ArtCursor) + sizeof(DbCursor)];
bool pass = false, isDup;
ARTKeyUniq *keyUniqNode;
volatile DbAddr *uniq;
DbCursor *dbCursor;
CursorStack* stack;
ArtCursor *cursor;
ArtIndex *artIdx;
DbIndex *dbIdx;
DbStatus stat;

	dbIdx = (DbIndex *)(map->arena + 1);
	artIdx = (ArtIndex *)((uint8_t *)(dbIdx + 1) + dbIdx->xtra);

	dbCursor = (DbCursor *)(area);
	memset(dbCursor, 0, sizeof(DbCursor));

	dbCursor->xtra = sizeof(ArtCursor);
	dbCursor->state = CursorPosAt;

	cursor = (ArtCursor *)((char *)dbCursor + dbCursor->xtra);
	memset(cursor, 0, offsetof(ArtCursor, key));
	dbCursor->key = cursor->key;

	stack = &cursor->stack[cursor->depth++];
	stack->slot->bits = artIdx->root->bits;
	stack->addr = artIdx->root;
	stack->lastFld = 0;
	stack->off = 0;
	stack->ch = -1;

	if ((stat = artFindKey(dbCursor, map, key, keyLen, uniqueLen)))
		return stat;

	//  see if we ended up on the KeyUniq node

	stack = &cursor->stack[cursor->depth - 1];

	//  latch and remember the terminal node

	lockLatch(stack->addr->latch);
	uniq = stack->addr;

	if (stack->addr->type != KeyUniq) {
		unlockLatch(stack->addr->latch);
		return DB_OK;
	}

	//  reset cursor to enumerate duplicate keys

	keyUniqNode = getObj(map, *stack->addr);
	cursor->depth = 0;

	stack = &cursor->stack[cursor->depth++];
	stack->slot->bits = keyUniqNode->dups->bits;
	stack->addr = keyUniqNode->dups;
	stack->lastFld = 0;
	stack->off = 0;
	stack->ch = -1;

	isDup = false;

	while (artNextKey(dbCursor, map) == DB_OK)
	  if ((isDup = (*evalFcn)(map, dbCursor)))
		break;

	unlockLatch(uniq->latch);
	return isDup ? DB_ERROR_unique_key_constraint : DB_OK;
}

bool evalUniq(DbMap *map, ARTKeyUniq *keyUniqNode, UniqCbFcn *evalFcn) {
uint8_t area[sizeof(ArtCursor) + sizeof(DbCursor)];
DbCursor *dbCursor;
CursorStack* stack;
ArtCursor *cursor;

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

	while (artNextKey(dbCursor, map) == DB_OK)
	  if ((*evalFcn)(map, dbCursor))
		return false;

	return true;
}
