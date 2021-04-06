//	skip list implementation

#include "base64.h"
#include "db.h"
#include "db_arena.h"
#include "db_map.h"
#include "db_skiplist.h"

//	initialize initial skip list node

uint64_t skipInit(DbMap *map, int numEntries) {
  if (numEntries > 127)
	numEntries = 127;

  return allocBlk(map, numEntries * sizeof(SkipEntry) + sizeof(SkipNode), true);
}

//	find key value in skiplist, return entry address

SkipEntry *skipFind(DbMap *map, DbAddr *skip, uint64_t key) {
DbAddr *next = skip;
SkipNode *skipNode;
SkipEntry *entry;

  while (next->addr) {
	skipNode = getObj(map, *next);

	if (*skipNode->array[next->nslot-1].key >= key) {
	  entry = skipNode->array + skipSearch(skipNode->array, next->nslot-1, key);

	  if (*entry->key == key)
		return entry;

	  return NULL;
	}

	next = skipNode->next;
  }

  return NULL;
}

//	remove key from skip list
//	returning value from slot

uint64_t skipDel(DbMap *map, DbAddr *skip, uint64_t key) {
SkipNode *skipNode = NULL, *prevNode;
DbAddr *next = skip;
SkipEntry *entry;
uint64_t val;

  while (next->addr) {
	prevNode = skipNode;
	skipNode = getObj(map, *next);

	if (*skipNode->array[next->nslot-1].key >= key) {
	  entry = skipNode->array + skipSearch(skipNode->array, next->nslot-1, key);

	  if (*entry->key == key)
		val = *entry->val;
	  else
		return 0;

	  //  remove the entry slot

	  if (--next->nslot) {
		while (entry - skipNode->array < next->nslot) {
		  entry[0] = entry[1];
		  entry++;
		}

		return val;
	  }

	  //  skip list node is empty, remove it

	  if (prevNode)
		prevNode->next->bits = skipNode->next->bits;
	  else
		skip->bits = skipNode->next->bits;

	  freeBlk(map, *next);
	  return val;
	}

	next = skipNode->next;
  }

  return 0;
}

//	Push new maximal key onto head of skip list
//	return the value slot address

SkipEntry *skipPush(DbMap *map, DbAddr *skip, uint64_t key) {
SkipNode *skipNode;
SkipEntry *entry;
uint64_t next;

	if (!skip->addr || skip->nslot == skipSize(skip)) {
	  next = skip->bits;

	  skip->bits = allocBlk(map, SKIP_node * sizeof(SkipEntry) + sizeof(SkipNode), true) | ADDR_MUTEX_SET;
	  skipNode = getObj(map, *skip);
	  skipNode->next->bits = next;
	}
    else
	  skipNode = getObj(map, *skip);

	entry = skipNode->array + skip->nslot++;
	*entry->key = key;
	return entry;
}

//	add item to skiplist

DbStatus addItemToSkiplist(DbMap *map, DbAddr *skip, uint64_t key, uint64_t item) {
SkipEntry *entry;

	lockLatch(skip->latch);
	entry = skipAdd(map, skip, key);
	*entry->val = item;
	unlockLatch(skip->latch);
	return DB_OK;
}

//	Add arbitrary key to skip list
//	call with skip hdr locked
//	return entry address

SkipEntry *skipAdd(DbMap *map, DbAddr *skip, uint64_t key) {
SkipNode *skipNode = NULL, *nextNode;
DbAddr *next = skip;
uint64_t prevBits;
SkipEntry *entry;
int min, max;

  while (next->addr) {
	skipNode = getObj(map, *next);

	//  find skiplist node that covers key

	if (*skipNode->array[next->nslot-1].key < key)
	  if (skipNode->next->bits) {
		next = skipNode->next;
		continue;
	  }

	// we belong in this skipNode
	//  find the first entry .ge. to the new key

	min = skipSearch(skipNode->array, next->nslot, key);
	entry = skipNode->array + min;
	
	//  does key already exist?

	if (min < next->nslot && *entry->key == key)
		return entry;

	//  split node if already full

	if (next->nslot == skipSize(next)) {
	  prevBits = skipNode->next->bits;
	  skipNode->next->bits = allocBlk(map, SKIP_node * sizeof(SkipEntry) + sizeof(SkipNode), true);

	  nextNode = getObj(map, *skipNode->next);
	  nextNode->next->bits = prevBits;
	  memcpy(nextNode->array, skipNode->array + skipSize(skipNode->next) / 2, sizeof(skipNode->next) * (skipSize(skipNode->next) - skipSize(skipNode->next) / 2));

	  skipNode->next->nslot = skipSize(skipNode->next) - skipSize(skipNode->next) / 2;
	  next->nslot = skipSize(next) / 2;
	  continue;
	}

	//  insert new entry slot at min

	max = next->nslot++;

	while (--max > min)
	  skipNode->array[max + 1] = skipNode->array[max];

	//  fill in key and return value slot

	*skipNode->array[min].key = key;
	return skipNode->array + min;
  }

  // initialize empty list

  skip->bits = allocBlk(map, SKIP_node * sizeof(SkipEntry) + sizeof(SkipNode), true) | ADDR_MUTEX_SET;
  skipNode = getObj(map, *skip);

  *skipNode->array->key = key;
  skip->nslot = 1;

  return skipNode->array;
}

//	search Skip node for idx of key value slot
//	e.g. key value is <= entry[idx]

int skipSearch(SkipEntry *array, int high, uint64_t key) {
int low = 0;

	//  invariants:
	//	key <= entry[high]
	//	key > entry[low-1]

	while (high > low) {
		int diff = (high - 1 - low) / 2;

		if (key <= *array[low + diff].key)
			high = low + diff;
		else
			low += diff + 1;
	}

	return high;
}

