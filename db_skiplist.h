#include "db_lock.h"

/**
 *	skip lists
 */

//	skip list head

struct SkipHead_ {
	DbAddr head[1];		// list head
	RWLock lock[1];		// reader/writer lock
};

//	Skip list entry

typedef struct SkipEntry_ {
	uint64_t key[1];	// entry key
	uint64_t val[1];	// entry value
} SkipEntry;

//	size of skip list entry array

typedef struct {
	DbAddr next[1];		// next block of keys
	SkipEntry array[0];	// array of key/value pairs
} SkipNode;

int skipSearch(SkipEntry *array, int high, uint64_t key);
SkipEntry *skipFind(DbMap *map, DbAddr *skip, uint64_t key);
SkipEntry *skipPush(DbMap *map, DbAddr *skip, uint64_t key);
SkipEntry *skipAdd(DbMap *map, DbAddr *skip, uint64_t key);
uint64_t skipInit(DbMap *map, int numEntries);
uint64_t skipDel(DbMap *map, DbAddr *skip, uint64_t key);
DbStatus addItemToSkiplist(DbMap *map, DbAddr *skip, uint64_t key, uint64_t item);

