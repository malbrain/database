#pragma once

//	red-black tree descent stack

#define RB_bits		24

typedef struct {
	uint64_t lvl;			// height of the stack
	DbAddr entry[RB_bits];	// stacked tree nodes
} PathStk;

typedef struct RedBlack_ {
	DbAddr left, right;		// next nodes down
	DbAddr payLoad;			// length of payload after key
	DbAddr addr;			// this entry addr in map
	uint32_t keyLen;		// length of key after entry
	char latch[1];			// this entry latch
	char red;				// is tree node red?
} RedBlack;

#define rbKey(entry) ((char *)(entry + 1))

typedef DbStatus (*RbFcnPtr)(DbMap *map, RedBlack *entry, void *params);

DbStatus rbList(DbMap *map, DbAddr *root, RbFcnPtr fcn, void *key, uint32_t keyLen, void *params);
RedBlack *rbFind(DbMap *parent, DbAddr *childNames, char *name, uint32_t nameLen, PathStk *path);
RedBlack *rbNew (DbMap *map, void *key, uint32_t keyLen, uint32_t payload);
RedBlack *rbNext(DbMap *map, PathStk *path); 

void rbAdd(DbMap *map, DbAddr *root, RedBlack *entry, PathStk *path);
bool rbDel (DbMap *map, DbAddr *root, void *key, uint32_t keylen);

