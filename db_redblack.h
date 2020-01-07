#pragma once

//	red-black tree descent stack

#define RB_bits		24

typedef struct {
	uint32_t lvl;			// height of the stack
	DbAddr entry[RB_bits];		// stacked tree nodes
} PathStk;

struct RedBlack_ {
	DbAddr left, right, addr;	// next nodes down, entry addr
	uint32_t payLoad;			// length of payload following
	uint16_t keyLen;			// length of key after payload
	uint8_t red;				// is tree node red?
};

#define rbkey(entry) ((char *)(entry + 1) + entry->payLoad)

RedBlack *rbFind(DbMap *parent, DbAddr *childNames, char *name, uint32_t nameLen, PathStk *path);
RedBlack *rbNew (DbMap *map, char *key, uint32_t keyLen, uint32_t payload);
RedBlack *rbStart(DbMap *map, PathStk *path, DbAddr *root); 
RedBlack *rbNext(DbMap *map, PathStk *path); 

void rbAdd(DbMap *map, DbAddr *root, RedBlack *entry, PathStk *path);
bool rbDel (DbMap *map, DbAddr *root, RedBlack *entry);
void rbKill (DbMap *map, DbAddr root);
