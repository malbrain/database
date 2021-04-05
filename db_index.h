#pragma once
//	Index data structure after DbArena object


//	Btree1Index global data on disk after Arena
//	Global Index data structure after DbArena object

typedef struct {
	uint64_t numKeys[1];  // number of keys in index
	DbAddr keySpec;
	bool binaryFlds;  // keys made with field values
	bool uniqueKeys;  // keys made with field values
} DbIndex;

