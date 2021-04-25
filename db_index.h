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

typedef struct {
  union {
    uint8_t *keyBuff;
    DbAddr bytes;
  };
  uint16_t keyLen; 	    // len of entire key
  uint8_t suffixLen; 	// len of payload key at end
  uint8_t unique : 1;   // index is unique
  uint8_t deferred : 1;	// uniqueness deferred
  uint8_t binaryKeys : 1;	// use key fields with binary comparisons
} DbKeyValue;

