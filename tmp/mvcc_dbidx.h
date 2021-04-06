//  index key mgmt

#pragma once

//  version Keys stored in docStore
//	and inserted into the index

typedef struct {
	uint32_t refCnt[1];
	uint32_t keyHash;
	uint16_t keyLen; 	    // len of base key
	uint16_t vecIdx;		// index in key vector
	uint8_t unique:1;		// index is unique
	uint8_t deferred:1;		// uniqueness deferred
	uint8_t binaryKeys:1;	// uniqueness deferred
	uint8_t suffixLen;		// size of docId suffix
	uint8_t bytes[];		// bytes of the key
} KeyValue;

uint64_t allocDocStore(Handle* docHndl, uint32_t size, bool zeroit);
extern Handle** bindDocIndexes(Handle* docHndl);
DbStatus installKeys(Handle* idxHndls[1], Ver* ver);
DbStatus removeKeys(Handle* idxHndls[1], Ver* ver, DbMmbr* mmbr, DbAddr* slot);
