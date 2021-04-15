//  index key mgmt

#pragma once

//  version Keys stored in docStore
//	and inserted into the index

typedef struct {
  DbKeyDef key[1];
  uint32_t refCnt[1];
  uint16_t vecIdx;		// index in document key vector
  uint16_t suffix;      // number of suffix bytes
  uint64_t keyHash;     // used by MVCC if key changed
  ObjId payLoad;        // docId key comes from
} KeyValue;

uint64_t mvcc_allocDocStore(Handle* docHndl, uint32_t size, bool zeroit);
DbStatus mvcc_insertKeyValue(Handle *idxHndl, KeyValue *keyValue);