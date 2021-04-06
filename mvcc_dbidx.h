//  index key mgmt

#pragma once

//  version Keys stored in docStore
//	and inserted into the index


uint64_t mvcc_allocDocStore(Handle* docHndl, uint32_t size, bool zeroit);
DbStatus insertKeyValue(Handle *idxHndl, KeyValue *keyValue);
