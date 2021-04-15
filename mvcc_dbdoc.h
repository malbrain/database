#pragma once

typedef uint32_t (*FillFcn)(uint8_t* rec, uint32_t size, void *fillOpt);
typedef int (*Intfcnp)();




MVCCResult mvcc_findCursorVer(DbCursor* dbCursor, DbMap* map, DbMvcc* dbMvcc, Ver* ver);
MVCCResult mvcc_addDocRdToTxn(Txn* txn, Handle *docHndl, Ver* ver);
MVCCResult mvcc_addDocWrToTxn(Txn* txn, Handle *docHndl, Doc* doc);

Ver * mvcc_getVersion(DbMap *map, Doc *doc, uint64_t verNo);
uint64_t mvcc_allocDocStore(Handle* docHndl, uint32_t size, bool zeroit);
DbStatus mvcc_installKeys(Handle* idxHndls[1], Ver* ver);
DbStatus mvcc_removeKeys(Handle* idxHndls[1], Ver* ver, DbMmbr* mmbr, DbAddr* slot);
