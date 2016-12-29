#pragma once

//	Database transactions: housed in database ObjId slots

typedef struct {
	uint64_t timestamp;	// txn timestamp, reader or writer
	DbAddr frame[1];	// contains versions in the TXN
	ObjId txnId;		// where we are stored.
} Txn;

//  txn command enum:

typedef enum {
	TxnAddDoc,	// add a new document
	TxnDelDoc,	// delete the document
	TxnUpdDoc	// add new version to document
} TxnCmd;
	
void addVerToTxn(DbMap *database, Txn *txn, Ver *ver, TxnCmd cmd);
Ver *findDocVer(DbMap *docStore, ObjId docId, Txn *txn);
