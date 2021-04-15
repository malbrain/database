//Serial Safety Net

typedef enum  {
	SsnReaders,
	SsnCommit
} SsnValidate;

Ver *mvcc_getVersion(DbMap *dbMap, Doc *doc, uint64_t verNo);

DbStatus mvcc_scan1(Txn *txn);
DbStatus mvcc_scan2(Txn *txn);
DbStatus mvcc_scan3(Txn *txn);
DbStatus mvcc_scan4(Txn *txn);

DbMap* txnMap, memMap[1];
