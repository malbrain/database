//Serial Safety Net

typedef enum  {
	SsnReaders,
	SsnCommit
} SsnValidate;

Ver *mvcc_getVersion(DbMap *dbMap, Doc *doc, uint64_t verNo);

DbStatus SSNScan1(Txn *txn);
DbStatus SSNScan2(Txn *txn);
DbStatus SSNScan3(Txn *txn);
DbStatus SSNScan4(Txn *txn);
DbStatus SSNScan5(Txn *txn);

DbMap* txnMap, memMap[1];
