// mvcc transactions

#pragma once

#include "base64.h"
#include "db.h"
#include "db_api.h"
#include "Hi-Performance-Timestamps/timestamps.h"

// MVCC and TXN definitions for DATABASE project

DbMap* txnMap, memMap[1];

DbStatus mvcc_scan1(Txn* txn);
DbStatus mvcc_scan2(Txn* txn);
DbStatus mvcc_scan3(Txn* txn);
DbStatus mvcc_scan4(Txn* txn);