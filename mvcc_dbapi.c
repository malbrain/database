// Define the document API for mvcc and ACID transactions
// implemented for the database project.

#include "mvcc_dbdoc.h"

DbStatus mvcc_dbdoc(void *body, uint32_t size, ObjId *docId, uint32_t verBase,
                    ObjId txnId, DbAddr keys) {
  return DB_OK;
}
