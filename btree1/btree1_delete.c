#include "../db.h"
#include "../db_map.h"
#include "../db_arena.h"
#include "../db_object.h"
#include "../db_handle.h"
#include "../db_index.h"
#include "btree1.h"

DbStatus btree1DeleteKey(Handle *index, void *key, uint32_t len) {
	return DB_OK;
}
