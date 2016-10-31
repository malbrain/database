#include "../db.h"
#include "../db_object.h"
#include "../db_handle.h"
#include "../db_index.h"
#include "../db_arena.h"
#include "../db_map.h"
#include "artree.h"

//	initialize ARTree

DbStatus artInit(Handle *hndl, Params *params) {

	hndl->map->arena->type[0] = Hndl_artIndex;
	return DB_OK;
}

