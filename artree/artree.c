#include "artree.h"

//	initialize ARTree

DbStatus artInit(Handle *hndl, Params *params) {

	hndl->map->arena->type[0] = Hndl_artIndex;
	return DB_OK;
}

