#include "artree.h"

//	initialize ARTree

DbStatus artInit(Handle *hndl, Params *params) {
ArtIndex *artIndex = (ArtIndex *)(hndl->map + 1);

	artIndex->base->binaryFlds = hndl->map->arenaDef->params[IdxKeyFlds].boolVal;

	hndl->map->arena->type[0] = Hndl_artIndex;
	return DB_OK;
}

