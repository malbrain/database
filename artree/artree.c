#include "artree.h"

//	initialize ARTree

DbStatus artInit(Handle *hndl, Params *params) {
ArtIndex *artIndex = artindex(hndl->map);

	artIndex->base->binaryFlds = hndl->map->arenaDef->params[IdxKeyFlds].charVal;
	artIndex->base->uniqueKeys = hndl->map->arenaDef->params[IdxKeyUnique].boolVal;

	hndl->map->arena->type[0] = Hndl_artIndex;
	return DB_OK;
}

