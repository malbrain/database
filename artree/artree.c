#include "artree.h"

//	initialize ARTree

DbStatus artInit(Handle *hndl, Params *params) {
  DbMap *artMap = MapAddr(hndl);
  ArtIndex *artIndex = artindex(artMap);

	artIndex->base->binaryFlds = artMap->arenaDef->params[IdxKeyFlds].charVal;
	artIndex->base->uniqueKeys = artMap->arenaDef->params[IdxKeyUnique].boolVal;

	artMap->arena->type[0] = Hndl_artIndex;
	return DB_OK;
}