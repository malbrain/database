#include "artree.h"

//	initialize ARTree

DbStatus artInit(Handle *hndl, Params *params) {
  DbMap *artMap = MapAddr(hndl);
  ArtIndex *artIndex = artindex(artMap);

	artIndex->dbIndex->delimFlds = artMap->arenaDef->params[IdxKeyFlds].charVal;
	artIndex->dbIndex->uniqueKeys =
    artMap->arenaDef->params[IdxKeyUnique].boolVal;

	artMap->arena->type[0] = Hndl_artIndex;
	return DB_OK;
}