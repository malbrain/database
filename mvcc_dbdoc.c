// mvcc document implementation for database project

#include "mvcc_dbdoc.h"

//	insert an uncommitted document or an updated version into a docStore
//	if update, call with docId slot locked.

DbStatus mvcc_InsertDoc(Handle* docHndl, uint8_t* val, uint32_t docSize, uint64_t docBits, uint64_t keyBits, ObjId txnId, Ver* ver[2]) {
	uint32_t verSize, rawSize;
	ObjId docId, *docSlot;
	Ver* prevVer = ver[0];
	DbAddr docAddr;
	Doc* doc;

	//	assign a new docId slot if inserting

	if (!(docId.bits = docBits))
		docId.bits = allocObjId(docHndl->map, listFree(docHndl, 0), listWait(docHndl, 0));
	
	docSlot = fetchIdSlot(docHndl->map, docId);

	if (prevVer)
		rawSize = db_rawSize(docSlot->bits);
	else
		rawSize = docSize + sizeof(Doc) + sizeof(Ver) + offsetof(Ver, keys);

	if (rawSize < 12 * 1024 * 1024)
		rawSize += rawSize / 2;

	//	allocate space in docStore for the version

	if ((docAddr.bits = allocDocStore(docHndl, rawSize, false)))
		rawSize = db_rawSize(docAddr.bits);
	else
		return DB_ERROR_outofmemory;

	//	set up the document header

	doc = getObj(docHndl->map, docAddr);
	memset(doc, 0, sizeof(Doc));

	doc->prevAddr.bits = docSlot->bits;
	doc->ourAddr.bits = docAddr.bits;
	doc->docId.bits = docId.bits;
	doc->txnId.bits = txnId.bits;
	doc->op = prevVer ? Update : Insert;

	//	fill-in stopper (verSize == 0) at end of version stack

	verSize = sizeof(Ver) - offsetof(Ver, keys);
	verSize += 15;
	verSize &= -16;

	//  fill in stopper version

	ver[1] = (Ver*)((uint8_t*)doc + rawSize - verSize);
	ver[1]->offset = rawSize - verSize;
	ver[1]->verSize = 0;

	doc->lastVer = ver[1]->offset;

	verSize = sizeof(Ver) + docSize;
	verSize += 15;
	verSize &= -16;

	doc->lastVer -= verSize;
	assert(doc->lastVer >= sizeof(Doc));

	//	fill-in new version

	ver[1] = (Ver*)((uint8_t*)doc + doc->lastVer);
	memset(ver, 0, sizeof(Ver));

	ver[1]->verNo = prevVer ? prevVer->verNo : 1;

	if (prevVer && prevVer->commit)
		ver[1]->verNo++;

	ver[1]->keys->bits = keyBits;
	ver[1]->offset = doc->lastVer;
	ver[1]->verSize = verSize;

	//	install the document
	//	and return new version

	docSlot->bits = docAddr.bits;
	return DB_OK;
}

//	allocate docStore power-of-two memory

uint64_t allocDocStore(Handle* docHndl, uint32_t size, bool zeroit) {
	DbAddr* free = listFree(docHndl, 0);
	DbAddr* wait = listWait(docHndl, 0);

	return allocObj(docHndl->map, free, wait, -1, size, zeroit);
}

