#pragma once

//	iterator object

typedef struct {
	ObjId docId;		// current ObjID
	uint32_t xtra;		// client area
	IterState state:8;
} Iterator;

DbAddr *iteratorSeek(Handle *itHndl, IteratorOp op, ObjId objId);
DbAddr *iteratorNext(Handle *itHndl);
DbAddr *iteratorPrev(Handle *itHndl);
