#pragma once

//	iterator object

typedef struct {
	ObjId docId;		// current ObjID
	uint32_t xtra;		// client area
	IterState state:8;
} Iterator;

void *iteratorSeek(Handle *itHndl, IteratorOp op, ObjId objId);
void *iteratorNext(Handle *itHndl);
void *iteratorPrev(Handle *itHndl);
