#pragma once

//	iterator object
//	created in handle client area

typedef struct {
	ObjId docId;		// current ObjID
	IterState state;
} Iterator;

DbAddr *iteratorSeek(Handle *itHndl, IteratorOp op, ObjId objId);
DbAddr *iteratorNext(Handle *itHndl);
DbAddr *iteratorPrev(Handle *itHndl);
