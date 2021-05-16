#pragma once

//	iterator object
//	created in handle client area

typedef enum { IterNone, IterLeftEof, IterRightEof, IterPosAt } IterState;

typedef struct {
	ObjId docId;		// current ObjID
	IterState state;
} Iterator;

//	Iterator operations

typedef enum {
  IterNext = 'n',
  IterPrev = 'p',
  IterBegin = 'b',
  IterEnd = 'e',
  IterSeek = 's',
  IterFetch = 'f'
} IteratorOp;

DbStatus iteratorMove(DbHandle hndl, IteratorOp op, DocId *docId);

DbDoc *iteratorFetch(DbHandle hndl, ObjId docId);
DbDoc *iteratorSeek(DbHandle hndl, ObjId docId);
DbDoc *iteratorNext(DbHandle hndl);
DbDoc *iteratorPrev(DbHandle hndl);

DbStatus createIterator(DbHandle *hndl, DbHandle docHndl, Params *params);


