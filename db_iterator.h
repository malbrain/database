#pragma once

//	iterator object

typedef enum {
	IterNone,
	IterLeftEof,
	IterRightEof,
	IterPosAt
} IterState;

typedef struct {
	Ver *ver;			// current doc version
	int64_t ts;			// iterator timestamp
	ObjId txnId;
	ObjId docId;		// current ObjID
	IterState state:8;
} Iterator;
