#pragma once

//	iterator object

typedef enum {
	IterNone,
	IterLeftEof,
	IterRightEof,
	IterPosAt
} IterState;

typedef struct {
	uint64_t ts;		// iterator timestamp
	uint64_t ver;		// curent doc version
	ObjId txnId;
	ObjId docId;		// current ObjID
	Doc *doc;			// current doc version
	IterState state:8;
} Iterator;
