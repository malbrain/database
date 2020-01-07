//  index key mgmt

#pragma once

//	maximum number of nested array fields in an index key

#define MAX_array_fields 8
#define INT_key 12		// max extra bytes store64 creates

typedef enum {
	val_undef = 0,
	val_char,
	val_short,
	val_int,
	val_long,
	val_float,
	val_dbl,
	val_string,
	val_bool
} ValueType;

typedef union {
	char	charVal;
	bool	boolVal;
	short	shortVal;
	int		intVal;
	int64_t	longVal;
	float	floatVal;
	double	dblVal;
	char	strVal[1];
} KeyVal;

typedef enum {
	key_undef = 0,
	key_bool,
	key_char,
	key_int,
	key_dbl,
	key_str,
	key_mask = 7,
	key_first = 8,
	key_reverse = 16
} KeyFieldType;

typedef struct {
	KeyFieldType fldType;	// type of field
	uint16_t fldLen;		// fixed length, zero for data dep
	uint16_t len;			// length of field descriptor
	uint8_t descr[];		// field description
} IndexKeyField;

//	Key field specifications
//	field descrption VLA array
//	field description bytes follow

typedef struct {
	uint8_t numFlds;		// number of fields present
	uint16_t nxtfld;		// offset of next field
	DbAddr addr[1];			// addr in docStore
} IndexKeySpec;

//  version Keys stored in docStore
//	and inserted into the index

typedef struct {
	uint64_t refCnt[1];
	uint32_t keyHash;
	uint16_t keyLen;    // len of base key
	uint16_t keyIdx;	// idxHndls vector idx
	uint8_t suffixLen;	// size of key suffix extension
	uint8_t unique;		// index is unique
	uint8_t deferred;	// uniqueness deferred
	uint8_t bytes[];	// bytes of the key
} IndexKeyValue;

typedef struct {
	uint16_t keyLen;	// size of key at this step
	uint16_t off;		// offset of the IndexKey
	uint16_t cnt;		// size of array
	uint16_t idx;		// next idx
	void* values;	// array vals
} KeyStack;

DbStatus addKeyField(DbHandle* idxHndl, IndexKeySpec* spec, struct Field* field);
void buildKeys(Handle* idxHndls[1], uint16_t keyIdx, DbAddr* keys, ObjId docId, Ver* prevVer, uint32_t idxCnt, void *cbEnv);
uint16_t appendKeyField(Handle* idxHndls, IndexKeySpec* spec, struct Field* field, uint8_t* keyDest, uint16_t keyRoom, void *cbEnv);

uint64_t allocDocStore(Handle* docHndl, uint32_t size, bool zeroit);
extern Handle** bindDocIndexes(Handle* docHndl);
DbStatus installKeys(Handle* idxHndls[1], Ver* ver);
DbStatus removeKeys(Handle* idxHndls[1], Ver* ver, DbMmbr* mmbr, DbAddr* slot);
