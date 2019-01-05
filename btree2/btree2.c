#include <inttypes.h>

#include "btree2.h"

//	create an empty page
//  return logical page number

uint64_t btree2NewPage (Handle *index, uint8_t lvl) {
Btree2Index *btree2 = btree2index(index->map);
Btree2PageType type;
Btree2Page *page;
uint32_t size;
DbAddr addr;

	size = btree2->pageSize;

	if (lvl)
		type = Btree2_interior;
	else {
		type = Btree2_leafPage;
		size <<= btree2->leafXtra;
	}
  
  //  allocate logical page number
  
	if ((addr.bits = allocObj(index->map, listFree(index,type), NULL, type, size, true)))
		page = getObj(index->map, addr);
	else
		return 0;
  
	page->lvl = lvl;
	page->min = size;
	return addr.bits;
}

//	initialize btree2 root page

DbStatus btree2Init(Handle *index, Params *params) {
Btree2Index *btree2 = btree2index(index->map);
ObjId pageNo, *pageSlot;
Btree2Page *page;
DbAddr addr;

	if (params[Btree2Bits].intVal > Btree2_maxbits) {
		fprintf(stderr, "createIndex: bits = %" PRIu64 " > max = %d\n", params[Btree2Bits].intVal, Btree2_maxbits);
		exit(1);
	}

	if (params[Btree2Bits].intVal + params[Btree2Xtra].intVal > Btree2_maxbits) {
		fprintf(stderr, "createIndex: bits = %" PRIu64 " + xtra = %" PRIu64 " > max = %d\n", params[Btree2Bits].intVal, params[Btree2Xtra].intVal, Btree2_maxbits);
		exit(1);
	}

	btree2->pageSize = 1 << params[Btree2Bits].intVal;
	btree2->pageBits = (uint32_t)params[Btree2Bits].intVal;
	btree2->leafXtra = (uint32_t)params[Btree2Xtra].intVal;

	//	initial btree2 root & leaf pages

	if ((addr.bits = btree2NewPage(index, 0)))
		page = getObj(index->map, addr);
	else
		return DB_ERROR_outofmemory;

	if ((pageNo.bits = allocObjId(index->map, btree2->pageNos, NULL)))
		pageSlot = fetchIdSlot(index->map, pageNo);
	else
		return DB_ERROR_outofmemory;

	pageSlot->bits = addr.bits;

	//  set up new leaf page with stopper key

	btree2->root.bits = pageNo.bits;
	btree2->right.bits = addr.bits;
	btree2->left.bits = addr.bits;

	//  set up new root page with stopper key

	index->map->arena->type[0] = Hndl_btree2Index;
	return DB_OK;
}

void btree2PutPageNo(uint8_t *key, uint32_t len, uint64_t bits) {
int idx = sizeof(uint64_t);

	while (idx--)
		key[len + idx] = (uint8_t)bits, bits >>= 8;
}

uint64_t btree2GetPageNo(uint8_t *key, uint32_t len) {
uint64_t result = 0;
int idx = 0;

	len -= sizeof(uint64_t);

	do result <<= 8, result |= key[len + idx];
	while (++idx < sizeof(uint64_t));

	return result;
}
