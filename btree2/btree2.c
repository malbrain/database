#include <inttypes.h>

#include "btree2.h"
#include "btree2_slot.h"

//	create an empty page
//  return logical page number

uint64_t btree2NewPage (Handle *index, uint8_t lvl, Btree2PageType type) {
Btree2Index *btree2 = btree2index(index->map);
Btree2Page *page;
uint32_t size;
DbAddr addr;

	size = btree2->pageSize;

	if (!lvl)
		size <<= btree2->leafXtra;
  
  //  allocate logical page number
  
	if ((addr.bits = allocObj(index->map, listFree(index,type), NULL, type, size, true)))
		page = getObj(index->map, addr);
	else
		return 0;
  
	size = (size >> btree2->skipBits) - 1;
	page->alloc->nxt = size;
	page->size = size;
	page->lvl = lvl;

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

	//	initial btree2 root/leaf page

	if ((addr.bits = btree2NewPage(index, 0, Btree2_leafPage)))
		page = getObj(index->map, addr);
	else
		return DB_ERROR_outofmemory;

	if ((pageNo.bits = btree2AllocPageNo(index)))
		pageSlot = fetchIdSlot(index->map, pageNo);
	else
		return DB_ERROR_outofmemory;

	page->attributes = Btree2_rootPage;
	pageSlot->bits = addr.bits;

	btree2->root.bits = pageNo.bits;
	btree2->right.bits = addr.bits;
	btree2->left.bits = addr.bits;

	// release arena

	index->map->arena->type[0] = Hndl_btree2Index;
	return DB_OK;
}

//	append page no at end of slot key

void btree2PutPageNo(Btree2Slot *slot, ObjId pageNo) {
uint8_t *key = slotkey(slot);
uint64_t bits = pageNo.bits;
int len = keylen(key);
int idx = 0;

	key += keypre(key);

	do key[len - ++idx] = (uint8_t)bits, bits >>= 8;
	while( idx < sizeof(uint64_t) );
}

uint64_t btree2GetPageNo(Btree2Slot *slot) {
uint8_t *key = slotkey(slot);
uint64_t result = 0;
int idx = 0, off;

	off = keylen(key) - sizeof(uint64_t);
	key += keypre(key);
	do result <<= 8, result |= key[off + idx];
	while (++idx < sizeof(uint64_t));

	return result;
}
