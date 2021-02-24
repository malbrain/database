#include <inttypes.h>

#include "btree2.h"
#include "btree2_slot.h"

//	create an empty page

uint64_t btree2NewPage (Handle *index, uint8_t lvl) {
DbMap *idxMap = MapAddr(index);
Btree2Index *btree2 = btree2index(idxMap);
Btree2PageType type;
Btree2Page *page;
uint32_t size;
DbAddr addr;

	size = btree2->pageSize;
	type = Btree2_interior;

	if (!lvl) {
		size <<= btree2->leafXtra;
		type = Btree2_leafPage;;
	}
  
	//  allocate page
  
	if ((addr.bits = allocObj(idxMap, listFree(index,type), NULL, type, size, true)))
		page = getObj(idxMap, addr);
	else
		return 0;
  
	page->alloc->nxt = (size >> btree2->skipBits) - 1;
	page->alloc->state = Btree2_pageactive;
	page->pageBits = btree2->pageBits;
	page->leafXtra = btree2->leafXtra;
	page->skipBits = btree2->skipBits;
	page->pageType = type;
    page->size = size;
	page->lvl = lvl;

 	return addr.bits;
}

//	initialize btree2 root page

extern uint32_t cursorSize[];

DbStatus btree2Init(Handle *index, Params *params) {
DbMap *idxMap = MapAddr(index);
Btree2Index *btree2 = btree2index(idxMap);
ObjId pageNo, *pageSlot;
Btree2Page *page;
DbAddr addr;

	if (params[Btree2Bits].intVal > Btree2_maxbits || params[Btree2Bits].intVal < Btree2_minbits ) {
		fprintf(stderr, "createIndex: bits = %" PRIu64 " > max = %d\n", params[Btree2Bits].intVal, Btree2_maxbits);
		exit(1);
	}

	if (params[Btree2Bits].intVal + params[Btree2Xtra].intVal > Btree2_maxbits || params[Btree2Bits].intVal < Btree2_minbits ) {
		fprintf(stderr, "createIndex: bits = %" PRIu64 " + xtra = %" PRIu64 " > max = %d\n", params[Btree2Bits].intVal, params[Btree2Xtra].intVal, Btree2_maxbits);
		exit(1);
	}

	btree2->pageSize = 1 << params[Btree2Bits].intVal;
	btree2->pageBits = (uint32_t)params[Btree2Bits].intVal;
	btree2->leafXtra = (uint32_t)params[Btree2Xtra].intVal;

	cursorSize[Hndl_btree2Index] = 1 << btree2->pageBits
                                          << btree2->leafXtra;
        //	initial btree2 root/leaf page

	if ((addr.bits = btree2NewPage(index, 0)))
		page = getObj(idxMap, addr);
	else
		return DB_ERROR_outofmemory;

	if ((pageNo.bits = btree2AllocPageNo(index)))
		pageSlot = fetchIdSlot(idxMap, pageNo);
	else
		return DB_ERROR_outofmemory;

	page->pageNo.bits = pageNo.bits;
	page->attributes = Btree2_rootPage;
	pageSlot->bits = addr.bits;

	btree2->root.bits = pageNo.bits;
	btree2->right.bits = pageNo.bits;
	btree2->left.bits = pageNo.bits;

	// release arena

	idxMap->arena->type[0] = Hndl_btree2Index;
	return DB_OK;
}

//	allocate btree2 pageNo

uint64_t btree2AllocPageNo(Handle *index) {
	return allocObjId(MapAddr(index), listFree(index, ObjIdType), listWait(index, ObjIdType));
}

bool btree2RecyclePageNo(Handle *index, ObjId pageNo) {
	return addSlotToFrame(MapAddr(index), listHead(index, ObjIdType), listWait(index, ObjIdType), pageNo.bits);
}

bool btree2RecyclePage(Handle *index, int type, DbAddr addr) {
	return addSlotToFrame(MapAddr(index), listHead(index, type), listWait(index, type), addr.bits);
}
