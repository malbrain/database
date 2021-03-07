#include "btree1.h"

extern uint32_t cursorSize[];

void btree1SlotClr(Btree1Page *page, int idx) { 
Btree1Slot *slot = (Btree1Slot *)(page + 1) + idx;

	slot->bits[0] = 0;
	slot->bits[1] = 0;
}

uint32_t btree1SlotMax(Btree1Page *page) {
uint32_t off = page->cnt * sizeof(Btree1Slot);

	return off + sizeof(Btree1Page);
}

void btree1InitPage(Btree1Page *page, Btree1PageType type) {
	initLock(page->latch->readwr);
	initLock(page->latch->parent);
	initLock(page->latch->link);
	page->type = type;
}

//	allocate btree1 pageId
//	create an empty page

Btree1Page *btree1NewPage (Handle *hndl, uint8_t lvl, Btree1PageType type) {
DbMap * idxMap = MapAddr(hndl);
Btree1Index *btree1 = btree1index(idxMap);
Btree1Page *page;
PageId pageId;
uint32_t size;
DbAddr *pageAddr;

	size = btree1->pageSize;

	if (type == Btree1_leafPage )
		size <<= btree1->leafXtra;

	if( pageId.bits = allocObjId(idxMap, listFree(hndl,    ObjIdType), listWait(hndl, ObjIdType)) )
		pageAddr = fetchIdSlot(idxMap, pageId);
	else
	  return 0;

	if ((pageAddr->bits = allocObj(idxMap, listFree(hndl,type), NULL, type, size, true ) ))
	  page = getObj(idxMap, *pageAddr);
	else
	  return 0;

	btree1InitPage(page, type);
	page->self.bits = pageAddr->bits;
	page->min = size;
	page->lvl = lvl;

	return page;
}

//	initialize btree1 root and first page

DbStatus btree1Init(Handle *hndl, Params *params) {
DbMap *idxMap = MapAddr(hndl);
Btree1Index *btree1 = btree1index(idxMap);
Btree1Page *page;
Btree1Page *root;
Btree1Slot *slot;

	if (params[Btree1Bits].intVal > Btree1_maxbits) {
		fprintf(stderr, "createIndex: bits = %" PRIu64 " > max = %d\n", params[Btree1Bits].intVal, Btree1_maxbits);
		exit(1);
	}

	if (params[Btree1Bits].intVal < Btree1_minbits) {
          fprintf(stderr, "createIndex: bits = %" PRIu64 " < min = %d\n",
                  params[Btree1Bits].intVal, Btree1_minbits);
          exit(1);
        }

        if (params[Btree1Bits].intVal + params[Btree1Xtra].intVal >
            Btree1_maxbits) {
		fprintf(stderr, "createIndex: bits = %" PRIu64 " + xtra = %" PRIu64" > max = %d\n", params[Btree1Bits].intVal, params[Btree1Xtra].intVal, Btree1_maxbits);		exit(1);
	}

	btree1->pageSize = 1 << params[Btree1Bits].intVal;
	btree1->pageBits = (uint32_t)params[Btree1Bits].intVal;
	btree1->leafXtra = (uint32_t)params[Btree1Xtra].intVal;

	cursorSize[Hndl_btree1Index] += 1 << btree1->pageBits << btree1->leafXtra;

	//	initial btree1 root & 
	//	right leaf pages

	if ((page = btree1NewPage(hndl, 0, Btree1_leafPage)))
		btree1->left = btree1->right = page->self;
	else
		return DB_ERROR_outofmemory;

	//	set  up the tree root page with stopper key

	if ((root = btree1NewPage(hndl, 1, Btree1_rootPage)))
		btree1->root = root->self;
	else
		return DB_ERROR_outofmemory;

	//  set up nil root stopper key for leaf page

	page->cnt = 1;
	page->act = 1;

	slot = slotptr(root, 1);
	slot->bits[0] = 0;
	slot->type = Btree1_stopper;
	slot->childId = page->self;

	// release index arena lock

	idxMap->arena->type[0] = Hndl_btree1Index;
	return DB_OK;
}

// place write, read, or parent lock on requested page_no.

void btree1LockPage(Btree1Page *page, Btree1Lock mode) {
	switch( mode ) {
	case Btree1_lockRead:
		readLock (page->latch->readwr);
		break;
	case Btree1_lockWrite:
		writeLock (page->latch->readwr);
		break;
	case Btree1_lockParent:
		writeLock (page->latch->parent);
		break;
	case Btree1_lockLink:
		writeLock (page->latch->link);
		break;
	}
}

void btree1UnlockPage(Btree1Page *page, Btree1Lock mode)
{
	switch( mode ) {
	case Btree1_lockWrite:
		writeUnlock (page->latch->readwr);
		break;
	case Btree1_lockRead:
		readUnlock (page->latch->readwr);
		break;
	case Btree1_lockParent:
		writeUnlock (page->latch->parent);
		break;
	case Btree1_lockLink:
		writeUnlock (page->latch->link);
		break;
	}
}