#include <inttypes.h>

#include "btree1.h"

void btree1InitPage (Btree1Page *page) {
	initLock(page->latch->readwr);
	initLock(page->latch->parent);
	initLock(page->latch->link);
}

//	create an empty page

uint64_t btree1NewPage (Handle *hndl, uint8_t lvl) {
Btree1Index *btree1 = btree1index(hndl->map);
Btree1PageType type;
Btree1Page *page;
uint32_t size;
DbAddr addr;

	size = btree1->pageSize;

	if (lvl)
		type = Btree1_interior;
	else {
		type = Btree1_leafPage;
		size <<= btree1->leafXtra;
	}

	if ((addr.bits = allocObj(hndl->map, listFree(hndl,type), NULL, type, size, true)))
		page = getObj(hndl->map, addr);
	else
		return 0;

	btree1InitPage(page);
	page->lvl = lvl;
	page->min = size;
	return addr.bits;
}

//	initialize btree1 root page

DbStatus btree1Init(Handle *hndl, Params *params) {
Btree1Index *btree1 = btree1index(hndl->map);
Btree1Page *page;
Btree1Slot *slot;
uint8_t *buff;
uint32_t amt;

	if (params[Btree1Bits].intVal > Btree1_maxbits) {
		fprintf(stderr, "createIndex: bits = %" PRIu64 " > max = %d\n", params[Btree1Bits].intVal, Btree1_maxbits);
		exit(1);
	}

	if (params[Btree1Bits].intVal + params[Btree1Xtra].intVal > Btree1_maxbits) {
		fprintf(stderr, "createIndex: bits = %" PRIu64 " + xtra = %" PRIu64 " > max = %d\n", params[Btree1Bits].intVal, params[Btree1Xtra].intVal, Btree1_maxbits);
		exit(1);
	}

	btree1->pageSize = 1 << params[Btree1Bits].intVal;
	btree1->pageBits = (uint32_t)params[Btree1Bits].intVal;
	btree1->leafXtra = (uint32_t)params[Btree1Xtra].intVal;

	//	initial btree1 root & leaf pages

	if ((btree1->left.bits = btree1NewPage(hndl, 0)))
		page = getObj(hndl->map, btree1->left);
	else
		return DB_ERROR_outofmemory;

	//  set up new leaf page with stopper key
	// of length zero

	btree1->left.type = Btree1_leafPage;
	btree1->right.bits = btree1->left.bits;

	page->min -= 1;

	//	set  up the tree root page with stopper key

	if ((btree1->root.bits = btree1NewPage(hndl, 1)))
		page = getObj(hndl->map, btree1->root);
	else
		return DB_ERROR_outofmemory;

	btree1->root.type = Btree1_rootPage;

	page->min -= Btree1_pagenobytes + 1;
	page->cnt = 1;
	page->act = 1;

	//  set up nil stopper key for leaf page

	buff = keyaddr(page, page->min);
	*buff++ = Btree1_pagenobytes;

	amt = store64(buff, 0, btree1->left.bits, false);

	while( amt < Btree1_pagenobytes )
		buff[amt++] = 0;

	//  set up slot

	slot = slotptr(page, 1);
	slot->type = Btree1_stopper;
	slot->off = page->min;

	hndl->map->arena->type[0] = Hndl_btree1Index;
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
