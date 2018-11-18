#include <inttypes.h>

#include "btree2.h"

ObjId *btree2InitPage (Btree2Page *page, uint8_t lvl, uint32_t size) {
ObjId objId = allocObjId ();

  objId->bits = allocObjId(docHndl->map, listFree(docHndl,ObjIdType), listWait(docHndl,ObjIdType));
	page->lvl = lvl;
	page->min = size;
  return objId;
}

//	create an empty page
//  return logical page number

uint64_t btree2NewPage (Handle *hndl, uint8_t lvl) {
Btree2Index *btree2 = btree2index(hndl->map);
ObjId pageNo[1];
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
  
  pageNo->bits = allocObjId(hndl->map, listFree(docHndl,ObjIdType), listWait(docHndl,ObjIdType));

  //  allocate physical page
  
	if ((addr.bits = allocObj(hndl->map, listFree(hndl,type), NULL, type, size, true)))
		page = getObj(hndl->map, addr);
	else
		return 0;
  
objSlot = btree2InitPage(page, lvl, size);

	return pageNo->bits;
}

//	initialize btree2 root page

DbStatus btree2Init(Handle *hndl, Params *params) {
Btree2Index *btree2 = btree2index(hndl->map);
Btree2Page *page;
Btree2Slot *slot;
uint8_t *buff;

	if (params[Btree2Bits].intVal > Btree2_maxbits) {
		fprintf(stderr, "createIndex: bits = %" PRIu64 " > max = %d\n", params[Btree2Bits].intVal, Btree2_maxbits);
		exit(1);
	}

	if (params[Btree2Bits].intVal + params[Btree2Xtra].intVal > Btree2_maxbits) {
		fprintf(stderr, "createIndex: bits = %" PRIu64 " + xtra = %" PRIu64 " > max = %d\n", params[Btree2Bits].intVal, params[Btree2Xtra].intVal, Btree1_maxbits);
		exit(1);
	}

	btree2->pageSize = 1 << params[Btree2Bits].intVal;
	btree2->pageBits = (uint32_t)params[Btree2Bits].intVal;
	btree2->leafXtra = (uint32_t)params[Btree2Xtra].intVal;

	//	initial btree2 root & leaf pages

	if ((btree2->left.bits = btree2NewPage(hndl, 0)))
		page = getObj(hndl->map, btree2->left);
	else
		return DB_ERROR_outofmemory;

	//  set up new leaf page with stopper key

	btree2->left.type = Btree2_leafPage;
	btree2->right.bits = btree2->left.bits;

	page->min -= 1;
	page->cnt = 1;
	page->act = 1;

	buff = keyaddr(page, page->min);
	buff[0] = 0;

	//  set up stopper slot

	slot = slotptr(page, 1);
	slot->type = Btree2_stopper;
	slot->off = page->min;

	//	set  up the tree root page with stopper key

	if ((btree2->root.bits = btree2NewPage(hndl, 1)))
		page = getObj(hndl->map, btree2->root);
	else
		return DB_ERROR_outofmemory;

	//  set up new root page with stopper key

	btree2->root.type = Btree2_rootPage;
	page->min -= 1 + sizeof(uint64_t);
	page->cnt = 1;
	page->act = 1;

	//  set up stopper key

	buff = keyaddr(page, page->min);
	btree2PutPageNo(buff + 1, 0, btree2->left.bits);
	buff[0] = sizeof(uint64_t);

	//  set up slot

	slot = slotptr(page, 1);
	slot->type = Btree2_stopper;
	slot->off = page->min;

	hndl->map->arena->type[0] = Hndl_btree2Index;
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
