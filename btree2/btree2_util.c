#include "btree2.h"

//	debug slot function

#ifdef DEBUG
Btree2Slot *btree2Slot(Btree2Page *page, uint32_t off)
{
	return slotptr(page, off);
}

uint8_t *btree2Key(Btree2Page *page, Btree2Slot *slot)
{
	return keyaddr(page, slot);
}

#undef keyaddr
#undef slotptr
#define keyaddr(p,o) btree2Addr(p,o)
#define slotptr(p,x) btree2Slot(p,x)
#endif

//	allocate btree2 pageNo

uint64_t allocPageNo (Handle *index) {
	return allocObjId(index->map, listFree(index,ObjIdType), listWait(index,ObjIdType));
}

bool recyclePageNo (Handle *index, uint64_t bits) {
	 return addSlotToFrame(index->map, listHead(index,ObjIdType), listWait(index,ObjIdType), bits);
}

uint64_t allocPage(Handle *index, int type, uint32_t size) {
	return allocObj(index->map, listFree(index, type), listWait(index,type), type, size, false);
}

bool recyclePage(Handle *index, int type, uint64_t bits) {
	return addSlotToFrame(index->map, listHead(index,type), listWait(index,type), bits);
}

// split the root and raise the height of the btree2
// call with key for smaller half and right page addr and root statuslocked.

DbStatus btree2SplitRoot(Handle *index, Btree2Set *root, DbAddr right, uint8_t *leftKey) {
}

//split set->page splice pages left to right

DbStatus btree2SplitPage (Handle *index, Btree2Set *set) {
Btree2Index *btree2 = btree2index(index->map);
Btree2Page *leftPage, *rightPage;
uint8_t leftKey[Btree2_maxkey];
Btree2Slot *rSlot, *lSlot;
ObjId lPageNo, rPageNo;
uint16_t off, keyLen;
DbAddr left, right;
ObjId *pageNoPtr;
uint16_t *tower;
uint8_t *key;
DbStatus stat;

	if( (left.bits = btree2NewPage(index, set->page->lvl, set->page->pageType)) )
		leftPage = getObj(index->map, left);
	else
		return DB_ERROR_outofmemory;

	if( (right.bits = btree2NewPage(index, set->page->lvl, set->page->pageType)) )
		rightPage = getObj(index->map, right);
	else
		return DB_ERROR_outofmemory;

	//	copy over smaller first half keys from old page into new left page

	tower = set->page->skipHead;

	while( leftPage->nxt < leftPage->size / 2 )
		if( (off = tower[0]) ) {
			lSlot = slotptr(set->page,off);
			if( install8(lSlot->state, active, moved) == active )
				btree2InstallSlot(leftPage, lSlot);
			tower = lSlot->tower;
		} else
			break;

	//	splice pages together

	rightPage->left.bits = leftPage->pageNo.bits;
	leftPage->right.bits = rightPage->pageNo.bits;

	//	copy over remaining slots from old page into new right page

	while( rightPage->nxt < rightPage->size )
		if( (off = tower[0]) ) {
			rSlot = slotptr(set->page,off);
			if( install8(&rSlot->state, active, moved) == active )
				btree2InstallSlot(rightPage, rSlot);
			tower = rSlot->tower;
		} else
			break;

	//	allocate a new pageNo for left page,
	//	reuse existing pageNo for right page

	lPageNo.bits = allocObjId(index->map, btree2->freePage, NULL);
	pageNoPtr = fetchIdSlot (index->map, lPageNo);
	pageNoPtr->bits = left.bits;

	//	install left page addr into original pageNo slot

	pageNoPtr = fetchIdSlot(index->map, set->pageNo);
	pageNoPtr->bits = left.bits;

	//	install right page addr into right pageNo slot

	pageNoPtr = fetchIdSlot(index->map, rightPage->pageNo);
	pageNoPtr->bits = right.bits;

	//	extend left fence key with
	//	the left page number on non-leaf page.

	key = keyaddr(set->page, lSlot);
	keyLen = keylen(key);

	if( set->page->lvl)
		keyLen -= sizeof(uint64_t);		// strip off pageNo

	if( keyLen + sizeof(uint64_t) < 128 )
		off = 1;
	else
		off = 2;

	//	copy leftkey and add its pageNo

	memcpy (leftKey + off, key + keypre(key), keyLen);
	btree2PutPageNo(leftKey + off, keyLen, lPageNo.bits);
	keyLen += sizeof(uint64_t);

	//	insert key for left page in parent

	if( (stat = btree2InsertKey(index, leftKey, keyLen, set->page->lvl + 1, set->page->pageNo, active)) )
		return stat;

	//	install right page addr into original pageNo slot

	pageNoPtr = fetchIdSlot(index->map, set->pageNo);
	pageNoPtr->bits = right.bits;

	//	recycle original page to free list
	//	and pageNo

	if(!recyclePageNo(index, set->pageNo.bits))
		return DB_ERROR_outofmemory;

	if( !recyclePage(index, set->page->pageType, set->pageAddr.bits))
		return DB_ERROR_outofmemory;
				
	return DB_OK;
}

//	check page for space available,
//	clean if necessary and return
//	>0 number of skip units needed
//	=0 split required

DbStatus btree2CleanPage(Handle *index, Btree2Set *set, uint32_t totKeyLen) {
Btree2Index *btree2 = btree2index(index->map);
uint16_t skipUnit = 1 << set->page->skipBits;
uint32_t spaceReq, size = btree2->pageSize;
int type = set->page->pageType;
Btree2Page *newPage;
Btree2Page *clean;
Btree2Slot *slot;
uint8_t *key;
DbAddr addr;
ObjId *pageNoPtr;
uint16_t *tower, off;
DbStatus stat;


	if( !set->page->lvl )
		size <<= btree2->leafXtra;

	spaceReq = (sizeof(Btree2Slot) + set->height * sizeof(uint16_t) + totKeyLen + skipUnit - 1) / skipUnit;

	if( set->page->nxt + spaceReq <= size)
		return spaceReq;

	//	skip cleanup and proceed directly to split
	//	if there's not enough garbage
	//	to bother with.

	if( *set->page->garbage < size / 5 )
		return DB_BTREE_needssplit;

	if( (addr.bits = btree2NewPage(index, set->page->lvl, set->page->pageType)) )
		newPage = getObj(index->map, addr);
	else
		return DB_ERROR_outofmemory;

	//	copy over keys from old page into new page

	tower = set->page->skipHead;

	while( newPage->nxt < newPage->size / 2 )
		if( (off = tower[0]) ) {
			slot = slotptr(set->page,off);
			if( install8(slot->state, active, moved) == active )
				btree2InstallSlot(newPage, slot);
			tower = slot->tower;
		} else
			break;

	//	install new page addr into original pageNo slot

	pageNoPtr = fetchIdSlot(index->map, set->pageNo);
	pageNoPtr->bits = addr.bits;

	return DB_BTREE_needssplit;
}

//  compare two keys, return > 0, = 0, or < 0
//  =0: all key fields are same
//  -1: key2 > key1
//  +1: key2 < key1

int btree2KeyCmp (uint8_t *key1, uint8_t *key2, uint32_t len2)
{
uint32_t len1 = keylen(key1);
int ans;

	key1 += keypre(key1);

	if((ans = memcmp (key1, key2, len1 > len2 ? len2 : len1)))
		return ans;

	if( len1 > len2 )
		return 1;
	if( len1 < len2 )
		return -1;

	return 0;
}

//  find slot in page for given key
//	return true for exact match
//	return false otherwise

bool btree2FindSlot (Btree2Set *set, uint8_t *key, uint32_t keyLen)
{
uint16_t height = set->page->height, off;
uint16_t *tower = set->page->skipHead;
Btree2Slot *slot = NULL;
int result = 0;

	//	Starting at the head tower go down or right after each comparison

	while( height-- ) {
	  off = 0;			// left skipHead tower

	  do {
		set->prevSlot[height] = off;

		if( (off = tower[height]) )
			slot = slotptr(set->page, off);
		else
			break;

		result = btree2KeyCmp (keyaddr(set->page, slot), key, keyLen); 

		// go right at same height if key > slot

		if( result < 0 )
			tower = slot->tower;

	  } while( result < 0 );
	}

	return !result;
}

//  find and load page at given level for given key
//	leave page rd or wr locked as requested

DbStatus btree2LoadPage(DbMap *map, Btree2Set *set, uint8_t *key, uint32_t keyLen, uint8_t lvl) {
Btree2Index *btree2 = btree2index(map);
uint8_t drill = 0xff, *ptr;
Btree2Page *prevPage = NULL;
ObjId prevPageNo;
ObjId *pageNoPtr;

  set->pageNo.bits = btree2->root.bits;
  pageNoPtr = fetchIdSlot (map, set->pageNo);
  set->pageAddr.bits = pageNoPtr->bits;
  prevPageNo.bits = 0;

  //  start at our idea of the root level of the btree2 and drill down

  do {
	set->parent.bits = prevPageNo.bits;
	set->page = getObj(map, set->pageAddr);

//	if( set->page->free )
//		return DB_BTREE_error;

	assert(lvl <= set->page->lvl);

	//  find key on page at this level
	//  and descend to requested level

	if( !set->page->kill )
	 if( (btree2FindSlot (set->page, key, keyLen)) ) {
	  if( drill == lvl )
		return DB_OK;

	  // find next non-dead slot -- the fence key if nothing else

	  while( set->slot[lvl]->dead ) {
		if( off = set->tower[lvl] )
		  set->tower[lvl] = off;
		if( set->slotIdx++ < set->page->cnt )
		  continue;
		else
		  return DB_BTREE_error;

	  // get next page down

	  ptr = keyaddr(set->page, set->slotIdx);
	  set->pageNo.bits = btree2GetPageNo(ptr + keypre(ptr), keylen(ptr));

	  assert(drill > 0);
	  drill--;
	  continue;
	 }

	//  or slide right into next page

	set->pageNo.bits = set->page->right.bits;
  } while( set->pageNo.bits );

  // return error on end of right chain

  return DB_BTREE_error;
}

