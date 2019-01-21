#include "btree1.h"

DbStatus btree1DeleteKey(Handle *index, void *key, uint32_t len) {
	return DB_OK;
}

// todo: adapt btree-source-code btree2 delete code below

#if 0
//  find and delete key on page by marking delete flag bit
//  if page becomes empty, delete it from the btree

BTERR bt_deletekey (BtDb *bt, unsigned char *key, uint len, uint lvl)
{
unsigned char lowerfence[256], higherfence[256];
uint slot, idx, dirty = 0, fence, found;
BtPageSet set[1], right[1];
BtKey ptr;

	if( slot = bt_loadpage (bt, set, key, len, lvl, BtLockParentWrt) )
		ptr = keyptr(set->page, slot);
	else
		return bt->err;

	//	are we deleting a fence slot?

	fence = slot == set->page->cnt;

	// if key is found delete it, otherwise ignore request

	if( found = !keycmp (ptr, key, len) )
	  if( found = slotptr(set->page, slot)->dead == 0 ) {
		dirty = slotptr(set->page, slot)->dead = 1;
 		set->page->dirty = 1;
 		set->page->act--;

		// collapse empty slots

		while( idx = set->page->cnt - 1 )
		  if( slotptr(set->page, idx)->dead ) {
			*slotptr(set->page, idx) = *slotptr(set->page, idx + 1);
			memset (slotptr(set->page, set->page->cnt--), 0, sizeof(BtSlot));
		  } else
			break;
	  }

	//	did we delete a fence key in an upper level?

	if( dirty && lvl && set->page->act && fence )
	  if( bt_fixfence (bt, set, lvl) )
		return bt->err;
	  else
		return bt->found = found, 0;

	//	is this a collapsed root?

	if( lvl > 1 && set->page_no == ROOT_page && set->page->act == 1 )
	  if( bt_collapseroot (bt, set) )
		return bt->err;
	  else
		return bt->found = found, 0;

	//	return if page is not empty

 	if( set->page->act ) {
		bt_unlockpage(BtLockParentWrt, set->latch);
		bt_unpinlatch (set->latch);
		bt_unpinpool (set->pool);
		return bt->found = found, 0;
	}

	//	cache copy of fence key
	//	to post in parent

	ptr = keyptr(set->page, set->page->cnt);
	memcpy (lowerfence, ptr, ptr->len + 1);

	//	obtain lock on right page

	right->page_no = bt_getid(set->page->right);
	right->latch = bt_pinlatch (bt, right->page_no);
	bt_lockpage (BtLockParentWrt, right->latch);

	// pin page contents

	if( right->pool = bt_pinpool (bt, right->page_no) )
		right->page = bt_page (bt, right->pool, right->page_no);
	else
		return 0;

	if( right->page->kill )
		return bt->err = BTERR_struct;

	// pull contents of right peer into our empty page

	memcpy (set->page, right->page, bt->mgr->page_size);

	// cache copy of key to update

	ptr = keyptr(right->page, right->page->cnt);
	memcpy (higherfence, ptr, ptr->len + 1);

	// mark right page deleted and point it to left page
	//	until we can post parent updates

	bt_putid (right->page->right, set->page_no);
	right->page->kill = 1;

	bt_unlockpage (BtLockWrite, right->latch);
	bt_unlockpage (BtLockWrite, set->latch);

	// redirect higher key directly to our new node contents

	if( bt_insertkey (bt, higherfence+1, *higherfence, lvl+1, set->page_no, time(NULL)) )
	  return bt->err;

	//	delete old lower key to our node

	if( bt_deletekey (bt, lowerfence+1, *lowerfence, lvl+1) )
	  return bt->err;

	//	obtain delete and write locks to right node

	bt_unlockpage (BtLockParent, right->latch);
	bt_lockpage (BtLockDelete, right->latch);
	bt_lockpage (BtLockWrite, right->latch);
	bt_freepage (bt, right);

	bt_unlockpage (BtLockParent, set->latch);
	bt_unpinlatch (set->latch);
	bt_unpinpool (set->pool);
	bt->found = found;
	return 0;
}
//	a fence key was deleted from a page
//	push new fence value upwards

BTERR bt_fixfence (BtDb *bt, BtPageSet *set, uint lvl)
{
unsigned char leftkey[256], rightkey[256];
uid page_no;
BtKey ptr;
uint idx;

	//	remove the old fence value

	ptr = keyptr(set->page, set->page->cnt);
	memcpy (rightkey, ptr, ptr->len + 1);

	memset (slotptr(set->page, set->page->cnt--), 0, sizeof(BtSlot));
	set->page->dirty = 1;

	ptr = keyptr(set->page, set->page->cnt);
	memcpy (leftkey, ptr, ptr->len + 1);
	page_no = set->page_no;

	bt_unlockpage (BtLockWrite, set->latch);

	//	insert new (now smaller) fence key

	if( bt_insertkey (bt, leftkey+1, *leftkey, lvl+1, page_no, time(NULL)) )
	  return bt->err;

	//	now delete old fence key

	if( bt_deletekey (bt, rightkey+1, *rightkey, lvl+1) )
		return bt->err;

	bt_unlockpage (BtLockParent, set->latch);
	bt_unpinlatch(set->latch);
	bt_unpinpool (set->pool);
	return 0;
}

//	root has a single child
//	collapse a level from the tree

BTERR bt_collapseroot (BtDb *bt, BtPageSet *root)
{
BtPageSet child[1];
uint idx;

  // find the child entry and promote as new root contents

  do {
	for( idx = 0; idx++ < root->page->cnt; )
	  if( !slotptr(root->page, idx)->dead )
		break;

	child->page_no = bt_getid (slotptr(root->page, idx)->id);

	child->latch = bt_pinlatch (bt, child->page_no);
	bt_lockpage (BtLockDelete, child->latch);
	bt_lockpage (BtLockWrite, child->latch);

	if( child->pool = bt_pinpool (bt, child->page_no) )
		child->page = bt_page (bt, child->pool, child->page_no);
	else
		return bt->err;

	memcpy (root->page, child->page, bt->mgr->page_size);
	bt_freepage (bt, child);

  } while( root->page->lvl > 1 && root->page->act == 1 );

  bt_unlockpage (BtLockParentWrt, root->latch);
  bt_unpinlatch (root->latch);
  bt_unpinpool (root->pool);
  return 0;
}

#endif
