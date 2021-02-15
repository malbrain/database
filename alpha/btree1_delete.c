#include "btree1.h"

DbStatus btree1DeleteKey(Handle *index, void *key, uint32_t len) {
	return DB_OK;
}

// todo: adapt btree-source-code delete below

#if 0
//	a fence key was deleted from an interiour level page
//	push new fence value upwards

BTERR bt_fixfence (BtMgr *mgr, BtPageSet *set, uint lvl, ushort thread_no)
{
unsigned char leftkey[BT_keyarray], rightkey[BT_keyarray];
unsigned char value[BtId];
BtKey* ptr;
uint idx;

	//	remove the old fence value

	ptr = fenceptr(set->page);
	memcpy (rightkey, ptr, ptr->len + sizeof(BtKey));
	memset (slotptr(set->page, set->page->cnt--), 0, sizeof(BtSlot));
	set->page->fence = slotptr(set->page, set->page->cnt)->off;

	//  cache new fence value

	ptr = fenceptr(set->page);
	memcpy (leftkey, ptr, ptr->len + sizeof(BtKey));

	bt_lockpage (BtLockParent, set->latch, thread_no, __LINE__);
	bt_unlockpage (BtLockWrite, set->latch, thread_no, __LINE__);

	//	insert new (now smaller) fence key

	bt_putid (value, set->latch->page_no);
	ptr = (BtKey*)leftkey;

	if( bt_insertkey (mgr, ptr->key, ptr->len, lvl+1, value, BtId, Unique, thread_no) )
	  return mgr->err_thread = thread_no, mgr->err;

	//	now delete old fence key

	ptr = (BtKey*)rightkey;

	if( bt_deletekey (mgr, ptr->key, ptr->len, lvl+1, thread_no) )
		return mgr->err_thread = thread_no, mgr->err;

	bt_unlockpage (BtLockParent, set->latch, thread_no, __LINE__);
	bt_unpinlatch(set->latch, 1, thread_no, __LINE__);
	return 0;
}

//	root has a single child
//	collapse a level from the tree

BTERR bt_collapseroot (BtMgr *mgr, BtPageSet *root, ushort thread_no)
{
BtPageSet child[1];
uid page_no;
BtVal *val;
uint idx;

  // find the child entry and promote as new root contents

  do {
	for( idx = 0; idx++ < root->page->cnt; )
	  if( !slotptr(root->page, idx)->dead )
		break;

	val = valptr(root->page, idx);

	if( val->len == BtId )
		page_no = bt_getid (valptr(root->page, idx)->value);
	else
  		return mgr->line = __LINE__, mgr->err_thread = thread_no, mgr->err = BTERR_struct;

	if( child->latch = bt_pinlatch (mgr, page_no, thread_no) )
		child->page = bt_mappage (mgr, child->latch);
	else
		return mgr->err_thread = thread_no, mgr->err;

	bt_lockpage (BtLockDelete, child->latch, thread_no, __LINE__);
	bt_lockpage (BtLockWrite, child->latch, thread_no, __LINE__);

	memcpy (root->page, child->page, mgr->page_size);
	bt_freepage (mgr, child, thread_no);

  } while( root->page->lvl > 1 && root->page->act == 1 );

  bt_unlockpage (BtLockWrite, root->latch, thread_no, __LINE__);
  bt_unpinlatch (root->latch, 1, thread_no, __LINE__);
  return 0;
}

//  delete a page and manage key
//  call with page writelocked

//	returns with page unpinned
//	from the page pool.

BTERR bt_deletepage (BtMgr *mgr, BtPageSet *set, ushort thread_no, uint lvl)
{
unsigned char lowerfence[BT_keyarray];
uint page_size = mgr->page_size, kill;
BtPageSet right[1], temp[1];
unsigned char value[BtId];
uid page_no, right2;
BtKey *ptr;

	if( !lvl )
		page_size <<= mgr->leaf_xtra;

	//  cache original copy of original fence key
	//	that is going to be deleted.

	ptr = fenceptr(set->page);
	memcpy (lowerfence, ptr, ptr->len + sizeof(BtKey));

	//	pin and lock our right page

	page_no = set->page->right;

	if( right->latch = lvl ? bt_pinlatch (mgr, page_no, thread_no) : bt_pinleaf (mgr, page_no, thread_no) )
		right->page = bt_mappage (mgr, right->latch);
	else
		return 0;

	bt_lockpage (BtLockWrite, right->latch, thread_no, __LINE__);

	if( right->page->kill || set->page->kill )
		return mgr->line = __LINE__, mgr->err = BTERR_struct;

	// pull contents of right sibling over our empty page
	//	preserving our left page number, and its right page number.

	bt_lockpage (BtLockLink, set->latch, thread_no, __LINE__);
	page_no = set->page->left;
	memcpy (set->page, right->page, page_size);
	set->page->left = page_no;
	bt_unlockpage (BtLockLink, set->latch, thread_no, __LINE__);

	//  fix left link from far right page

	if( right2 = set->page->right ) {
	  if( temp->latch = lvl ? bt_pinlatch (mgr, right2, thread_no) : bt_pinleaf (mgr, right2, thread_no) )
		temp->page = bt_mappage (mgr, temp->latch);
	  else
		return 0;

	  bt_lockpage (BtLockAccess, temp->latch, thread_no, __LINE__);
      bt_lockpage(BtLockLink, temp->latch, thread_no, __LINE__);
	  temp->page->left = set->latch->page_no;
	  bt_unlockpage(BtLockLink, temp->latch, thread_no, __LINE__);
	  bt_unlockpage(BtLockAccess, temp->latch, thread_no, __LINE__);
	  bt_unpinlatch (temp->latch, 1, thread_no, __LINE__);
	} else if( !lvl ) {	// our page is now rightmost leaf
	  bt_mutexlock (mgr->lock);
	  mgr->pagezero->alloc->left = set->latch->page_no;
	  bt_releasemutex(mgr->lock);
	}

	// mark right page as being deleted and release lock

	right->page->kill = 1;
	bt_unlockpage (BtLockWrite, right->latch, thread_no, __LINE__);

	// redirect the new higher key directly to our new node

	ptr = fenceptr(set->page);
	bt_putid (value, set->latch->page_no);

	if( bt_insertkey (mgr, ptr->key, ptr->len, lvl+1, value, BtId, Update, thread_no) )
	  return mgr->err;

	//	delete our original fence key in parent

	ptr = (BtKey *)lowerfence;

	if( bt_deletekey (mgr, ptr->key, ptr->len, lvl+1, thread_no) )
	  return mgr->err;

	//  wait for all access to drain away with delete lock,
	//	then obtain write lock to right node and free it.

	bt_lockpage (BtLockDelete, right->latch, thread_no, __LINE__);
	bt_lockpage (BtLockWrite, right->latch, thread_no, __LINE__);
	bt_lockpage (BtLockLink, right->latch, thread_no, __LINE__);
	bt_freepage (mgr, right, thread_no);

	//	release write lock to our node

	bt_unlockpage (BtLockWrite, set->latch, thread_no, __LINE__);
	bt_unpinlatch (set->latch, 1, thread_no, __LINE__);
	return 0;
}

//  find and delete key on page by marking delete flag bit
//  if page becomes empty, delete it from the btree

BTERR bt_deletekey (BtMgr *mgr, unsigned char *key, uint len, uint lvl, ushort thread_no)
{
uint slot, idx, found, fence;
BtPageSet set[1];
BtSlot *node;
BtKey *ptr;
BtVal *val;

	if( slot = bt_loadpage (mgr, set, key, len, lvl, BtLockWrite, thread_no) ) {
		node = slotptr(set->page, slot);
		ptr = keyptr(set->page, slot);
	} else
		return mgr->err_thread = thread_no, mgr->err;

	// if librarian slot, advance to real slot

	if( node->type == Librarian ) {
		ptr = keyptr(set->page, ++slot);
		node = slotptr(set->page, slot);
	}

	fence = slot == set->page->cnt;

	// delete the key, ignore request if already dead

	if( found = !keycmp (ptr, key, len) )
	  if( found = node->dead == 0 ) {
		val = valptr(set->page,slot);
 		set->page->garbage += ptr->len + val->len + sizeof(BtKey) + sizeof(BtVal);
 		set->page->act--;
		node->dead = 1;

		// collapse empty slots beneath the fence
		// on interiour nodes

		if( lvl )
		 while( idx = set->page->cnt - 1 )
		  if( slotptr(set->page, idx)->dead ) {
			*slotptr(set->page, idx) = *slotptr(set->page, idx + 1);
			memset (slotptr(set->page, set->page->cnt--), 0, sizeof(BtSlot));
		  } else
			break;
	  }

	if( !found )
		return 0;

	//	did we delete a fence key in an upper level?

	if( lvl && set->page->act && fence )
	  return bt_fixfence (mgr, set, lvl, thread_no);

	//	do we need to collapse root?

	if( lvl > 1 && set->latch->page_no == ROOT_page && set->page->act == 1 )
	  return bt_collapseroot (mgr, set, thread_no);

	//	delete empty page

 	if( !set->page->act )
	  return bt_deletepage (mgr, set, thread_no, set->page->lvl);

	bt_unlockpage(BtLockWrite, set->latch, thread_no, __LINE__);
	bt_unpinlatch (set->latch, 1, thread_no, __LINE__);
	return 0;
}
#endif
