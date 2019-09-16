#include "artree.h"

typedef enum {
	ContinueSearch,
	EndSearch,
	RetrySearch,
	RestartSearch,
	ErrorSearch
} ReturnState;

DbStatus artDeleteKey(Handle *index, uint8_t *key, uint16_t keyLen, uint16_t suffixLen) {
uint8_t tmpCursor[sizeof(DbCursor) + sizeof(ArtCursor)];
ReturnState rt = ErrorSearch;
DbCursor *dbCursor;
ArtCursor *cursor;
DbAddr newSlot;
DbStatus stat;
uint32_t bit;
uint8_t ch;

	memset(tmpCursor, 0, sizeof(tmpCursor));

	dbCursor = (DbCursor *)tmpCursor;
	cursor = (ArtCursor *)(tmpCursor + sizeof(DbCursor));

	dbCursor->key = cursor->key;

	if ((stat = artFindKey(dbCursor, index->map, key, keyLen, suffixLen)))
		return stat;

	//	we take the trie nodes in the cursor stack
	//	and go through them backwards to remove empties.

	if (cursor->depth)
	 if (cursor->stack[cursor->depth - 1].addr->type == KeyEnd) {
	  while (cursor->depth) {
		CursorStack *stack = &cursor->stack[--cursor->depth];
		uint32_t pass = 0;
		bool retry = true;

		ch = (uint8_t)stack->ch;

		//	wait if we run into a dead slot
		do {
			if (pass)
				yield();
			else
				pass = 1;

			// obtain write lock on the node

			lockLatch(stack->addr->latch);
			newSlot.bits = stack->addr->bits;

			if ((retry = newSlot.kill))
				unlockLatch(stack->addr->latch);
		} while (retry);

		switch (newSlot.type < SpanNode ? newSlot.type : SpanNode) {
			case UnusedSlot: {
				continue;
			}

			case FldEnd: {
				ARTFldEnd* fldEndNode = getObj(index->map, *stack->addr);
				stack->addr->bits = fldEndNode->sameFld->bits;
				fldEndNode->nextFld->bits = 0;

				if(addSlotToFrame(index->map, listHead(index,newSlot.type), listWait(index,newSlot.type), newSlot.bits)) {
				  if (stack->addr->type)
					rt = EndSearch;
				  else
					continue;
				} else
				  rt = ErrorSearch;

				break;
			}

/*			case KeyUniq: {
				ARTKeyUniq* keyUniqNode = getObj(index->map, *stack->addr);

				if (stack->off == uniqueLen) {
					stack->addr->bits = keyUniqNode->dups->bits;
					keyUniqNode->dups->bits = 0;
				} else {
					stack->addr->bits = keyUniqNode->next->bits;
					keyUniqNode->next->bits = 0;
				}

				if(addSlotToFrame(index->map, listHead(index,newSlot.type), listWait(index,newSlot.type), newSlot.bits)) {
				  if (stack->addr->type)
					rt = EndSearch;
				  else
					continue;
				} else
					rt = ErrorSearch;

				break;
			}
*/
			case KeyEnd: {
				if (newSlot.addr) { // is there a continuation?
				  ARTKeyEnd* keyEndNode = getObj(index->map, *stack->addr);
				  stack->addr->bits = keyEndNode->next->bits;
				  keyEndNode->next->bits = 0;

				  if(addSlotToFrame(index->map, listHead(index,newSlot.type), listWait(index,newSlot.type), newSlot.bits)) {
					if (stack->addr->type)
					  rt = EndSearch;
					else
					  continue;
				  } else
					rt = ErrorSearch;
				}

				break;
			}

			case SpanNode: {
				stack->addr->bits = 0;

				if(addSlotToFrame(index->map, listHead(index,newSlot.type), listWait(index,newSlot.type), newSlot.bits))
					continue;

				rt = ErrorSearch;
				break;
			}

			case Array4: {
				ARTNode4 *node = getObj(index->map, *stack->addr);

				for (bit = 0; bit < 4; bit++) {
					if (node->alloc & (1 << bit))
						if (ch == node->keys[bit])
							break;
				}

				if (bit == 4) {
					rt = EndSearch;  // key byte not found
					break;
				}

				// we are not the last entry in the node?

				node->alloc &= ~(1 << bit);

				if (node->alloc) {
					rt = EndSearch;
					break;
				}

				stack->addr->bits = 0;

				if(addSlotToFrame(index->map, listHead(index,newSlot.type), listWait(index,newSlot.type), newSlot.bits))
					continue;

				rt = ErrorSearch;
				break;
			}

			case Array14: {
				ARTNode14 *node = getObj(index->map, *stack->addr);

				for (bit = 0; bit < 14; bit++) {
					if (node->alloc & (1 << bit))
						if (ch == node->keys[bit])
							break;
				}

				if (bit == 14) {
					rt = EndSearch;  // key byte not found
					break;
				}

				// we are not the last entry in the node?

				node->alloc &= ~(1 << bit);

				if (node->alloc) {
					rt = EndSearch;
					break;
				}

				stack->addr->bits = 0;

				if(addSlotToFrame(index->map, listHead(index,newSlot.type), listWait(index,newSlot.type), newSlot.bits))
					continue;

				rt = ErrorSearch;
				break;
			}

			case Array64: {
				ARTNode64 *node = getObj(index->map, *stack->addr);
				bit = node->keys[ch];

				if (bit == 0xff) {
					rt = EndSearch;
					break;
				}

				node->keys[ch] = 0xff;
				node->alloc &= ~(1ULL << bit);

				if (node->alloc) {
					rt = EndSearch;
					break;
				}

				stack->addr->bits = 0;

				if(addSlotToFrame(index->map, listHead(index,newSlot.type), listWait(index,newSlot.type), newSlot.bits))
					continue;

				rt = ErrorSearch;
				break;
			}

			case Array256: {
				ARTNode256 *node = getObj(index->map, *stack->addr);
				bit = ch;

				// is radix slot empty?
				if (!node->radix[bit].type) {
					rt = EndSearch;
					break;
				}

				// was this the last used slot?
				if (--stack->addr->nslot) {
					rt = EndSearch;
					break;
				}

				// remove the slot
				stack->addr->bits = 0;

				if(addSlotToFrame(index->map, listHead(index,newSlot.type), listWait(index,newSlot.type), newSlot.bits))
					continue;

				rt = ErrorSearch;
				break;
			}
		}	// end switch

		unlockLatch(stack->addr->latch);
		break;

	  }	// end while
	}	// end if

	return rt == EndSearch ? DB_OK : DB_ERROR_deletekey;
}
