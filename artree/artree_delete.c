#include "../db.h"
#include "../db_object.h"
#include "../db_handle.h"
#include "../db_index.h"
#include "../db_arena.h"
#include "../db_frame.h"
#include "../db_map.h"
#include "artree.h"

typedef enum {
	ContinueSearch,
	EndSearch,
	RetrySearch,
	RestartSearch,
	ErrorSearch
} ReturnState;

DbStatus artDeleteKey(Handle *index, ArtCursor *cursor) {
ReturnState rt = EndSearch;
DbAddr newSlot;
uint32_t bit;
uint8_t ch;

	//	now that we have the trie nodes in the cursor stack
	//	we can go through them backwards to remove empties.

	while (cursor->depth) {
		CursorStack *stack = &cursor->stack[--cursor->depth];
		uint32_t pass = 0;
		bool retry = true;

		ch = stack->ch;

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
				rt = EndSearch;
				break;
			}
			case KeyEnd: {
				ARTKeyEnd* keyEndNode = getObj(index->map, *stack->addr);
				keyEndNode->next->bits = 0;

				if(!addSlotToFrame(index->map, listHead(index,newSlot.type), listTail(index,newSlot.type), newSlot.bits))
					rt = ErrorSearch;
				else
					rt = EndSearch;
				break;
			}

			case SpanNode: {
				stack->addr->bits = 0;

				if(!addSlotToFrame(index->map, listHead(index,newSlot.type), listTail(index,newSlot.type), newSlot.bits))
					rt = ErrorSearch;
				else
					continue;

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

				if(addSlotToFrame(index->map, listHead(index,newSlot.type), listTail(index,newSlot.type), newSlot.bits))
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

				if(addSlotToFrame(index->map, listHead(index,newSlot.type), listTail(index,newSlot.type), newSlot.bits))
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

				if(addSlotToFrame(index->map, listHead(index,newSlot.type), listTail(index,newSlot.type), newSlot.bits))
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

				if(addSlotToFrame(index->map, listHead(index,newSlot.type), listTail(index,newSlot.type), newSlot.bits))
					continue;

				rt = ErrorSearch;
				break;
			}
		}	// end switch

		unlockLatch(stack->addr->latch);
		break;

	}	// end while

	return rt == EndSearch ? DB_OK : DB_ERROR_deletekey;
}
