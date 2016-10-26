#include "../db.h"
#include "../db_object.h"
#include "../db_index.h"
#include "../db_arena.h"
#include "../db_map.h"
#include "artree.h"

DbStatus artFindKey( DbCursor *dbCursor, DbMap *map, uint8_t *key, uint32_t keyLen) {
ArtCursor *cursor = (ArtCursor *)dbCursor;
uint32_t idx, offset = 0, spanMax;
volatile DbAddr *slot;
CursorStack* stack;

	cursor->base->keyLen = 0;
	cursor->depth = 0;

	// loop through the key bytes

	slot = artIndexAddr(map)->root;

	while (offset < keyLen) {
		if (cursor->depth < MAX_cursor)
			stack = cursor->stack + cursor->depth++;
		else
			return DB_ERROR_cursoroverflow;

		stack->off = cursor->base->keyLen;
		stack->slot->bits = slot->bits;;
		stack->ch = key[offset];
		stack->addr = slot;

		switch (slot->type < SpanNode ? slot->type : SpanNode) {
		  case KeyPass: {
		   	ARTSplice* splice = getObj(map, *slot);

			slot = splice->next;

			if (cursor)
				stack->ch = 256;

			continue;
		  }

		  case KeyEnd: {
			return artNextKey(dbCursor, map);
		  }

		  case SpanNode: {
			ARTSpan* spanNode = getObj(map, *slot);
			uint32_t amt = keyLen - offset;
			int diff;

			spanMax = slot->nbyte + 1;

			if (amt > spanMax)
				amt = spanMax;

			diff = memcmp(key + offset, spanNode->bytes, amt);

			//  does the key end inside the span?

			if (spanMax > amt || diff)
				break;

			//  continue to the next slot

			cursor->base->keyLen += spanMax;
			slot = spanNode->next;
			offset += spanMax;
			continue;
		  }

		  case Array4: {
			ARTNode4 *node = getObj(map, *slot);

			// simple loop comparing bytes

			for (idx = 0; idx < 4; idx++)
			  if (node->alloc & (1 << idx))
				if (key[offset] == node->keys[idx])
				  break;

			if (idx < 4) {
			  slot = node->radix + idx;
			  cursor->base->keyLen++;
			  offset++;
			  continue;
			}

			// key byte not found

			break;
		  }

		  case Array14: {
			ARTNode14 *node = getObj(map, *slot);

			// simple loop comparing bytes

			for (idx = 0; idx < 14; idx++)
			  if (node->alloc & (1 << idx))
				if (key[offset] == node->keys[idx])
				  break;

			if (idx < 14) {
			  slot = node->radix + idx;
			  cursor->base->keyLen++;
			  offset++;
			  continue;
			}

			// key byte not found

			break;
		  }

		  case Array64: {
			ARTNode64* node = getObj(map, *slot);
			idx = node->keys[key[offset]];

			if (idx < 0xff && (node->alloc & (1ULL << idx))) {
			  slot = node->radix + idx;
			  cursor->base->keyLen++;
			  offset++;
			  continue;
			}

			// key byte not found

			break;
		  }

		  case Array256: {
			ARTNode256* node = getObj(map, *slot);
			idx = key[offset];

			if (node->radix[idx].type) {
			  slot = node->radix + idx;
			  cursor->base->keyLen++;
			  offset++;
			  continue;
			}

			// key byte not found

			break;
		  }

		  case UnusedSlot: {
			cursor->base->state = CursorRightEof;
			return DB_OK;
		  }
		}  // end switch

		break;
	}  // end while (offset < keylen)

	memcpy (cursor->key, key, cursor->base->keyLen);
	cursor->base->state = CursorPosAt;

	if (slot->type == KeyEnd)
		return DB_OK;

	if (slot->type == KeyPass)
		return DB_OK;

	if (cursor->depth < MAX_cursor)
		stack = cursor->stack + cursor->depth++;
	else
		return DB_ERROR_cursoroverflow;

	stack->off = cursor->base->keyLen;
	stack->slot->bits = slot->bits;;
	stack->addr = slot;
	stack->ch = -1;

	//  complete the key

	return artNextKey(dbCursor, map);
}
