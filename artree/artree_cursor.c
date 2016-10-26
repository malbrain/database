#include "../db.h"
#include "../db_object.h"
#include "../db_arena.h"
#include "../db_index.h"
#include "../db_map.h"
#include "artree.h"

//	TODO: lock record

DbStatus artNewCursor(Handle *index, ArtCursor *cursor) {
	cursor->base->key = cursor->key;
	return DB_OK;
}

DbStatus artReturnCursor(Handle *index, DbCursor *dbCursor) {
	return DB_OK;
}

DbStatus artLeftKey(DbCursor *dbCursor, DbMap *map) {
ArtCursor *cursor = (ArtCursor *)dbCursor;
CursorStack* stack;
DbAddr *base;

	base = artIndexAddr(map)->root;
	cursor->depth = 0;

	stack = &cursor->stack[cursor->depth++];
	stack->slot->bits = base->bits;
	stack->addr = base;
	stack->off = 0;
	stack->ch = -1;
	return DB_OK;
}

DbStatus artRightKey(DbCursor *dbCursor, DbMap *map) {
ArtCursor *cursor = (ArtCursor *)dbCursor;
CursorStack* stack;
DbAddr *base;

	base = artIndexAddr(map)->root;
	cursor->depth = 0;

	stack = &cursor->stack[cursor->depth++];
	stack->slot->bits = base->bits;
	stack->addr = base;
	stack->off = 0;
	stack->ch = 256;
	return DB_OK;
}

/**
 * note: used by either 4 or 14 slot node
 * returns entry previous to 'ch'
 * algorithm: place each key byte into radix array, scan backwards
 *			  from 'ch' to preceeding entry.
 */

int slotrev4x14(int ch, uint8_t max, uint32_t alloc, volatile uint8_t* keys) {
uint8_t radix[256];
uint32_t slot;

	memset(radix, 0xff, sizeof(radix));

	for (slot = 0; slot < max; slot++) {
		if (alloc & (1 << slot))
			radix[keys[slot]] = slot;
	}

	while (--ch >= 0) {
		if (radix[ch] < 0xff)
			return radix[ch];
	}
	return -1;
}

int slot4x14(int ch, uint8_t max, uint32_t alloc, volatile uint8_t* keys) {
uint8_t radix[256];
uint32_t slot;

	memset(radix, 0xff, sizeof(radix));

	for (slot = 0; slot < max; slot++) {
		if (alloc & (1 << slot))
			radix[keys[slot]] = slot;
	}

	while (++ch < 256) {
		assert(ch >= 0);
		if (radix[ch] < 0xff)
			return radix[ch];
	}
	return 256;
}

int slotrev64(int ch, uint64_t alloc, volatile uint8_t* keys) {

	while (--ch >= 0) {
		if (keys[ch] < 0xff)
			if (alloc & (1ULL << keys[ch]))
				return ch;
	}
	return -1;
}

int slot64(int ch, uint64_t alloc, volatile uint8_t* keys) {

	while (++ch < 256) {
		assert(ch >= 0);
		if (keys[ch] < 0xff)
			if (alloc & (1ULL << keys[ch]))
				return ch;
	}
	return 256;
}

/**
 * retrieve next key from cursor
 * note:
 *	nextKey sets rightEOF when it cannot advance
 *	prevKey sets leftEOF when it cannot advance
 *
 */

DbStatus artNextKey(DbCursor *dbCursor, DbMap *map) {
ArtCursor *cursor = (ArtCursor *)dbCursor;
int slot, prev, len;

  switch (cursor->base->state) {
	case CursorNone:
	  artLeftKey(dbCursor, map);
	  break;

	case CursorRightEof:
	  return DB_CURSOR_eof;
  }

  while (cursor->depth < MAX_cursor) {
	CursorStack* stack = &cursor->stack[cursor->depth - 1];
	cursor->base->keyLen = stack->off;

	switch (stack->slot->type < SpanNode ? stack->slot->type : SpanNode) {
	  case KeyPass: {
		ARTSplice* splice = getObj(map, *stack->slot);

		if (stack->ch < 0) {
			cursor->stack[cursor->depth].slot->bits = splice->next->bits;
			cursor->stack[cursor->depth].addr = splice->next;
			cursor->stack[cursor->depth].ch = -1;
			cursor->stack[cursor->depth++].off = cursor->base->keyLen;

  			cursor->base->state = CursorPosAt;
			stack->ch = 0;
			return DB_OK;
		}

		break;
	  }

	  case KeyEnd: {
		if (stack->ch < 0) {
  			cursor->base->state = CursorPosAt;
			stack->ch = 0;
			return DB_OK;
		}

		break;
	  }

	  case SpanNode: {
		ARTSpan* spanNode = getObj(map, *stack->slot);
		len = stack->slot->nbyte + 1;

		if (spanNode->timestamp > cursor->base->ts)
			break;

		//  continue into our next slot

		if (stack->ch < 0) {
			memcpy(cursor->key + cursor->base->keyLen, spanNode->bytes, len);
			cursor->base->keyLen += len;
			cursor->stack[cursor->depth].slot->bits = spanNode->next->bits;
			cursor->stack[cursor->depth].addr = spanNode->next;
			cursor->stack[cursor->depth].ch = -1;
			cursor->stack[cursor->depth++].off = cursor->base->keyLen;
			stack->ch = 0;
			continue;
		}

		break;
	  }

	  case Array4: {
		ARTNode4* radix4Node = getObj(map, *stack->slot);

		if (radix4Node->timestamp > cursor->base->ts)
			break;

		slot = slot4x14(stack->ch, 4, radix4Node->alloc, radix4Node->keys);
		if (slot >= 4)
			break;

		stack->ch = radix4Node->keys[slot];
		cursor->key[cursor->base->keyLen++] = radix4Node->keys[slot];

		cursor->stack[cursor->depth].slot->bits = radix4Node->radix[slot].bits;
		cursor->stack[cursor->depth].addr = &radix4Node->radix[slot];
		cursor->stack[cursor->depth].off = cursor->base->keyLen;
		cursor->stack[cursor->depth++].ch = -1;
		continue;
	  }

	  case Array14: {
		ARTNode14* radix14Node = getObj(map, *stack->slot);

		if (radix14Node->timestamp > cursor->base->ts)
			break;

		slot = slot4x14(stack->ch, 14, radix14Node->alloc, radix14Node->keys);
		if (slot >= 14)
			break;

		stack->ch = radix14Node->keys[slot];
		cursor->key[cursor->base->keyLen++] = radix14Node->keys[slot];
		cursor->stack[cursor->depth].slot->bits = radix14Node->radix[slot].bits;
		cursor->stack[cursor->depth].addr = &radix14Node->radix[slot];
		cursor->stack[cursor->depth].ch = -1;
		cursor->stack[cursor->depth++].off = cursor->base->keyLen;
		continue;
	  }

	  case Array64: {
		ARTNode64* radix64Node = getObj(map, *stack->slot);

		if (radix64Node->timestamp > cursor->base->ts)
			break;

		stack->ch = slot64(stack->ch, radix64Node->alloc, radix64Node->keys);
		if (stack->ch == 256)
			break;

		cursor->key[cursor->base->keyLen++] = stack->ch;
		cursor->stack[cursor->depth].slot->bits = radix64Node->radix[radix64Node->keys[stack->ch]].bits;
		cursor->stack[cursor->depth].addr = &radix64Node->radix[radix64Node->keys[stack->ch]];
		cursor->stack[cursor->depth].ch = -1;
		cursor->stack[cursor->depth++].off = cursor->base->keyLen;
		continue;
	  }

	  case Array256: {
		ARTNode256* radix256Node = getObj(map, *stack->slot);

		if (radix256Node->timestamp > cursor->base->ts)
			break;

		while (stack->ch < 256) {
			uint32_t idx = ++stack->ch;
			if (idx < 256 && radix256Node->radix[idx].type)
				break;
		}

		if (stack->ch == 256)
			break;

		cursor->key[cursor->base->keyLen++] = stack->ch;
		cursor->stack[cursor->depth].slot->bits = radix256Node->radix[stack->ch].bits;
		cursor->stack[cursor->depth].addr = &radix256Node->radix[stack->ch];
		cursor->stack[cursor->depth].ch = -1;
		cursor->stack[cursor->depth++].off = cursor->base->keyLen;
		continue;
	  }
	}  // end switch

	if (--cursor->depth) {
		stack = &cursor->stack[cursor->depth];
		cursor->base->keyLen = stack->off;
		continue;
	}

	break;
  }  // end while

  cursor->base->state = CursorRightEof;
  return DB_CURSOR_eof;
}

/**
 * retrieve previous key from the cursor
 */

DbStatus artPrevKey(DbCursor *dbCursor, DbMap *map) {
ArtCursor *cursor = (ArtCursor *)dbCursor;
CursorStack *stack;
int slot, len;

  switch (cursor->base->state) {
	case CursorNone:
	  artRightKey(dbCursor, map);
	  break;

	case CursorLeftEof:
	  return DB_CURSOR_eof;
  }

  while (cursor->depth) {
	stack = &cursor->stack[cursor->depth - 1];
	cursor->base->keyLen = stack->off;

	switch (stack->slot->type < SpanNode ? stack->slot->type : SpanNode) {
	  case UnusedSlot: {
			break;
	  }

	  case KeyPass: {
		ARTSplice* splice = getObj(map, *stack->slot);

		if (stack->ch > 255) {
			cursor->stack[cursor->depth].slot->bits = splice->next->bits;
			cursor->stack[cursor->depth].addr = splice->next;
			cursor->stack[cursor->depth].ch = 256;
			cursor->stack[cursor->depth++].off = cursor->base->keyLen;
			stack->ch = 0;
			continue;
		}

  		cursor->base->state = CursorPosAt;
		stack->ch = -1;
		return DB_OK;
	  }

	  case KeyEnd: {
		if (stack->ch == 256) {
  			cursor->base->state = CursorPosAt;
			stack->ch = -1;
			return DB_OK;
		}

		break;
	  }

	  case SpanNode: {
		ARTSpan* spanNode = getObj(map, *stack->slot);
		len = stack->slot->nbyte + 1;

		if (spanNode->timestamp > cursor->base->ts)
			break;

		// examine next node under slot

		if (stack->ch > 255) {
			memcpy(cursor->key + cursor->base->keyLen, spanNode->bytes, len);
			cursor->base->keyLen += len;
			cursor->stack[cursor->depth].slot->bits = spanNode->next->bits;
			cursor->stack[cursor->depth].addr = spanNode->next;
			cursor->stack[cursor->depth].ch = 256;
			cursor->stack[cursor->depth++].off = cursor->base->keyLen;
			stack->ch = 0;
			continue;
		}
		break;
	  }

	  case Array4: {
		ARTNode4* radix4Node = getObj(map, *stack->slot);

		if (radix4Node->timestamp > cursor->base->ts)
			break;

		slot = slotrev4x14(stack->ch, 4, radix4Node->alloc, radix4Node->keys);
		if (slot < 0)
			break;

		stack->ch = radix4Node->keys[slot];
		cursor->key[cursor->base->keyLen++] = stack->ch;

		cursor->stack[cursor->depth].slot->bits = radix4Node->radix[slot].bits;
		cursor->stack[cursor->depth].addr = &radix4Node->radix[slot];
		cursor->stack[cursor->depth].off = cursor->base->keyLen;
		cursor->stack[cursor->depth++].ch = 256;
		continue;
	  }

	  case Array14: {
		ARTNode14* radix14Node = getObj(map, *stack->slot);

		if (radix14Node->timestamp > cursor->base->ts)
			break;

		slot = slotrev4x14(stack->ch, 14, radix14Node->alloc, radix14Node->keys);
		if (slot < 0)
			break;

		stack->ch = radix14Node->keys[slot];
		cursor->key[cursor->base->keyLen++] = stack->ch;

		cursor->stack[cursor->depth].slot->bits = radix14Node->radix[slot].bits;
		cursor->stack[cursor->depth].addr = &radix14Node->radix[slot];
		cursor->stack[cursor->depth].off = cursor->base->keyLen;
		cursor->stack[cursor->depth++].ch = 256;
		continue;
	  }

	  case Array64: {
	  	ARTNode64* radix64Node = getObj(map, *stack->slot);

	  	if (radix64Node->timestamp > cursor->base->ts)
	  		break;

	  	stack->ch = slotrev64(stack->ch, radix64Node->alloc, radix64Node->keys);
	  	if (stack->ch < 0)
	  		break;

	  	slot = radix64Node->keys[stack->ch];
	  	cursor->key[cursor->base->keyLen++] = stack->ch;

	  	cursor->stack[cursor->depth].slot->bits = radix64Node->radix[slot].bits;
	  	cursor->stack[cursor->depth].addr = &radix64Node->radix[slot];
	  	cursor->stack[cursor->depth].off = cursor->base->keyLen;
	  	cursor->stack[cursor->depth++].ch = 256;
	  	continue;
	  }

	  case Array256: {
	  	ARTNode256* radix256Node = getObj(map, *stack->slot);

	  	if (radix256Node->timestamp > cursor->base->ts)
	  		break;

	  	while (--stack->ch >= 0) {
	  		uint32_t idx = stack->ch;
	  		if (radix256Node->radix[idx].type)
	  			break;
	  	}

	  	if (stack->ch < 0)
	  		break;

	  	slot = stack->ch;
	  	cursor->key[cursor->base->keyLen++] = stack->ch;

	  	cursor->stack[cursor->depth].slot->bits = radix256Node->radix[slot].bits;
	  	cursor->stack[cursor->depth].addr = &radix256Node->radix[slot];
	  	cursor->stack[cursor->depth].off = cursor->base->keyLen;
	  	cursor->stack[cursor->depth++].ch = 256;
	  	continue;
	  }
	}  // end switch

	if (--cursor->depth) {
		cursor->base->keyLen = stack[-1].off;
		continue;
	}

	break;
  }  // end while

  cursor->base->state = CursorLeftEof;
  return DB_CURSOR_eof;
}
