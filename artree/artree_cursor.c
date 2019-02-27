#include "artree.h"

//	TODO: lock record

DbStatus artNewCursor(DbCursor *dbCursor, DbMap *map) {
ArtCursor *cursor = (ArtCursor *)dbCursor;

	dbCursor->binaryFlds = map->arenaDef->params[IdxKeyFlds].boolVal;
	dbCursor->key = cursor->key;
	return DB_OK;
}

DbStatus artReturnCursor(DbCursor *dbCursor, DbMap *map) {
	return DB_OK;
}

DbStatus artLeftKey(DbCursor *dbCursor, DbMap *map) {
ArtCursor *cursor = (ArtCursor *)dbCursor;
CursorStack* stack;
ArtIndex *artIdx;
DbAddr *base;

	artIdx = artindex(map);

	base = artIdx->root;
	cursor->depth = 0;

	stack = &cursor->stack[cursor->depth++];

	stack->slot->bits = base->bits;
	stack->lastFld = 0;
	stack->addr = base;
	stack->off = 0;
	stack->ch = -1;

	if (dbCursor->binaryFlds)
		stack->off = 2;

	dbCursor->state = CursorLeftEof;
	return DB_OK;
}

DbStatus artRightKey(DbCursor *dbCursor, DbMap *map) {
ArtCursor *cursor = (ArtCursor *)dbCursor;
CursorStack* stack;
ArtIndex *artIdx;
DbAddr *base;

	artIdx = artindex(map);

	base = artIdx->root;
	cursor->depth = 0;

	stack = &cursor->stack[cursor->depth++];
	stack->slot->bits = base->bits;
	stack->lastFld = 0;
	stack->addr = base;
	stack->off = 0;
	stack->ch = 256;

	if (dbCursor->binaryFlds)
		stack->off = 2;

	dbCursor->state = CursorRightEof;
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
int slot, len;

  switch (dbCursor->state) {
	case CursorLeftEof:
	case CursorNone:
	  artLeftKey(dbCursor, map);
	  break;

	case CursorRightEof:
	  return DB_CURSOR_eof;

	default:
	  break;
  }

  while (cursor->depth < MAX_cursor) {
	CursorStack* stack = &cursor->stack[cursor->depth - 1];
	dbCursor->keyLen = stack->off;
	cursor->lastFld = stack->lastFld;

	switch (stack->slot->type < SpanNode ? stack->slot->type : SpanNode) {
	  case UnusedSlot: {
			break;
	  }

	  case FldEnd: {
		//  this case only occurs with binaryFlds is true
		ARTFldEnd* fldEndNode = getObj(map, *stack->slot);

		// end this field and begin next one

		if (stack->ch < 0) {
		  int fldLen = dbCursor->keyLen - cursor->lastFld - 2;
		  cursor->key[cursor->lastFld] = fldLen >> 8;
		  cursor->key[cursor->lastFld + 1] = fldLen;
		  cursor->lastFld = dbCursor->keyLen;
		  dbCursor->keyLen += 2;

		  cursor->stack[cursor->depth].slot->bits = fldEndNode->nextFld->bits;
		  cursor->stack[cursor->depth].addr = fldEndNode->nextFld;
		  cursor->stack[cursor->depth].ch = -1;
		  cursor->stack[cursor->depth].lastFld = cursor->lastFld;
		  cursor->stack[cursor->depth++].off = dbCursor->keyLen;
		  stack->ch = 0;
		  continue;
		}

		// continue processing the current field bytes

		if (stack->ch > 255)
			break;

		cursor->stack[cursor->depth].slot->bits = fldEndNode->sameFld->bits;
		cursor->stack[cursor->depth].addr = fldEndNode->sameFld;
		cursor->stack[cursor->depth].off = dbCursor->keyLen;
		cursor->stack[cursor->depth].lastFld = cursor->lastFld;
		cursor->stack[cursor->depth++].ch = -1;
		stack->ch = 256;
		continue;
	  }

	  case KeyUniq: {
		if (stack->ch < 0) {
		  ARTKeyUniq* keyUniqNode = getObj(map, *stack->slot);

		  cursor->stack[cursor->depth].slot->bits = keyUniqNode->dups->bits;
		  cursor->stack[cursor->depth].addr = keyUniqNode->dups;
		  cursor->stack[cursor->depth].ch = -1;
		  cursor->stack[cursor->depth].lastFld = cursor->lastFld;
		  cursor->stack[cursor->depth++].off = dbCursor->keyLen;
		  stack->ch = 0;
		  continue;
		}

		if (stack->ch == 0) {
		  ARTKeyUniq* keyUniqNode = getObj(map, *stack->slot);

		  cursor->stack[cursor->depth].slot->bits = keyUniqNode->next->bits;
		  cursor->stack[cursor->depth].addr = keyUniqNode->next;
		  cursor->stack[cursor->depth].ch = -1;
		  cursor->stack[cursor->depth].lastFld = cursor->lastFld;
		  cursor->stack[cursor->depth++].off = dbCursor->keyLen;
		  stack->ch = 256;
		  continue;
		}

		break;
	  }

	  case KeyEnd: {
		if (stack->ch < 0) {
		  if (dbCursor->binaryFlds) {
			int fldLen = dbCursor->keyLen - cursor->lastFld - 2;
			cursor->key[cursor->lastFld] = fldLen >> 8;
			cursor->key[cursor->lastFld + 1] = fldLen;
			cursor->lastFld = dbCursor->keyLen;
		  }

		  if (stack->slot->addr) {  // was there a continuation?
			ARTKeyEnd* keyEndNode = getObj(map, *stack->slot);

			cursor->stack[cursor->depth].slot->bits = keyEndNode->next->bits;
			cursor->stack[cursor->depth].addr = keyEndNode->next;
			cursor->stack[cursor->depth].ch = -1;
			cursor->stack[cursor->depth].lastFld = cursor->lastFld;
			cursor->stack[cursor->depth++].off = dbCursor->keyLen;
			stack->ch = 0;
			continue;
		  }

		  dbCursor->state = CursorPosAt;
		  stack->ch = 0;
		  return DB_OK;
		}

		break;
	  }

	  case SpanNode: {
		ARTSpan* spanNode = getObj(map, *stack->slot);
		len = stack->slot->nbyte + 1;

		//  continue into our next slot

		if (stack->ch < 0) {
		  memcpy(cursor->key + dbCursor->keyLen, spanNode->bytes, len);
		  dbCursor->keyLen += len;
		  cursor->stack[cursor->depth].slot->bits = spanNode->next->bits;
		  cursor->stack[cursor->depth].addr = spanNode->next;
		  cursor->stack[cursor->depth].ch = -1;
		  cursor->stack[cursor->depth].lastFld = cursor->lastFld;
		  cursor->stack[cursor->depth++].off = dbCursor->keyLen;
		  stack->ch = 0;
		  continue;
		}

		break;
	  }

	  case Array4: {
		ARTNode4* radix4Node = getObj(map, *stack->slot);

		slot = slot4x14(stack->ch, 4, radix4Node->alloc, radix4Node->keys);

		if (slot >= 4)
		  break;

		stack->ch = radix4Node->keys[slot];
		cursor->key[dbCursor->keyLen++] = radix4Node->keys[slot];

		cursor->stack[cursor->depth].slot->bits = radix4Node->radix[slot].bits;
		cursor->stack[cursor->depth].addr = &radix4Node->radix[slot];
		cursor->stack[cursor->depth].off = dbCursor->keyLen;
		cursor->stack[cursor->depth].lastFld = cursor->lastFld;
		cursor->stack[cursor->depth++].ch = -1;
		continue;
	  }

	  case Array14: {
		ARTNode14* radix14Node = getObj(map, *stack->slot);

		slot = slot4x14(stack->ch, 14, radix14Node->alloc, radix14Node->keys);

		if (slot >= 14)
		  break;

		stack->ch = radix14Node->keys[slot];
		cursor->key[dbCursor->keyLen++] = radix14Node->keys[slot];
		cursor->stack[cursor->depth].slot->bits = radix14Node->radix[slot].bits;
		cursor->stack[cursor->depth].addr = &radix14Node->radix[slot];
		cursor->stack[cursor->depth].ch = -1;
		cursor->stack[cursor->depth].lastFld = cursor->lastFld;
		cursor->stack[cursor->depth++].off = dbCursor->keyLen;
		continue;
	  }

	  case Array64: {
		ARTNode64* radix64Node = getObj(map, *stack->slot);

		stack->ch = slot64(stack->ch, radix64Node->alloc, radix64Node->keys);

		if (stack->ch == 256)
		  break;

		cursor->key[dbCursor->keyLen++] = (uint8_t)stack->ch;
		cursor->stack[cursor->depth].slot->bits = radix64Node->radix[radix64Node->keys[stack->ch]].bits;
		cursor->stack[cursor->depth].addr = &radix64Node->radix[radix64Node->keys[stack->ch]];
		cursor->stack[cursor->depth].ch = -1;
		cursor->stack[cursor->depth].lastFld = cursor->lastFld;
		cursor->stack[cursor->depth++].off = dbCursor->keyLen;
		continue;
	  }

	  case Array256: {
		ARTNode256* radix256Node = getObj(map, *stack->slot);

		while (stack->ch < 256) {
		  uint32_t idx = ++stack->ch;

		  if (idx < 256 && radix256Node->radix[idx].type)
			break;
		}

		if (stack->ch == 256)
		  break;

		cursor->key[dbCursor->keyLen++] = (uint8_t)stack->ch;
		cursor->stack[cursor->depth].slot->bits = radix256Node->radix[stack->ch].bits;
		cursor->stack[cursor->depth].addr = &radix256Node->radix[stack->ch];
		cursor->stack[cursor->depth].ch = -1;
		cursor->stack[cursor->depth].lastFld = cursor->lastFld;
		cursor->stack[cursor->depth++].off = dbCursor->keyLen;
		continue;
	  }
	}  // end switch

	if (--cursor->depth) {
	  stack = &cursor->stack[cursor->depth];
	  dbCursor->keyLen = stack->off;
	  continue;
	}

	break;
  }  // end while

  dbCursor->state = CursorRightEof;
  return DB_CURSOR_eof;
}

/**
 * retrieve previous key from the cursor
 */

DbStatus artPrevKey(DbCursor *dbCursor, DbMap *map) {
ArtCursor *cursor = (ArtCursor *)dbCursor;
CursorStack *stack;
int slot, len;

  switch (dbCursor->state) {
	case CursorRightEof:
	case CursorNone:
	  artRightKey(dbCursor, map);
	  break;

	case CursorLeftEof:
	  return DB_CURSOR_eof;

	default:
	  break;
  }

  while (cursor->depth) {
	stack = &cursor->stack[cursor->depth - 1];
	dbCursor->keyLen = stack->off;
	cursor->lastFld = stack->lastFld;

	switch (stack->slot->type < SpanNode ? stack->slot->type : SpanNode) {
	  case UnusedSlot: {
			break;
	  }

	  case FldEnd: {
		// this node only occurs when binaryFlds is true
		ARTFldEnd* fldEndNode = getObj(map, *stack->slot);
		int fldLen;

		// continue with sameFld before ending the current field

		if (stack->ch > 255) {
		  cursor->stack[cursor->depth].slot->bits = fldEndNode->sameFld->bits;
		  cursor->stack[cursor->depth].addr = fldEndNode->sameFld;
		  cursor->stack[cursor->depth].ch = 256;
		  cursor->stack[cursor->depth].lastFld = cursor->lastFld;
		  cursor->stack[cursor->depth++].off = dbCursor->keyLen;
		  stack->ch = 0;
		  continue;
		}

		// after finishing same field, end this field and start next

		fldLen = dbCursor->keyLen - cursor->lastFld - 2;
		cursor->key[cursor->lastFld] = fldLen >> 8;
		cursor->key[cursor->lastFld + 1] = fldLen;
		cursor->lastFld = dbCursor->keyLen;
		dbCursor->keyLen += 2;

		if (stack->ch < 0)
			break;

		cursor->stack[cursor->depth].slot->bits = fldEndNode->nextFld->bits;
		cursor->stack[cursor->depth].addr = fldEndNode->nextFld;
		cursor->stack[cursor->depth].off = dbCursor->keyLen;
		cursor->stack[cursor->depth].lastFld = cursor->lastFld;
		cursor->stack[cursor->depth++].ch = 256;
		stack->ch = -1;
		continue;
	  }

	  case KeyUniq: {
		if (stack->ch > 255) {
		  ARTKeyUniq* keyUniqNode = getObj(map, *stack->slot);

		  cursor->stack[cursor->depth].slot->bits = keyUniqNode->next->bits;
		  cursor->stack[cursor->depth].addr = keyUniqNode->next;
		  cursor->stack[cursor->depth].ch = 256;
		  cursor->stack[cursor->depth].lastFld = cursor->lastFld;
		  cursor->stack[cursor->depth++].off = dbCursor->keyLen;
		  stack->ch = 0;
		  continue;
		}

		if (stack->ch == 0) {
		  ARTKeyUniq* keyUniqNode = getObj(map, *stack->slot);

		  cursor->stack[cursor->depth].slot->bits = keyUniqNode->dups->bits;
		  cursor->stack[cursor->depth].addr = keyUniqNode->dups;
		  cursor->stack[cursor->depth].ch = 256;
		  cursor->stack[cursor->depth].lastFld = cursor->lastFld;
		  cursor->stack[cursor->depth++].off = dbCursor->keyLen;
		  stack->ch = -1;
		  continue;
		}

		break;
	  }

	  case KeyEnd: {
		if (stack->ch > 255) {
		  if (dbCursor->binaryFlds) {
			int fldLen = dbCursor->keyLen - cursor->lastFld - 2;
			cursor->key[cursor->lastFld] = fldLen >> 8;
			cursor->key[cursor->lastFld + 1] = fldLen;
			cursor->lastFld = dbCursor->keyLen;
		  }

		  if (stack->slot->addr) {
			ARTKeyEnd* keyEndNode = getObj(map, *stack->slot);

			cursor->stack[cursor->depth].slot->bits = keyEndNode->next->bits;
			cursor->stack[cursor->depth].addr = keyEndNode->next;
			cursor->stack[cursor->depth].ch = 256;
			cursor->stack[cursor->depth].lastFld = cursor->lastFld;
			cursor->stack[cursor->depth++].off = dbCursor->keyLen;
			stack->ch = 0;
			continue;
		  }

		  dbCursor->state = CursorPosAt;
		  stack->ch = 0;
		  return DB_OK;
		}

		break;
	  }

	  case SpanNode: {
		ARTSpan* spanNode = getObj(map, *stack->slot);
		len = stack->slot->nbyte + 1;

		// examine next node under slot

		if (stack->ch > 255) {
			memcpy(cursor->key + dbCursor->keyLen, spanNode->bytes, len);
			dbCursor->keyLen += len;
			cursor->stack[cursor->depth].slot->bits = spanNode->next->bits;
			cursor->stack[cursor->depth].addr = spanNode->next;
			cursor->stack[cursor->depth].ch = 256;
			cursor->stack[cursor->depth].lastFld = cursor->lastFld;
			cursor->stack[cursor->depth++].off = dbCursor->keyLen;
			stack->ch = 0;
			continue;
		}
		break;
	  }

	  case Array4: {
		ARTNode4* radix4Node = getObj(map, *stack->slot);

		slot = slotrev4x14(stack->ch, 4, radix4Node->alloc, radix4Node->keys);
		if (slot < 0)
			break;

		stack->ch = radix4Node->keys[slot];
		cursor->key[dbCursor->keyLen++] = (uint8_t)stack->ch;

		cursor->stack[cursor->depth].slot->bits = radix4Node->radix[slot].bits;
		cursor->stack[cursor->depth].addr = &radix4Node->radix[slot];
		cursor->stack[cursor->depth].off = dbCursor->keyLen;
		cursor->stack[cursor->depth].lastFld = cursor->lastFld;
		cursor->stack[cursor->depth++].ch = 256;
		continue;
	  }

	  case Array14: {
		ARTNode14* radix14Node = getObj(map, *stack->slot);

		slot = slotrev4x14(stack->ch, 14, radix14Node->alloc, radix14Node->keys);
		if (slot < 0)
			break;

		stack->ch = radix14Node->keys[slot];
		cursor->key[dbCursor->keyLen++] = (uint8_t)stack->ch;

		cursor->stack[cursor->depth].slot->bits = radix14Node->radix[slot].bits;
		cursor->stack[cursor->depth].addr = &radix14Node->radix[slot];
		cursor->stack[cursor->depth].off = dbCursor->keyLen;
		cursor->stack[cursor->depth].lastFld = cursor->lastFld;
		cursor->stack[cursor->depth++].ch = 256;
		continue;
	  }

	  case Array64: {
	 	ARTNode64* radix64Node = getObj(map, *stack->slot);

		stack->ch = slotrev64(stack->ch, radix64Node->alloc, radix64Node->keys);
		if (stack->ch < 0)
			break;

		slot = radix64Node->keys[stack->ch];
		cursor->key[dbCursor->keyLen++] = (uint8_t)stack->ch;

		cursor->stack[cursor->depth].slot->bits = radix64Node->radix[slot].bits;
		cursor->stack[cursor->depth].addr = &radix64Node->radix[slot];
		cursor->stack[cursor->depth].off = dbCursor->keyLen;
		cursor->stack[cursor->depth].lastFld = cursor->lastFld;
		cursor->stack[cursor->depth++].ch = 256;
		continue;
	  }

	  case Array256: {
		ARTNode256* radix256Node = getObj(map, *stack->slot);

		while (--stack->ch >= 0) {
			uint32_t idx = stack->ch;
			if (radix256Node->radix[idx].type)
				break;
		}

		if (stack->ch < 0)
			break;

		slot = stack->ch;
		cursor->key[dbCursor->keyLen++] = (uint8_t)stack->ch;

		cursor->stack[cursor->depth].slot->bits = radix256Node->radix[slot].bits;
		cursor->stack[cursor->depth].addr = &radix256Node->radix[slot];
		cursor->stack[cursor->depth].off = dbCursor->keyLen;
		cursor->stack[cursor->depth].lastFld = cursor->lastFld;
		cursor->stack[cursor->depth++].ch = 256;
		continue;
	  }
	}  // end switch

	if (--cursor->depth) {
		dbCursor->keyLen = stack[-1].off;
		continue;
	}

	break;
  }  // end while

  dbCursor->state = CursorLeftEof;
  return DB_CURSOR_eof;
}
