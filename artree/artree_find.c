#include "artree.h"

DbStatus artFindKey( DbCursor *dbCursor, DbMap *map, void *findKey, uint16_t keyLen, uint16_t suffixLen) {
ArtCursor *cursor = (ArtCursor *)dbCursor;
uint32_t fldLen = 0, idx, offset = 0, spanMax;
bool binaryFlds = map->arenaDef->params[IdxKeyFlds].charVal;
CursorStack* stack = NULL;
uint8_t *key = findKey;
DbAddr *slot;
ArtIndex *artIdx;
uint16_t lastFld = 0;

  artIdx = artindex(map);

  dbCursor->keyLen = 0;
  cursor->depth = 0;

  // loop through the key bytes
  // 
  slot = artIdx->root;

  if (binaryFlds && !dbCursor->keyLen) {
	fldLen = key[offset] << 8 | key[offset + 1];
	dbCursor->keyLen = 2;
	offset += 2;
  }

  while (offset < keyLen) {
	if (cursor->depth < MAX_cursor)
	  stack = cursor->stack + cursor->depth++;
	else
	  return DB_ERROR_cursoroverflow;

	stack->slot->bits = slot->bits;
	stack->off = dbCursor->keyLen;
	stack->ch = key[offset];
	stack->addr = slot;

	switch (slot->type < SpanNode ? slot->type : SpanNode) {
	  case FldEnd: {
		// this case only occurs with binaryFlds

		ARTFldEnd *fldEndNode = getObj(map, *slot);

		// do we need to finish the search key field?

		if (fldLen) {
		  slot = fldEndNode->sameFld;
		  stack->ch = 256;
		  continue;
		}

		if (cursor) {
		  lastFld = dbCursor->keyLen;
		  fldLen = key[offset] << 8 | key[offset + 1];
		  dbCursor->keyLen += 2;
		  stack->ch = 256;
		  offset += 2;
		}

		slot = fldEndNode->nextFld;
		continue;
	  }

/*	  case KeyUniq: {
	   	  ARTKeyUniq* keyUniqNode = getObj(map, *slot);

		  if (stack->off == uniqueLen)
			slot = keyUniqNode->dups;
		  else
			slot = keyUniqNode->next;

		  if (cursor)
			stack->ch = 256;

		  continue;
		}
*/
	  case KeyEnd: {
		if (slot->addr) {	// do key bytes fork here?
	   	  ARTKeyEnd* keyEndNode = getObj(map, *slot);
		  slot = keyEndNode->next;

		  if (cursor)
			stack->ch = 256;

		  continue;
		}

		// otherwise our key isn't here

		break;
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

		dbCursor->keyLen += spanMax;
		slot = spanNode->next;
		offset += spanMax;

		if (binaryFlds)
		  fldLen -= spanMax;

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
		  dbCursor->keyLen++;

		  if (binaryFlds)
			fldLen--;

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
		  dbCursor->keyLen++;

		  if (binaryFlds)
			fldLen--;

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
		  dbCursor->keyLen++;

		  if (binaryFlds)
			fldLen--;

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
		  dbCursor->keyLen++;

		  if (binaryFlds)
			fldLen--;

		  offset++;
		  continue;
		}

		// key byte not found

		break;
	  }

	  case UnusedSlot: {
		dbCursor->state = CursorRightEof;
		return DB_OK;
	  }
	}  // end switch

	break;
  }  // end while (offset < keylen)

  memcpy (cursor->key, key, dbCursor->keyLen);

  //  did we end on a complete key?

  if (slot->type == KeyEnd)
  	dbCursor->state = CursorPosAt;
  else
  	dbCursor->state = CursorPosBefore;

  //  add the terminal node to the cursor

  if (cursor->depth < MAX_cursor)
	stack = cursor->stack + cursor->depth++;
  else
	return DB_ERROR_cursoroverflow;

  stack->slot->bits = slot->bits;
  stack->off = dbCursor->keyLen;
  stack->addr = slot;
  stack->ch = -1;
  return DB_OK;
}
