#include "artree.h"

typedef enum {
	ContinueSearch,
	RawContinue,
	EndSearch,
	RetrySearch,
	RestartSearch,
	ErrorSearch
} ReturnState;

ReturnState insertKeySpan(ARTSpan*, InsertParam *);
ReturnState insertKeyNode4(ARTNode4*, InsertParam *);
ReturnState insertKeyNode14(ARTNode14*, InsertParam *);
ReturnState insertKeyNode64(ARTNode64*, InsertParam *);
ReturnState insertKeyNode256(ARTNode256*, InsertParam *);

extern bool stats;
uint64_t nodeAlloc[64];
uint64_t nodeFree[64];
uint64_t nodeWait[64];

uint64_t artAllocateNode(Handle *index, int type, uint32_t size) {
DbMap *idxMap = MapAddr(index);
DbAddr *free = listFree(index,type);
DbAddr *wait = listWait(index,type);

	if( stats )
		atomicAdd64(&nodeAlloc[type], 1ULL);

	return allocObj(idxMap, free, wait, type, size, true);
}

uint64_t allocSpanNode(InsertParam *p, uint32_t len) {
int type = SpanNode, size = sizeof(ARTSpan);
int segments;

	assert(len > 0);
	assert(len <= 256);

	//	additional segments beyond first one

	if ( len > 8) {
		segments = (len + 15) / 16;
		size += segments * 16 - 8;
		type += segments;
	}

	return artAllocateNode(p->index, type, size);
}

// fill in the given slot with span nodes
//	containing the remaining key bytes

//	return false if out of memory

bool fillKey(InsertParam *p, DbAddr *first) {
DbAddr fill[1], *slot;
ARTSpan *spanNode;
uint32_t len;

  if( p->keyLen == p->off )
	  return true;

  fill->bits = 0;
  slot = fill;

  while ((len = (p->keyLen - p->off))) {
	if (p->binaryFlds && !p->fldLen) {
	  p->fldLen = p->key[p->off] << 8 | p->key[p->off + 1];

	  p->off += 2;
	  continue;
	}

	if (p->binaryFlds)
	  if (len > p->fldLen)
		len = p->fldLen;

	if (len > 256)
	  len = 256;

	if ((slot->bits = allocSpanNode(p, len)))
	  spanNode = getObj(p->idxMap, *slot);
	else
	  return false;

	slot->nbyte = len - 1;
	memcpy(spanNode->bytes, p->key + p->off, len);

	slot = spanNode->next;
	p->off += len;

	if (p->binaryFlds)
	  p->fldLen -= len;

	if (p->binaryFlds && !p->fldLen) {
	  if ((slot->bits = artAllocateNode(p->index, FldEnd, sizeof(ARTFldEnd)))) {
		ARTFldEnd *fldEndNode = getObj(p->idxMap, *slot);
		slot = fldEndNode->nextFld;
	  } else
		return false;
	}
  }

  //  install the span chain

  first->bits = fill->bits;
  p->slot = slot;
  return true;
}

// basic insert of key value

DbStatus artInsertKey( Handle *index, DbKeyBase *kv, uint8_t lvl) {
DbMap *idxMap = MapAddr(index);
ARTKeyEnd *keyEndNode;
ArtIndex *artIndex;
bool pass = false;
DbAddr keyEndSlot;
InsertParam p[1];
volatile DbAddr* install;

	artIndex = artindex(idxMap);

	if (kv->keyLen > MAX_key)
		return DB_ERROR_keylength;

	do {
		memset (p, 0, sizeof (p));

		p->binaryFlds = artIndex->dbIndex->binaryFlds;
        p->slot = artIndex->root;
        p->keyLen = kv->keyLen;
        p->restart = false;
        p->idxMap = idxMap;
        p->key = getObj(idxMap, kv->bytes);
        p->index = index;
		p->fldLen = 0;
		p->off = 0;
	
		//  we encountered a dead node

		if (pass) {
			pass = false;
			yield();
		}

		if (!artInsertParam(p))
			continue;

	} while (!p->stat && (pass = p->restart));

	//	if necessary splice in a new KeyEnd node
	//	begin by locking pointer

	if (p->stat)
		return p->stat;

	lockLatch(p->slot->latch);

	if (p->slot->type == KeyEnd)
		keyEndSlot.bits = p->slot->bits & ~ADDR_MUTEX_SET;
	else if( !(keyEndSlot.bits = artAllocateNode (p->index, KeyEnd, sizeof (ARTKeyEnd)))) {
	  unlockLatch (p->slot->latch);
	  return DB_ERROR_outofmemory;
	}

	keyEndNode = getObj(p->idxMap, keyEndSlot);

	if (p->slot->type != KeyEnd)
	  keyEndNode->next->bits = p->slot->bits & ~ADDR_MUTEX_SET;
	
	install = p->slot;

	do {
	    //	add suffix to keyEnd node

		p->slot = keyEndNode->suffix;

		p->key = kv->bytes + kv->keyLen;
        p->keyLen = kv->suffixLen;
        p->restart = false;
		p->binaryFlds = 0;
		p->fldLen = 0;
		p->off = 0;

		//  we encountered a dead node

		if (pass) {
			pass = false;
			yield();
		}

		if (!artInsertParam(p))
			continue;

	} while (!p->stat && (pass = p->restart));

	if (p->stat)
		return p->stat;

	//	duplicate suffix?

	if (p->slot->type == SuffixEnd )
		return DB_ERROR_duplicate_suffix;

	p->slot->type = SuffixEnd;

	//	unlock/install keyend node

	install->bits = keyEndSlot.bits;
    atomicAdd64(artIndex->dbIndex->numKeys, 1);
	return DB_OK;
}

bool artInsertParam(InsertParam *p) {
DbAddr slot;

	//	loop invariant: p->slot points
	//	to node to process next, or is empty
	//	and needs to be filled to continue key

	while (p->off < p->keyLen ) {
	  ReturnState rt;

	  p->oldSlot->bits = p->slot->bits | ADDR_MUTEX_SET;
	  p->newSlot->bits = 0;
	  p->prev = p->slot;

	  //  begin a new field?

	  if (p->binaryFlds && !p->fldLen) {
		p->fldLen = p->key[p->off] << 8 | p->key[p->off + 1];
		p->off += 2;
		continue;
      }

	  p->ch = p->key[p->off];

	  switch (p->oldSlot->type < SpanNode ? p->oldSlot->type : SpanNode) {
		case FldEnd: {
			ARTFldEnd *fldEndNode = getObj(p->idxMap, *p->oldSlot);
			p->slot = fldEndNode->sameFld;
			rt = RawContinue;
			break;
		}
		case KeyEnd: {
			ARTKeyEnd *keyEndNode = getObj(p->idxMap, *p->oldSlot);
			p->slot = keyEndNode->next;
			rt = RawContinue;
			break;
		}
		case SpanNode: {
			ARTSpan *spanNode = getObj(p->idxMap, *p->oldSlot);
			rt = insertKeySpan(spanNode, p);
			break;
		}
		case Array4: {
			ARTNode4 *radix4Node = getObj(p->idxMap, *p->oldSlot);
			rt = insertKeyNode4(radix4Node, p);
			break;
		}
		case Array14: {
			ARTNode14 *radix14Node = getObj(p->idxMap, *p->oldSlot);
			rt = insertKeyNode14(radix14Node, p);
			break;
		}
		case Array64: {
			ARTNode64 *radix64Node = getObj(p->idxMap, *p->oldSlot);
			rt = insertKeyNode64(radix64Node, p);
			break;
		}
		case Array256: {
                  ARTNode256 *radix256Node = getObj(p->idxMap, *p->oldSlot);
			rt = insertKeyNode256(radix256Node, p);
			break;
		}
		case UnusedSlot: {
			lockLatch(p->slot->latch);

			// retry if node has changed.

			if (p->slot->bits != p->oldSlot->bits) {
			  unlockLatch(p->slot->latch);
			  continue;
			}

			rt = fillKey(p, p->newSlot) ? EndSearch : ErrorSearch;
			break;
		}
		default:
			p->stat = DB_ERROR_indexnode;
			return false;

	  }  // end switch

	  switch (rt) {
		case ErrorSearch:		//	out of memory error
		  p->stat = DB_ERROR_outofmemory;
		  return false;

		case RetrySearch:		//  retry current key byte
		  continue;

		case RestartSearch:		//	restart insert from beginning
		  p->restart = true;
		  return false;

		case ContinueSearch:	//	continue to next key byte
		  if (p->binaryFlds)
			p->fldLen--;

		  p->off++;
		  break;

		case RawContinue:
		  break;

		case EndSearch:			//  p->slot points to key continuation
		  if (!p->newSlot->bits) {
			unlockLatch(p->prev->latch);
			return true;
		  }

		  // install new node value
		  // and recycle old node

		  slot.bits = p->prev->bits;
		  p->prev->bits = p->newSlot->bits;

		  // add old node to free/wait list

		  if (slot.type && slot.addr) {
                    if ((!addSlotToFrame(
                            p->idxMap, listHead(p->index, slot.type),
                            listWait(p->index, slot.type), slot.bits)))
			  return p->stat = DB_ERROR_outofmemory, false;

			if( stats )
			  atomicAdd64(&nodeFree[slot.type], 1ULL);;
		  }

		  return true;
	  }	// end switch

	  //  insert FldEnd node?

	  if( !p->binaryFlds || p->fldLen )
		  continue;

	  if ((slot.bits = artAllocateNode(p->index, FldEnd, sizeof(ARTFldEnd)))) {
            ARTFldEnd *fldEndNode = getObj(p->idxMap, slot);
		fldEndNode->sameFld->bits = p->slot->bits & ~ADDR_MUTEX_SET;

	  	p->slot->bits = slot.bits;
		p->slot = fldEndNode->nextFld;
	  } else
		return false;

	}	// end while (p->off < p->keyLen)

	return true;
}

ReturnState insertKeyNode4(ARTNode4 *node, InsertParam *p) {
ARTNode14 *radix14Node;
uint32_t idx, out;
uint8_t bits;

	// note the logic for continue consumes one char

	for (bits = node->alloc, idx = 0; bits && idx < 4; bits /= 2, idx++)
	  if (bits & 1)
		if (p->ch == node->keys[idx]) {
			p->slot = node->radix + idx;
			return ContinueSearch;
		}

	// obtain write lock on the node

	lockLatch(p->slot->latch);

	// restart if slot has been killed
	// or node has changed by another insert.

	if (p->slot->kill) {
		unlockLatch(p->slot->latch);
		return RestartSearch;
	}

	if (p->slot->bits != p->oldSlot->bits) {
		unlockLatch(p->slot->latch);
		return RetrySearch;
	}

	// retry search under lock
	// note the logic for continue consumes one char

	for (bits = node->alloc, idx = 0; bits && idx < 4; bits /= 2, idx++)
	  if (bits & 1)
		if (p->ch == node->keys[idx]) {
			unlockLatch(p->slot->latch);
			p->slot = node->radix + idx;
			return ContinueSearch;
		}

	// add to radix4 node if room

	if (node->alloc < 0xF) {
#ifdef _WIN32
		_BitScanForward((DWORD *)&idx, ~node->alloc);
#else
		idx = __builtin_ctz(~node->alloc);
#endif

		if (p->binaryFlds)
		  p->fldLen--;

		node->keys[idx] = p->ch;
		p->off++;

		if (!fillKey(p, node->radix + idx))
			return ErrorSearch;

		node->alloc |= 1 << idx;
		return EndSearch;
	}

	// the radix node is full, promote to the next larger size.

	if ( (p->newSlot->bits = artAllocateNode(p->index, Array14, sizeof(ARTNode14))) )
          radix14Node = getObj(p->idxMap, *p->newSlot);
	else {
		unlockLatch(p->slot->latch);
		return ErrorSearch;
	}

	for (idx = 0; idx < 4; idx++) {
		DbAddr *slot = node->radix + idx;
		lockLatch(slot->latch);

		if (!slot->kill) {
#ifdef _WIN32
			_BitScanForward((DWORD *)&out, ~radix14Node->alloc);
#else
			out = __builtin_ctz(~radix14Node->alloc);
#endif
			radix14Node->alloc |= 1 << out;
			radix14Node->radix[out].bits = slot->bits & ~ADDR_MUTEX_SET;
			radix14Node->keys[out] = node->keys[idx];
		}

		// clear mutex & set kill bits

		*slot->latch = slot->type << BYTE_SHIFT | KILL_BIT;
	}

#ifdef _WIN32
	_BitScanForward((DWORD *)&out, ~radix14Node->alloc);
#else
	out = __builtin_ctz(~radix14Node->alloc);
#endif

	if (p->binaryFlds)
	  p->fldLen--;

	radix14Node->keys[out] = p->ch;
	p->off++;

	// fill in rest of the key in span nodes

	if (!fillKey(p, radix14Node->radix + out))
		return ErrorSearch;

	radix14Node->alloc |= 1 << out;
	return EndSearch;
}

ReturnState insertKeyNode14(ARTNode14 *node, InsertParam *p) {
ARTNode64 *radix64Node;
uint32_t idx, out;
uint16_t bits;

	// note the logic for continue consumes one char

	for (bits = node->alloc, idx = 0; bits && idx < 14; bits /= 2, idx++)
	  if (bits & 1)
		if (p->ch == node->keys[idx]) {
			p->slot = node->radix + idx;
			return ContinueSearch;
		}

	// obtain write lock on the node

	lockLatch(p->slot->latch);

	// restart if slot has been killed
	// or node has changed.

	if (p->slot->kill) {
		unlockLatch(p->slot->latch);
		return RestartSearch;
	}

	if (p->slot->bits != p->oldSlot->bits) {
		unlockLatch(p->slot->latch);
		return RetrySearch;
	}

	//  retry search under lock

	// note the logic for continue consumes one char

	for (bits = node->alloc, idx = 0; bits && idx < 14; bits /= 2, idx++)
	  if (bits & 1)
		if (p->ch == node->keys[idx]) {
			unlockLatch(p->slot->latch);
			p->slot = node->radix + idx;
			return ContinueSearch;
		}

	// add to radix node if room

	if (node->alloc < 0x3fff) {
#ifdef _WIN32
		_BitScanForward((DWORD *)&idx, ~node->alloc);
#else
		idx = __builtin_ctz(~node->alloc);
#endif

		if (p->binaryFlds)
		  p->fldLen--;

		node->keys[idx] = p->ch;
		p->off++;

		if (!fillKey(p, node->radix + idx))
			return ErrorSearch;

		node->alloc |= 1 << idx;
		return EndSearch;
	}

	// the radix node is full, promote to the next larger size.

	if ( (p->newSlot->bits = artAllocateNode(p->index, Array64, sizeof(ARTNode64))) )
          radix64Node = getObj(p->idxMap, *p->newSlot);
	else
		return ErrorSearch;

	// initialize all the keys as currently unassigned.

	memset((void*)radix64Node->keys, 0xff, sizeof(radix64Node->keys));

	for (idx = 0; idx < 14; idx++) {
		DbAddr *slot = node->radix + idx;
		lockLatch(slot->latch);

		if (!slot->kill) {
#ifdef _WIN32
			_BitScanForward64((DWORD *)&out, ~radix64Node->alloc);
#else
			out = __builtin_ctzll(~radix64Node->alloc);
#endif
			radix64Node->alloc |= 1ULL << out;
			radix64Node->radix[out].bits = slot->bits & ~ADDR_MUTEX_SET;
			radix64Node->keys[node->keys[idx]] = out;
		}

		// clear mutex & set kill bits

		*slot->latch = slot->type << BYTE_SHIFT | KILL_BIT;
	}
#ifdef _WIN32
	_BitScanForward64((DWORD *)&out, ~radix64Node->alloc);
#else
	out = __builtin_ctzll(~radix64Node->alloc);
#endif

	if (p->binaryFlds)
	  p->fldLen--;

	radix64Node->keys[p->ch] = out;
	p->off++;

	// fill in rest of the key bytes into span nodes

	if (!fillKey(p, radix64Node->radix + out))
		return ErrorSearch;

	radix64Node->alloc |= 1ULL << out;
	return EndSearch;
}

ReturnState insertKeyNode64(ARTNode64 *node, InsertParam *p) {
ARTNode256 *radix256Node;
uint32_t idx, out;

	idx = node->keys[p->ch];

	// note the logic for continue consumes one char

	if (idx < 0xff ) {
		p->slot = node->radix + idx;
		return ContinueSearch;
	}

	// obtain write lock on the node

	lockLatch(p->slot->latch);

	// restart if slot has been killed
	// or node has changed.

	if (p->slot->kill) {
		unlockLatch(p->slot->latch);
		return RestartSearch;
	}

	if (p->slot->bits != p->oldSlot->bits) {
		unlockLatch(p->slot->latch);
		return RetrySearch;
	}

	//  retry under lock

	idx = node->keys[p->ch];

	// note the logic for continue consumes one char

	if (idx < 0xff ) {
		unlockLatch(p->slot->latch);
		p->slot = node->radix + idx;
		return ContinueSearch;
	}

	// if room, add to radix node

	if (node->alloc < 0xffffffffffffffffULL) {
#ifdef _WIN32
		_BitScanForward64((DWORD *)&out, ~node->alloc);
#else
		out = __builtin_ctzll(~node->alloc);
#endif
		if (p->binaryFlds)
		  p->fldLen--;

		p->off++;

		if (!fillKey(p, node->radix + out))
			return ErrorSearch;

		node->alloc |= 1ULL << out;
		node->keys[p->ch] = out;
		return EndSearch;
	}

	// the radix node is full, promote to the next larger size.

	if ( (p->newSlot->bits = artAllocateNode(p->index, Array256, sizeof(ARTNode256))) )
          radix256Node = getObj(p->idxMap, *p->newSlot);
	else
		return ErrorSearch;

	for (idx = 0; idx < 256; idx++)
	  if (node->keys[idx] < 0xff) {
		DbAddr *slot = node->radix + node->keys[idx];
		lockLatch(slot->latch);

		if (!slot->kill) {
			radix256Node->radix[idx].bits = slot->bits & ~ADDR_MUTEX_SET;
			p->newSlot->nslot++;
		}

		// clear mutex & set kill bits

		*slot->latch = slot->type << BYTE_SHIFT | KILL_BIT;
	  }

	// fill in the rest of the key bytes into Span nodes

	if (p->binaryFlds)
	  p->fldLen--;

	p->newSlot->nslot++;
	p->off++;

	return fillKey(p, radix256Node->radix + p->ch) ? EndSearch : ErrorSearch;
}

ReturnState insertKeyNode256(ARTNode256 *node, InsertParam *p) {

	//  is radix slot occupied?

	// note the logic for continue consumes one char

	if (node->radix[p->ch].type) {
	  p->slot = node->radix + p->ch;
	  return ContinueSearch;
	}

	// lock and retry

	lockLatch(p->prev->latch);

	// note the logic for continue consumes one char

	if (node->radix[p->ch].type) {
		unlockLatch(p->prev->latch);
		return ContinueSearch;
	}

	// fill-in empty radix slot

	if (p->binaryFlds)
	  p->fldLen--;

	p->off++;

	if (!fillKey(p, node->radix + p->ch))
		return ErrorSearch;

	//	increment count of used radix slots

	p->slot->nslot++;
	return EndSearch;
}

ReturnState insertKeySpan(ARTSpan *node, InsertParam *p) {
uint32_t len = p->oldSlot->nbyte + 1;
ARTSpan *overflowSpanNode;
uint32_t max = len, idx;
DbAddr *contSlot = NULL;
DbAddr *nxtSlot = NULL;
ARTNode4 *radix4Node;

	if (len > (uint16_t)(p->keyLen - p->off) )
		len = p->keyLen - p->off;

	if (p->binaryFlds)
	  if (len > p->fldLen)
		len = p->fldLen;

	//	count how many span bytes can be used

	for (idx = 0; idx < len; idx++)
		if (p->key[p->off + idx] != node->bytes[idx])
			break;

	// did we use the entire span node exactly?  If so continue search

	if (idx == max) {
	  if (p->binaryFlds)
		p->fldLen -= idx;
	  p->off += idx;
	  p->slot = node->next;
	  return RawContinue;
	}

	// obtain write lock on the node

	lockLatch(p->slot->latch);

	// restart if slot has been killed
	// or node has changed.

	if (p->slot->kill) {
		unlockLatch(p->slot->latch);
		return RestartSearch;
	}

	if (p->slot->bits != p->oldSlot->bits) {
		unlockLatch(p->slot->latch);
		return RetrySearch;
	}

	lockLatch(node->next->latch);

	if (node->next->kill) {
		unlockLatch(p->slot->latch);
		unlockLatch(node->next->latch);
		return RestartSearch;
	}

	// copy matching prefix bytes to a new span node

	if (idx) {
		ARTSpan *spanNode2;
		if (p->binaryFlds)
			p->fldLen -= idx;

		p->off += idx;
		len -= idx;

		if ((p->newSlot->bits = allocSpanNode(p, idx)))
                  spanNode2 = getObj(p->idxMap, *p->newSlot);
		else
			return ErrorSearch;

		memcpy(spanNode2->bytes, (void *)node->bytes, idx);
		p->newSlot->nbyte = idx - 1;
		nxtSlot = spanNode2->next;
		contSlot = nxtSlot;
	} else {
		// else replace the original span node with a radix4 node.
		// note that p->off < p->keyLen, which will set contSlot
		nxtSlot = p->newSlot;
	}

	// if we have more key bytes, insert a radix node after span1 and before
	// possible span2 for the next key byte and the next remaining original
	// span byte (if any).  note:  max > idx

	if (len) {
		if ( (nxtSlot->bits = artAllocateNode(p->index, Array4, sizeof(ARTNode4))) )
            radix4Node = getObj(p->idxMap, *nxtSlot);
		else
			return ErrorSearch;

		// fill in first radix element with first of the remaining span bytes

		radix4Node->keys[0] = node->bytes[idx++];
		radix4Node->alloc |= 1;
		nxtSlot = radix4Node->radix + 0;

		// fill in second radix element with next byte of our search key

		if (p->binaryFlds)
			p->fldLen--;

		radix4Node->keys[1] = p->key[p->off++];
		radix4Node->alloc |= 2;
		contSlot = radix4Node->radix + 1;
	} else {
	 if (p->off < p->keyLen) { 	//	we have a field end before KeyEnd
	  if ((nxtSlot->bits = artAllocateNode(p->index, FldEnd, sizeof(ARTFldEnd)))) {
             ARTFldEnd *fldEndNode = getObj(p->idxMap, *nxtSlot);
		contSlot = fldEndNode->nextFld;
		nxtSlot = fldEndNode->sameFld;
		p->fldLen = p->key[p->off] << 8 | p->key[p->off + 1];
		p->off += 2;
	  } else
		return ErrorSearch;
	 }
	}

	// place original span bytes remaining after the preceeding node
	// in a second span node after the radix or span node
	// i.e. fill in nxtSlot.

	if (max - idx) {
		if ((nxtSlot->bits = allocSpanNode(p, max - idx)))
			nxtSlot->nbyte = max - idx - 1;
		else
			return ErrorSearch;

		overflowSpanNode = getObj(p->idxMap, *nxtSlot);
		memcpy(overflowSpanNode->bytes, (uint8_t *)node->bytes + idx, max - idx);
		overflowSpanNode->next->bits = node->next->bits & ~ADDR_MUTEX_SET;
	} else {
		// append second span node after span or radix node from above
		// otherwise hook remainder of the trie into the
		// span or radix node's next slot (nxtSlot)

		nxtSlot->bits = node->next->bits & ~ADDR_MUTEX_SET;
	}

	// turn off mutex & set kill bits

	*node->next->latch = node->next->type << BYTE_SHIFT | KILL_BIT;
	assert(p->newSlot->bits > 0);

	// fill in the rest of the key into overflow span nodes

	return fillKey(p, contSlot) ? EndSearch : ErrorSearch;
}

