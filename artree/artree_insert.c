#include "artree.h"

typedef enum {
	ContinueSearch,
	EndSearch,
	RetrySearch,
	RestartSearch,
	ErrorSearch
} ReturnState;

ReturnState insertKeySpan(volatile ARTSpan*, InsertParam *);
ReturnState insertKeyNode4(volatile ARTNode4*, InsertParam *);
ReturnState insertKeyNode14(volatile ARTNode14*, InsertParam *);
ReturnState insertKeyNode64(volatile ARTNode64*, InsertParam *);
ReturnState insertKeyNode256(volatile ARTNode256*, InsertParam *);

#ifdef DEBUG
uint64_t nodeAlloc[64];
uint64_t nodeFree[64];
uint64_t nodeWait[64];
#endif

uint64_t artAllocateNode(Handle *index, int type, uint32_t size) {
DbAddr *free = listFree(index,type);
DbAddr *wait = listWait(index,type);

#ifdef DEBUG
	atomicAdd64(&nodeAlloc[type], 1ULL);;
#endif
	return allocObj(index->map, free, wait, type, size, true);
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

// fill in the slot with span node
//	with remaining key bytes
//	return false if out of memory

bool fillKey(InsertParam *p, volatile DbAddr *slot) {
ARTSpan *spanNode;
DbAddr fill[1];
uint32_t len;

  p->slot = fill;

  while ((len = (p->keyLen - p->off))) {
	if (p->binaryFlds && !p->fldLen) {
	  if ((p->slot->bits = artAllocateNode(p->index, FldEnd, sizeof(ARTFldEnd)))) {
		ARTFldEnd *fldEndNode = getObj(p->index->map, *p->slot);
		p->slot = fldEndNode->nextFld;
	  }

	  p->fldLen = p->key[p->off] << 8 | p->key[p->off + 1];
	  p->off += 2;
	  continue;
	}

	if (p->binaryFlds)
	  if (len > p->fldLen)
		len = p->fldLen;

	if (len > 256)
	  len = 256;

	if ((p->slot->bits = allocSpanNode(p, len)))
	  spanNode = getObj(p->index->map, *p->slot);
	else
	  return false;

	p->slot->nbyte = len - 1;
	memcpy(spanNode->bytes, p->key + p->off, len);

	p->slot = spanNode->next;
	p->off += len;

	if (p->binaryFlds)
	  p->fldLen -= len;
  }

  slot->bits = fill->bits;
  return true;
}

DbStatus artInsertKey( Handle *index, void *key, uint32_t keyLen, uint64_t suffixValue) {
uint8_t keyBuff[MAX_key];
ArtIndex *artIndex;
bool pass = false;
InsertParam p[1];
DbAddr slot;

	artIndex = (ArtIndex *)(index->map->arena + 1);

	if (keyLen > MAX_key)
		return DB_ERROR_keylength;

    memcpy(keyBuff, key, keyLen);
    keyLen += store64(keyBuff, keyLen, suffixValue, artIndex->base->binaryFlds);

	memset(p, 0, sizeof(p));
	p->binaryFlds = artIndex->base->binaryFlds;

	do {
        p->slot = artIndex->root;
        p->keyLen = keyLen;
        p->restart = false;
        p->key = keyBuff;
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

		//	duplicate key?

		if (p->slot->type == KeyEnd)
		  break;

		//  if not, splice in a KeyEnd node to end the key

		lockLatch(p->slot->latch);

		//	check duplicate again after getting lock

		if (p->slot->type == KeyEnd) {
		  unlockLatch(p->slot->latch);
		  break;
		}

		//	end the path with a zero-addr KeyEnd

		if (p->slot->type == UnusedSlot) {
		  p->slot->bits = (uint64_t)KeyEnd << TYPE_SHIFT;
		  break;
		}

		//	splice in a new KeyEnd node

		if ((slot.bits = artAllocateNode(p->index, KeyEnd, sizeof(ARTKeyEnd)))) {
		  ARTKeyEnd *keyEndNode = getObj(p->index->map, slot);
		  keyEndNode->next->bits = p->slot->bits & ~ADDR_MUTEX_SET;

		  p->slot->bits = slot.bits;
		  break;
		}

		unlockLatch(p->slot->latch);
		return DB_ERROR_outofmemory;
	} while (!p->stat && (pass = p->restart));

	if (p->stat)
		return p->stat;

	atomicAdd64(artIndex->base->numKeys, 1);
	return DB_OK;
}

bool artInsertParam(InsertParam *p) {
DbAddr slot;

	//	loop invariant: p->slot points
	//	to node to process next, or is empty
	//	and needs to be filled to continue key

	while (p->off < p->keyLen) {
	  ReturnState rt;

	  p->oldSlot->bits = p->slot->bits | ADDR_MUTEX_SET;
	  p->ch = p->key[p->off];
	  p->newSlot->bits = 0;
	  p->prev = p->slot;

	  //  begin a new field?

	  if (p->binaryFlds)
		if (!p->fldLen) {
		  ARTFldEnd *fldEndNode;

		  if (p->off) {			// splice-in a FldEnd?
		   if (p->slot->type == FldEnd) {
			  fldEndNode = getObj(p->index->map, *p->slot);
			  p->slot = fldEndNode->nextFld;
		   } else {
			lockLatch(p->slot->latch);

			// retry if node has changed.

			if (p->slot->bits != p->oldSlot->bits) {
			  unlockLatch(p->slot->latch);
			  continue;
			}

			if ((slot.bits = artAllocateNode(p->index, FldEnd, sizeof(ARTFldEnd)))) {
			  fldEndNode = getObj(p->index->map, slot);
			  fldEndNode->sameFld->bits = p->slot->bits & ~ADDR_MUTEX_SET;
	  		  p->slot->bits = slot.bits;
			  p->slot = fldEndNode->nextFld;
			}
		  }
		}

		p->fldLen = p->key[p->off] << 8 | p->key[p->off + 1];
		p->off += 2;
		continue;
      }

	  switch (p->oldSlot->type < SpanNode ? p->oldSlot->type : SpanNode) {
		case FldEnd: {
			ARTFldEnd *fldEndNode = getObj(p->index->map, *p->oldSlot);
			p->slot = fldEndNode->sameFld;
			continue;
		}
		case KeyUniq: {
			ARTKeyUniq *keyUniqNode = getObj(p->index->map, *p->oldSlot);
			p->slot = keyUniqNode->next;
			continue;
		}
		case KeyEnd: {
			if (p->oldSlot->addr) {		// do we continue?
				ARTKeyEnd *keyEndNode = getObj(p->index->map, *p->oldSlot);
				p->slot = keyEndNode->next;
				continue;
			}

			//  splice in a KeyEnd node to fork our key sequence

			if ((p->newSlot->bits = artAllocateNode(p->index, KeyEnd, sizeof(ARTKeyEnd)))) {
				ARTKeyEnd *keyEndNode = getObj(p->index->map, *p->newSlot);
				rt = fillKey(p, keyEndNode->next) ? EndSearch : ErrorSearch;
			} else
				rt = ErrorSearch;

			break;
		}
		case SpanNode: {
			ARTSpan *spanNode = getObj(p->index->map, *p->oldSlot);
			assert(spanNode->next->type);
			rt = insertKeySpan(spanNode, p);
			break;
		}
		case Array4: {
			ARTNode4 *radix4Node = getObj(p->index->map, *p->oldSlot);
			rt = insertKeyNode4(radix4Node, p);
			break;
		}
		case Array14: {
			ARTNode14 *radix14Node = getObj(p->index->map, *p->oldSlot);
			rt = insertKeyNode14(radix14Node, p);
			break;
		}
		case Array64: {
			ARTNode64 *radix64Node = getObj(p->index->map, *p->oldSlot);
			rt = insertKeyNode64(radix64Node, p);
			break;
		}
		case Array256: {
			ARTNode256 *radix256Node = getObj(p->index->map, *p->oldSlot);
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
		  continue;

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
			if((!addSlotToFrame(p->index->map, listHead(p->index,slot.type), listWait(p->index,slot.type), slot.bits)))
			  return p->stat = DB_ERROR_outofmemory, false;
			#ifdef DEBUG
			atomicAdd64(&nodeFree[slot.type], 1ULL);;
			#endif
		  }

		  return true;
	  }	// end switch
	}	// end while (p->off < p->keyLen)

	return true;
}

ReturnState insertKeyNode4(volatile ARTNode4 *node, InsertParam *p) {
ARTNode14 *radix14Node;
uint32_t idx, out;
uint8_t bits;

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
		radix14Node = getObj(p->index->map, *p->newSlot);
	else {
		unlockLatch(p->slot->latch);
		return ErrorSearch;
	}

	for (idx = 0; idx < 4; idx++) {
		volatile DbAddr *slot = node->radix + idx;
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

ReturnState insertKeyNode14(volatile ARTNode14 *node, InsertParam *p) {
ARTNode64 *radix64Node;
uint32_t idx, out;
uint16_t bits;

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
		radix64Node = getObj(p->index->map,*p->newSlot);
	else
		return ErrorSearch;

	// initialize all the keys as currently unassigned.

	memset((void*)radix64Node->keys, 0xff, sizeof(radix64Node->keys));

	for (idx = 0; idx < 14; idx++) {
		volatile DbAddr *slot = node->radix + idx;
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

ReturnState insertKeyNode64(volatile ARTNode64 *node, InsertParam *p) {
ARTNode256 *radix256Node;
uint32_t idx, out;

	idx = node->keys[p->ch];

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
		radix256Node = getObj(p->index->map,*p->newSlot);
	else
		return ErrorSearch;

	for (idx = 0; idx < 256; idx++)
	  if (node->keys[idx] < 0xff) {
		volatile DbAddr *slot = node->radix + node->keys[idx];
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

ReturnState insertKeyNode256(volatile ARTNode256 *node, InsertParam *p) {

	//  is radix slot occupied?

	if (node->radix[p->ch].type) {
	  p->slot = node->radix + p->ch;
	  return ContinueSearch;
	}

	// lock and retry

	lockLatch(p->prev->latch);

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

ReturnState insertKeySpan(volatile ARTSpan *node, InsertParam *p) {
uint32_t len = p->oldSlot->nbyte + 1;
ARTSpan *overflowSpanNode;
uint32_t max = len, idx;
DbAddr *contSlot = NULL;
DbAddr *nxtSlot = NULL;
ARTNode4 *radix4Node;

	if (len > p->keyLen - p->off)
		len = p->keyLen - p->off;

	if (p->binaryFlds)
	  if (len > p->fldLen)
		len = p->fldLen;

	for (idx = 0; idx < len; idx++)
		if (p->key[p->off + idx] != node->bytes[idx])
			break;

	// did we use the entire span node exactly?

	if (idx == max) {
	  if (p->binaryFlds)
		p->fldLen -= idx - 1;
	  p->off += idx - 1;
	  p->slot = node->next;
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
			spanNode2 = getObj(p->index->map,*p->newSlot);
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
			radix4Node = getObj(p->index->map,*nxtSlot);
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
		ARTFldEnd *fldEndNode = getObj(p->index->map, *nxtSlot);
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

		overflowSpanNode = getObj(p->index->map, *nxtSlot);
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

