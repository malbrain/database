#include "../db.h"
#include "../db_object.h"
#include "../db_handle.h"
#include "../db_index.h"
#include "../db_arena.h"
#include "../db_frame.h"
#include "../db_map.h"
#include "artree.h"

typedef struct {
	volatile DbAddr *slot;
	volatile DbAddr *prev;
	DbAddr oldSlot[1];
	DbAddr newSlot[1];

	char *key;
	Handle *index;

	uint32_t keyLen;	// length of the key
	uint32_t depth;		// current tree depth
	uint32_t off;	 	// progress down the key bytes
	uint16_t fldLen;	// remaining field length
	uint8_t ch;			// current key character
	uint8_t binaryFlds;	// string fields are binary
} ParamStruct;

typedef enum {
	ContinueSearch,
	EndSearch,
	RetrySearch,
	RestartSearch,
	ErrorSearch
} ReturnState;

ReturnState insertKeySpan(volatile ARTSpan*, ParamStruct *);
ReturnState insertKeyNode4(volatile ARTNode4*, ParamStruct *);
ReturnState insertKeyNode14(volatile ARTNode14*, ParamStruct *);
ReturnState insertKeyNode64(volatile ARTNode64*, ParamStruct *);
ReturnState insertKeyNode256(volatile ARTNode256*, ParamStruct *);

uint64_t artAllocateNode(Handle *index, int type, uint32_t size) {
DbAddr *free = listFree(index,type);
DbAddr *tail = listTail(index,type);

	return allocObj(index->map, free, tail, type, size, true);
}

uint64_t allocSpanNode(ParamStruct *p, uint32_t len) {
int type = SpanNode, size = sizeof(ARTSpan);
int segments;

	if ( len > 8) {
		segments = (len + 15) / 16;
		size += segments * 16 - 8;
		type += segments;
	}

	return artAllocateNode(p->index, type, size);
}

// fill in the empty slot with span node
//	with remaining key bytes
//	return false if out of memory

bool fillKey(ParamStruct *p, volatile DbAddr *slot) {
DbAddr fill[1], *next = fill;
ARTSpan *spanNode;
uint32_t len;

  fill->bits = 0;

  while ( (len = (p->keyLen - p->off)) ) {
	if (p->binaryFlds && !p->fldLen) {
	  if ((next->bits = artAllocateNode(p->index, FldEnd, sizeof(ARTFldEnd)))) {
		ARTFldEnd *fldEndNode = getObj(p->index->map, *next);
		next = fldEndNode->nextFld;
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

	if ((next->bits = allocSpanNode(p, len)))
	  spanNode = getObj(p->index->map, *next);
	else
	  return false;

	next->nbyte = len - 1;
	spanNode->timestamp = allocateTimestamp(p->index->map->db, en_writer);
	memcpy(spanNode->bytes, p->key + p->off, len);

	next = spanNode->next;
	p->off += len;

	if (p->binaryFlds)
	  p->fldLen -= len;
  }

  //	finish the key with a KeyEnd node

  if (!(next->bits = artAllocateNode(p->index, KeyEnd, sizeof(ARTKeyEnd))))
	return false;

  //  splice into caller's tree

  slot->bits = fill->bits;
  return true;
}

DbStatus artInsertKey( Handle *index, void *key, uint32_t keyLen) {
bool restart, pass = false;
ParamStruct p[1];
DbAddr slot;

  memset(p, 0, sizeof(p));
  p->binaryFlds = index->map->arenaDef->params[IdxBinary].boolVal;

  do {
	restart = false;

	p->off = 0;
	p->depth = 0;
	p->key = key;
	p->index = index;
	p->keyLen = keyLen;
	p->fldLen = 0;
	p->slot = artIndexAddr(index->map)->root;

	//  we encountered a dead node

	if (pass) {
	  pass = false;
	  yield();
	}

	while (p->off < p->keyLen) {
	  ReturnState rt;

	  p->oldSlot->bits = p->slot->bits | ADDR_MUTEX_SET;
	  p->ch = p->key[p->off];
	  p->newSlot->bits = 0;
	  p->prev = p->slot;

	  //  begin a new field?

	  if (p->binaryFlds)
		if (!p->fldLen) {

		  // splice-in a FldEnd?

		  if (p->off && p->slot->type != FldEnd) {
			lockLatch(p->slot->latch);

			// retry if node has changed.

			if (p->slot->bits != p->oldSlot->bits) {
			  unlockLatch(p->slot->latch);
			  continue;
			}

			if ((slot.bits = artAllocateNode(p->index, FldEnd, sizeof(ARTFldEnd)))) {
			  ARTFldEnd *fldEndNode = getObj(index->map, *p->newSlot);
			  fldEndNode->nextFld->bits = p->slot->bits & ~ADDR_MUTEX_SET;
	  		  p->slot->bits = slot.bits;
			  p->slot = fldEndNode->sameFld;
			}
		  }

		  p->fldLen = p->key[p->off] << 8 | p->key[p->off + 1];
		  p->off += 2;
		  continue;
		}

	  switch (p->oldSlot->type < SpanNode ? p->oldSlot->type : SpanNode) {
		case KeyEnd: {
			ARTKeyEnd *keyEndNode = getObj(index->map, *p->oldSlot);
			p->slot = keyEndNode->next;
			p->depth++;
			continue;
		}
		case SpanNode: {
			ARTSpan *spanNode = getObj(index->map, *p->oldSlot);
			rt = insertKeySpan(spanNode, p);
			break;
		}
		case Array4: {
			ARTNode4 *radix4Node = getObj(index->map, *p->oldSlot);
			rt = insertKeyNode4(radix4Node, p);
			break;
		}
		case Array14: {
			ARTNode14 *radix14Node = getObj(index->map, *p->oldSlot);
			rt = insertKeyNode14(radix14Node, p);
			break;
		}
		case Array64: {
			ARTNode64 *radix64Node = getObj(index->map, *p->oldSlot);
			rt = insertKeyNode64(radix64Node, p);
			break;
		}
		case Array256: {
			ARTNode256 *radix256Node = getObj(index->map, *p->oldSlot);
			rt = insertKeyNode256(radix256Node, p);
			break;
		}
		case UnusedSlot: {
			// note this only occurs on the initial insert
			// into an empty tree

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
			return DB_ERROR_indexnode;

	  }  // end switch

	  switch (rt) {
		case ErrorSearch:		//	out of memory error
		  return DB_ERROR_outofmemory;

		case RetrySearch:
		  continue;

		case RestartSearch:
		  restart = true;
		  break;

		case ContinueSearch:
		  if (p->binaryFlds)
			p->fldLen--;

		  p->off++;
		  p->depth++;
		  continue;

		case EndSearch:
		  // is there a new node, or a change?

		  if (!p->newSlot->bits) {
			unlockLatch(p->prev->latch);
			return DB_OK;
		  }

		  // install new node value

		  slot.bits = p->prev->bits;
		  p->prev->bits = p->newSlot->bits;

		  // add old slot to free/wait list

		  if (slot.type)
			if((!addSlotToFrame(index->map, listHead(index,slot.type), listTail(index,slot.type), slot.bits)))
			  return DB_ERROR_outofmemory;

		  return DB_OK;
	  }  // end switch

	  break;	// only restart comes here

	}	// end while (p->off < p->keyLen)

	if (restart) {
	  pass = true;
	  continue;
	}

	// does p->slot continue with another key?
	//	return if not

	if (p->slot->type == KeyEnd)
	  return DB_OK;

	// if so, splice in a KeyEnd node to end our key
	//	and continue with another existing key

	lockLatch(p->slot->latch);

	if (p->slot->type == KeyEnd) {
	  unlockLatch(p->slot->latch);
	  return DB_OK;
	}

	if ((slot.bits = artAllocateNode(p->index, KeyEnd, sizeof(ARTKeyEnd)))) {
	  ARTKeyEnd *keyEndNode = getObj(p->index->map, slot);
	  keyEndNode->next->bits = p->slot->bits & ~ADDR_MUTEX_SET;
	  p->slot->bits = slot.bits;
	  return DB_OK;
	} else
	  return DB_ERROR_outofmemory;
  } while (restart);

  return DB_OK;
}

ReturnState insertKeyNode4(volatile ARTNode4 *node, ParamStruct *p) {
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

	radix14Node->timestamp = node->timestamp;

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

		*slot->latch = slot->type | KILL_BIT;
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

ReturnState insertKeyNode14(volatile ARTNode14 *node, ParamStruct *p) {
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
	radix64Node->timestamp = node->timestamp;

	for (idx = 0; idx < 14; idx++) {
		volatile DbAddr *slot = node->radix + idx;
		lockLatch(slot->latch);

		if (!slot->kill) {
#ifdef _WIN32
			_BitScanForward64((DWORD *)&out, ~radix64Node->alloc);
#else
			out = __builtin_ctzl(~radix64Node->alloc);
#endif
			radix64Node->alloc |= 1ULL << out;
			radix64Node->radix[out].bits = slot->bits & ~ADDR_MUTEX_SET;
			radix64Node->keys[node->keys[idx]] = out;
		}

		// clear mutex & set kill bits

		*slot->latch = slot->type | KILL_BIT;
	}
#ifdef _WIN32
	_BitScanForward64((DWORD *)&out, ~radix64Node->alloc);
#else
	out = __builtin_ctzl(~radix64Node->alloc);
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

ReturnState insertKeyNode64(volatile ARTNode64 *node, ParamStruct *p) {
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
		out = __builtin_ctzl(~node->alloc);
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

	radix256Node->timestamp = node->timestamp;

	for (idx = 0; idx < 256; idx++)
	  if (node->keys[idx] < 0xff) {
		volatile DbAddr *slot = node->radix + node->keys[idx];
		lockLatch(slot->latch);
		if (!slot->kill) {
			radix256Node->radix[idx].bits = slot->bits & ~ADDR_MUTEX_SET;
			p->newSlot->nslot++;
		}

		// clear mutex & set kill bits

		*slot->latch = slot->type | KILL_BIT;
	  }

	// fill in the rest of the key bytes into Span nodes

	if (p->binaryFlds)
	  p->fldLen--;

	p->newSlot->nslot++;
	p->off++;

	return fillKey(p, radix256Node->radix + p->ch) ? EndSearch : ErrorSearch;
}

ReturnState insertKeyNode256(volatile ARTNode256 *node, ParamStruct *p) {

	//  is radix slot occupied?

	p->slot = node->radix + p->ch;

	if (node->radix[p->ch].type)
	  return ContinueSearch;

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

	if (!fillKey(p, p->slot))
		return ErrorSearch;

	//	increment count of used radix slots

	p->prev->nslot++;
	return EndSearch;
}

ReturnState insertKeySpan(volatile ARTSpan *node, ParamStruct *p) {
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

	if (p->binaryFlds)
		p->fldLen -= idx;

	p->off += idx;

	// copy matching prefix bytes to a new span node

	if (idx) {
		ARTSpan *spanNode2;

		if ((p->newSlot->bits = allocSpanNode(p, idx)))
			spanNode2 = getObj(p->index->map,*p->newSlot);
		else
			return ErrorSearch;

		memcpy(spanNode2->bytes, (void *)node->bytes, idx);
		spanNode2->timestamp = node->timestamp;
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

	if (p->off < p->keyLen) {
		if ( (nxtSlot->bits = artAllocateNode(p->index, Array4, sizeof(ARTNode4))) )
			radix4Node = getObj(p->index->map,*nxtSlot);
		else
			return ErrorSearch;

		// fill in first radix element with first of the remaining span bytes

		radix4Node->timestamp = node->timestamp;
		radix4Node->keys[0] = node->bytes[idx++];
		radix4Node->alloc |= 1;
		nxtSlot = radix4Node->radix + 0;

		// fill in second radix element with next byte of our search key

		if (p->binaryFlds)
			p->fldLen--;

		radix4Node->keys[1] = p->key[p->off++];
		radix4Node->alloc |= 2;
		contSlot = radix4Node->radix + 1;
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
		memcpy(overflowSpanNode->bytes, (char *)node->bytes + idx, max - idx);
		overflowSpanNode->next->bits = node->next->bits & ~ADDR_MUTEX_SET;
		overflowSpanNode->timestamp = node->timestamp;
	} else {
		// append second span node after span or radix node from above
		// otherwise hook remainder of the trie into the
		// span or radix node's next slot (nxtSlot)

		nxtSlot->bits = node->next->bits & ~ADDR_MUTEX_SET;
	}

	// turn off mutex & set kill bits

	*node->next->latch = node->next->type | KILL_BIT;
	assert(p->newSlot->bits > 0);

	// fill in the rest of the key into the radix or overflow span node

	return fillKey(p, contSlot) ? EndSearch : ErrorSearch;
}

