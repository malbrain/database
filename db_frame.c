#include "base64.h"
#include "db.h"

#include "db_arena.h"
#include "db_map.h"
#include "db_frame.h"
#include "db_handle.h"
#include "db_object.h"

uint64_t getFreeFrame(DbMap *map);
uint64_t allocFrame( DbMap *map);

#ifdef DEBUG
extern uint64_t nodeAlloc[64];
extern uint64_t nodeFree[64];
extern uint64_t nodeWait[64];
#endif

//	fill in new frame with new available objects
//	call with free list head locked.
//	return false if out of memory

uint32_t initObjFrame(DbMap *map, DbAddr *free, uint32_t type, uint32_t size) {
uint32_t dup = FrameSlots, idx;
Frame *frame;
DbAddr slot;
	
	if (size * dup > 4096 * 4096)
		dup >>= 5;

	else if (size * dup > 1024 * 1024)
		dup >>= 3;

	else if (size * dup > 256 * 256)
		dup >>= 1;

	if (!(slot.bits = allocMap(map, size * dup)))
		return false;

	if (!free->addr)
	  if (!(free->addr = allocFrame(map)))
		return false;

	free->type = FrameType;
	free->nslot = dup;

	frame = getObj(map, *free);
	frame->next.bits = 0;
	frame->prev.bits = 0;

	slot.type = type;

	for (idx = dup; idx--; ) {
		frame->slots[idx] = slot.bits;
		slot.off += size >> 4;
	}

	return dup;
}

//  allocate frame full of empty frames for free list
//  call with freeFrame latched.

bool initFreeFrame (DbMap *map) {
uint64_t addr = allocMap (map, sizeof(Frame) * (FrameSlots + 1));
uint32_t dup = FrameSlots;
DbAddr head, slot;
Frame *frame;

	if (!addr)
		return false;

	head.bits = addr;
	head.type = FrameType;
	head.nslot = FrameSlots;
	head.mutex = 1;

	frame = getObj(map, head);
	frame->next.bits = 0;
	frame->prev.bits = 0;

	while (dup--) {
		addr += sizeof(Frame) >> 4;
		slot.bits = addr;
		slot.type = FrameType;
		slot.nslot = FrameSlots;
		frame->slots[dup] = slot.bits;
	}

	map->arena->sysFrame[freeFrame].freeFrame->bits = head.bits;
	return true;
}

//	obtain available frame

uint64_t allocFrame(DbMap *map) {
Frame *frame;
DbAddr slot, *free;

	free = map->arena->sysFrame[freeFrame].freeFrame;
	lockLatch(free->latch);

	while (!(slot.bits = getFreeFrame(map)))
		if (!initFreeFrame (map)) {
			unlockLatch(map->arena->sysFrame[freeFrame].freeFrame->latch);
			return false;
		}

	unlockLatch(free->latch);
	frame = getObj(map, slot);
	frame->next.bits = 0;
	frame->prev.bits = 0;

	slot.type = FrameType;
	return slot.bits;
}

/* sys frame entries
typedef enum {
	freeFrame = 1,			// frames
	freeObjId,					// 
} SYSFrame;
*/

//  Add empty frame to frame free-list

void returnFreeFrame(DbMap *map, DbAddr free) {
Frame *frame;
DbAddr *addr;

	addr = map->arena->sysFrame[freeFrame].freeFrame;
	lockLatch(addr->latch);

	// space in current free-list frame?

	if (addr)
	  if (addr->nslot < FrameSlots) {
			frame = getObj(map, *addr);
			frame->slots[addr->nslot++] = free.bits;
		  unlockLatch(addr->latch);
		  return;
	  }

	// otherwise turn free into new freeFrame frame

	frame = getObj(map, free);
	frame->next.bits = addr->bits;
	frame->prev.bits = 0;

	//	add free frame to freeFrame list
	//	and remove mutex

	addr->bits = free.addr;
}

//  Add value to frame

bool addSlotToFrame(DbMap *map, DbAddr *queue, uint64_t value) {
bool resp;

	//  this latch covers both free and tail

	lockLatch(queue->latch);
	resp = addValuesToFrame(map, queue, &value, 1);
	unlockLatch(queue->latch);
	return resp;
}

//	Add vector of values to free frame
//	call with free slot locked.

bool addValuesToFrame(DbMap *map, DbAddr *queue, uint64_t *values, int count) {
DbAddr slot2;
Frame *frame;

  if (queue->addr)
		frame = getObj(map, *queue);
  else
		frame = NULL;

  while (count--) {
	//  space in current frame?

	if (queue->addr && queue->nslot < FrameSlots) {
	  frame->slots[queue->nslot++] = values[count];
	  continue;
	}

	//	allocate new frame and
	//  push frame onto queue list

	if (!(slot2.bits = allocFrame(map)))
		return false;

	frame = getObj(map, slot2);
	frame->prev.bits = 0;

	//	link new frame onto tail of wait chain

	if ((frame->next.bits = queue->addr)) {
		Frame *prevFrame = getObj(map, *queue);
		prevFrame->timestamp = map->arena->nxtTs;
		prevFrame->prev.bits = slot2.bits;
		prevFrame->prev.nslot = FrameSlots;
	}	

	// install new frame at queue head, with lock set

	slot2.nslot = 1;
	queue->bits = slot2.bits | ADDR_MUTEX_SET;
	frame->slots[0] = values[count];
 }

 return true;
}

//  pull free frame from free list
//	call with freeFrame locked

uint64_t getFreeFrame(DbMap *map) {
uint64_t addr;
Frame *frame;
DbAddr *free, *tail;

	free = map->arena->sysFrame[freeFrame].freeFrame;
	tail = map->arena->sysFrame[freeFrame].tailFrame;

	if (!free->addr)
		return 0;

	frame = getObj(map, *free);

	// are there available free frames?

	if (free->nslot)
		return frame->slots[--free->nslot] & ADDR_BITS;

	// is there more than one freeFrame?

	if (!frame->next.bits)
		return 0;

	addr = free->addr;
	frame->next.nslot = FrameSlots;
	frame->next.mutex = 1;

	tail->bits = frame->next.bits;
	return addr;
}

//  pull available node from free object frame
//   call with free frame list head locked.

uint64_t getNodeFromFrame(DbMap *map, DbAddr* free) {
	while (free->addr) {
		Frame *frame = getObj(map, *free);

		//  are there available free objects?

		if (free->nslot)
			return frame->slots[--free->nslot];
	
		//  leave empty frame in place to collect
		//  new nodes

		if (frame->next.addr)
			returnFreeFrame(map, *free);
		else
			return 0;

		//	move the head of the free list
		//	to the next frame

		free->bits = frame->next.addr | ADDR_MUTEX_SET;
		free->nslot = FrameSlots;
	}

  return 0;
}

//	initialize frame of available ObjId/DocId

bool initObjIdFrame(DbMap *map, DbAddr *free) {
uint32_t dup, seg, off;
uint64_t max;
ObjId objId[1];
Frame *frame;
DbAddr addr;

	lockLatch(map->arena->mutex);

	if (!(addr.bits = free->addr))
	  if (!(addr.bits = allocFrame(map))) {
			unlockLatch(map->arena->mutex);
			return false;
	  }

	while (true) {
		seg = map->arena->objSeg;
		dup = FrameSlots;

	  max = map->arena->segs[seg].size * 16ULL -
		    map->arena->segs[seg].maxId * map->objSize;

		max -= dup * map->objSize;

		//  does it fit?

	  if (map->arena->segs[seg].nextObject.off * 16ULL < max )
			break;

		// move onto next segment

	  if (seg < map->arena->currSeg) {
			map->arena->objSeg++;
			continue;
	  }

		// build empty segment

	  if (!newSeg(map, dup * map->objSize)) {
			unlockLatch(map->arena->mutex);
			return false;
		}

	  map->arena->objSeg = map->arena->currSeg;
	  break;
	}

	// allocate a batch of ObjIds

	off = map->arena->segs[map->arena->objSeg].maxId += dup;

	objId->bits = off;
	objId->seg = seg;

	frame = getObj(map, addr);
	frame->next.bits = 0;
	frame->prev.bits = 0;

	free->addr = addr.addr;
	free->type = FrameType;
	free->nslot = dup;

	while (dup--) {
		objId->off = off - dup;
		frame->slots[dup] = objId->bits;
	}

	unlockLatch(map->arena->mutex);
	return true;
}

