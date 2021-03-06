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

	map->arena->freeFrame->bits = head.bits;
	return true;
}

//	obtain available frame

uint64_t allocFrame(DbMap *map) {
Frame *frame;
DbAddr slot;

	lockLatch(map->arena->freeFrame->latch);

	while (!(slot.bits = getFreeFrame(map)))
		if (!initFreeFrame (map)) {
			unlockLatch(map->arena->freeFrame->latch);
			return false;
		}

	unlockLatch(map->arena->freeFrame->latch);
	frame = getObj(map, slot);
	frame->next.bits = 0;
	frame->prev.bits = 0;

	slot.type = FrameType;
	return slot.bits;
}

//  Add empty frame to frame free-list

void returnFreeFrame(DbMap *map, DbAddr free) {
Frame *frame;

	lockLatch(map->arena->freeFrame->latch);

	// space in current free-list frame?

	if (map->arena->freeFrame->addr)
	  if (map->arena->freeFrame->nslot < FrameSlots) {
		frame = getObj(map, *map->arena->freeFrame);
		frame->slots[map->arena->freeFrame->nslot++] = free.bits;
		unlockLatch(map->arena->freeFrame->latch);
		return;
	  }

	// otherwise turn free into new freeFrame frame

	frame = getObj(map, free);
	frame->next.bits = map->arena->freeFrame->bits;
	frame->prev.bits = 0;

	//	add free frame to freeFrame list
	//	and remove mutex

	map->arena->freeFrame->bits = free.addr;
}

//  Add value to free frame

bool addSlotToFrame(DbMap *map, DbAddr *free, DbAddr *wait, uint64_t value) {
bool resp;

	//  this latch covers both free and wait

	lockLatch(free->latch);
	resp = addValuesToFrame(map, free, wait, &value, 1);
	unlockLatch(free->latch);
	return resp;
}

//	Add vector of values to free frame
//	call with free slot locked.

bool addValuesToFrame(DbMap *map, DbAddr *free, DbAddr *wait, uint64_t *values, int count) {
DbAddr slot2;
Frame *frame;


  if (free->addr)
	frame = getObj(map, *free);

  while (count--) {
	//  space in current frame?

	if (free->addr && free->nslot < FrameSlots) {
	  frame->slots[free->nslot++] = *values++;
	  continue;
	}

	//	allocate new frame and
	//  push frame onto free list
	//	and possibly wait list.

	//  n.b. all frames on wait list are full frames.

	if (!(slot2.bits = allocFrame(map)))
		return false;

	frame = getObj(map, slot2);
	frame->prev.bits = 0;

	//	link new frame onto tail of wait chain

	if ((frame->next.bits = free->addr)) {
		Frame *prevFrame = getObj(map, *free);
		prevFrame->timestamp = map->arena->nxtTs;
		prevFrame->prev.bits = slot2.bits;
		prevFrame->prev.nslot = FrameSlots;
	}	

	//  initialize head of wait queue

	if (wait && !wait->addr)
		wait->bits = slot2.bits;


	// install new frame at list head, with lock set

	slot2.nslot = 1;
	free->bits = slot2.bits | ADDR_MUTEX_SET;
	frame->slots[0] = *values++;
  }

  return true;
}

//  pull free frame from free list
//	call with freeFrame locked

uint64_t getFreeFrame(DbMap *map) {
uint64_t addr;
Frame *frame;

	if (!map->arena->freeFrame->addr)
		return 0;

	frame = getObj(map, *map->arena->freeFrame);

	// are there available free frames?

	if (map->arena->freeFrame->nslot)
		return frame->slots[--map->arena->freeFrame->nslot] & ADDR_BITS;

	// is there more than one freeFrame?

	if (!frame->next.bits)
		return 0;

	addr = map->arena->freeFrame->addr;
	frame->next.nslot = FrameSlots;
	frame->next.mutex = 1;

	map->arena->freeFrame->bits = frame->next.bits;
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

//	pull frame from tail of wait queue to free list
//	call with free list empty and latched

bool getNodeWait(DbMap *map, DbAddr* free, DbAddr* wait) {
bool result = false;
Frame *frame;

	if (!wait || !wait->addr)
		return false;

	frame = getObj(map, *wait);

	if (frame->timestamp >= map->arena->lowTs)
		map->arena->lowTs = scanHandleTs(map);

	//	move waiting frames to free list
	//	after the frame timestamp is
	//	less than the lowest handle timestamp

	while (frame->prev.bits && frame->timestamp < map->arena->lowTs) {
		Frame *prevFrame = getObj(map, frame->prev);

		// the tail frame previous must be already filled for it to
		// be installed as the new tail

		if (!prevFrame->prev.addr)
			break;

		//	return empty free frame

		if (free->addr && !free->nslot)
			returnFreeFrame(map, *free);

		// pull the frame from tail of wait queue
		// and place at the head of the free list

		free->bits = wait->bits | ADDR_MUTEX_SET;
		free->nslot = FrameSlots;
		result = true;

		//  install new tail node
		//	and zero its next

		wait->bits = frame->prev.addr;
		prevFrame->next.bits = 0;

		// advance to next candidate

		frame = prevFrame;
	}

	return result;
}

//	initialize frame of available ObjId

bool initObjIdFrame(DbMap *map, DbAddr *free) {
uint64_t dup = FrameSlots;
uint64_t max, bits;
Frame *frame;

	lockLatch(map->arena->mutex);

	while (true) {
	  max = map->arena->segs[map->arena->objSeg].size -
		map->arena->segs[map->arena->objSeg].nextId.off * map->objSize;
	  max -= dup * map->objSize;

	  if (map->arena->segs[map->arena->objSeg].nextObject.off * 16ULL < max )
		break;

	  if (map->arena->objSeg < map->arena->currSeg) {
		map->arena->segs[++map->arena->objSeg].nextId.off++;
		continue;
	  }

	  if (!newSeg(map, dup * map->objSize))
	  	return false;

	  map->arena->objSeg = map->arena->currSeg;
	  map->arena->segs[map->arena->objSeg].nextId.off++;
	  break;
	}

	// allocate a batch of ObjIds

	map->arena->segs[map->arena->objSeg].nextId.off += dup;
	bits = map->arena->segs[map->arena->objSeg].nextId.bits;
	unlockLatch(map->arena->mutex);

	if (!free->addr)
	  if (!(free->addr = allocFrame(map))) {
		unlockLatch(map->arena->mutex);
		return false;
	  }

	free->type = FrameType;
	free->nslot = FrameSlots;

	frame = getObj(map, *free);
	frame->next.bits = 0;
	frame->prev.bits = 0;

	while (dup--)
		frame->slots[dup] = bits - dup;

	return true;
}
