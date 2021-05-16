#pragma once

#define FrameSlots 125			// make sizeof(Frame) a multiple of 16

typedef struct {
	DbAddr next;				// next frame in queue
	DbAddr prev;				// prev frame in queue
	uint64_t timestamp;			// latest timestamp
	uint64_t slots[FrameSlots];	// array of waiting/free slots
} Frame;

void returnFreeFrame(DbMap *map, DbAddr slot);

uint64_t getNodeFromFrame (DbMap *map, DbAddr *queue);
uint32_t initObjFrame (DbMap *map, DbAddr *queue, uint32_t type, uint32_t size);
bool addValuesToFrame(DbMap *map, DbAddr *queue, uint64_t *values, int count);
bool addSlotToFrame(DbMap *map, DbAddr *queue, uint64_t value);
bool initObjIdFrame(DbMap *map, DbAddr *free);
void returnFreeFrame(DbMap *map, DbAddr free);


