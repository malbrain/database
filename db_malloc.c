#include <errno.h>

#ifndef __APPLE__
#include <malloc.h>
#endif

#include "db.h"
#include "db_map.h"
#include "db_arena.h"
#include "db_malloc.h"

//
// Raw object wrapper
//

typedef struct {
	DbAddr addr;
} dbobj_t;

DbArena memArena[1];
DbMap memMap[1];

void memInit() {
ArenaDef arenaDef[1];

	memMap->arena = memArena;
	memMap->db = memMap;

#ifdef _WIN32
	memMap->hndl = INVALID_HANDLE_VALUE;
#else
	memMap->hndl = -1;
#endif

	memset (arenaDef, 0, sizeof(arenaDef));
	initArena(memMap, arenaDef, "malloc", 6, NULL);
}

uint32_t db_rawSize (uint64_t address) {
uint32_t size;
DbAddr addr;
int bits;

	addr.bits = address;
	bits = addr.type / 2;

	size = 1 << bits;

	// implement half-bit sizing
	//	if type is even.

	if (~addr.type & 1)
		size -= size / 4;

	return size;
}

void *db_memObj(uint64_t bits) {
DbAddr addr;

	addr.bits = bits;
	return getObj (memMap, addr);
}

void db_memFree (uint64_t bits) {
DbAddr addr;

	addr.bits = bits;
	freeBlk(memMap, addr);
}

void db_free (void *obj) {
dbobj_t *raw = obj;

	if (raw[-1].addr.kill) {
		fprintf(stderr, "Duplicate db_free\n");
		exit (1);
	}

	raw[-1].addr.kill = 1;
	freeBlk(memMap, raw[-1].addr);
}

//	raw memory allocator

uint64_t db_rawAlloc(uint32_t amt, bool zeroit) {
uint64_t bits;

	if ((bits = allocBlk(memMap, amt, zeroit)))
		return bits;

	fprintf (stderr, "db_rawAlloc: out of memory!\n");
	exit(1);
}

//	allocate object

void *db_malloc(uint32_t len, bool zeroit) {
dbobj_t *mem;
DbAddr addr;

	addr.bits = db_rawAlloc(len + sizeof(dbobj_t), zeroit);
	mem = getObj(memMap, addr);
	mem->addr.bits = addr.bits;
	return mem + 1;
}
