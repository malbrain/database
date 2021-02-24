//  db_malloc.c

#include "base64.h"
#include "db.h"

#include "db_arena.h"
#include "db_object.h"
#include "db_handle.h"
#include "db_cursor.h"
#include "db_map.h"
#include "db_malloc.h"

#ifndef __APPLE__
#include <malloc.h>
#endif

bool mallocDebug;

//
// Raw object wrapper
//

typedef struct {
  DbAddr addr;
  uint32_t size;
} dbobj_t;

DbArena memArena[1];
DbMap memMap[1];

void memInit(void) {
  ArenaDef arenaDef[1];

  memMap->arena = memArena;
  memMap->db = memMap;

#ifdef _WIN32
  memMap->hndl = INVALID_HANDLE_VALUE;
#else
  memMap->hndl = -1;
#endif

  //	set up memory arena and handle addr ObjId

  memset(arenaDef, 0, sizeof(arenaDef));
  arenaDef->objSize = sizeof(DbAddr);

  initArena(memMap, arenaDef, "malloc", 6, NULL);
}

uint32_t db_memSize(void *obj) { 
dbobj_t *raw = obj;

  return raw[-1].size;
}

DbAddr db_memAddr(void *obj) {
dbobj_t *raw = obj;

  return raw[-1].addr;
}

void *db_memObj(DbAddr addr) {
  return (uint8_t *)getObj(memMap, addr) + sizeof(dbobj_t);
}

void db_memFree(DbAddr addr) {
  freeBlk(memMap, addr);
}

void db_free(void *obj) {
dbobj_t *raw = obj;
DbAddr addr = raw[-1].addr;

  if (mallocDebug) {
    raw[-1].addr.bits = 0xdeadbeef;

    if (addr.bits == 0xdeadbeef) {
      fprintf(stderr, "db_free: duplicate free!\n");
      exit(0);
    }
  }

  freeBlk(memMap, addr);
}

uint32_t db_size(void *obj) {
  dbobj_t *raw = obj;

  return raw[-1].size;
}

//	allocate object

void *db_malloc(uint32_t len, bool zeroit) {
  dbobj_t *mem;
  DbAddr addr;

  addr.bits = db_rawAlloc(len + sizeof(dbobj_t), zeroit);
  mem = getObj(memMap, addr);
  mem->addr.bits = addr.bits;
  mem->size = len;
  return mem + 1;
}

//	raw memory allocator

uint64_t db_rawAlloc(uint32_t amt, bool zeroit) {
  uint64_t bits;

  if ((bits = allocBlk(memMap, amt, zeroit))) return bits;

  fprintf(stderr, "db_rawAlloc: out of memory!\n");
  exit(1);
}

uint32_t db_rawSize(DbAddr addr) {
  int bits = addr.type / 2;
  uint32_t size = 1 << bits;

  // implement half-bit sizing
  //	if type is even.

  if (~addr.type & 1) size -= size / 4;

  return size;
}

void *db_rawObj(DbAddr addr) { return getObj(memMap, addr); }
