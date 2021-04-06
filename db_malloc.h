#pragma once
#include "base64.h"
#include "db.h"

/**
 *	memory allocation
 */

uint64_t db_rawAlloc(uint32_t amt, bool zero);
uint32_t db_rawSize(DbAddr addr);
void *db_rawObj(DbAddr addr);

void *db_malloc(uint32_t amt, bool zero);
void db_free(void *obj);
uint32_t db_size(void *obj);
DbAddr db_memAddr(void *obj);
uint32_t db_memSize(void *mem);
void *db_memObj(DbAddr addr);
void db_memFree(DbAddr addr);
