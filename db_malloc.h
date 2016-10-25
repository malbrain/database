#pragma once

/**
 *	memory allocation
 */

uint64_t db_rawAlloc(uint32_t amt, bool zero);
void *db_malloc(uint32_t amt, bool zero);
uint32_t db_rawSize(uint64_t bits);
void db_memFree (uint64_t bits);
void *db_memObj(uint64_t bits);
void db_free (void *obj);

