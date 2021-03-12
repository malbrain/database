#pragma once

#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE  1
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <memory.h>
#include <limits.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <sys/types.h>

#ifndef _WIN32
#include <unistd.h>
#endif

#define MIN(a,b) (((a)<(b))?(a):(b))

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

typedef enum {
	prngRandom,		// pure randomness all streams unique
	prngProcess,	// all streams are the same
	prngThread		// each thread has own stream, repeats across processes
} PRNG;

void mynrand48seed(uint16_t* nrandState, PRNG prng, uint16_t init);

int createB64(uint8_t* key, int size, unsigned short next[3]);

//  assemble binary values after key bytes

uint32_t append64(uint8_t *keyDest, int64_t *keyValues, uint8_t max, uint32_t avail);

//	fill in top down valueS from keyend
//	return number of values

uint8_t parse64(uint8_t *sourceKey, int64_t *keyValues, uint8_t max);

//	return 64 bit suffix value from key

uint64_t get64(uint8_t *key, uint32_t len);

//	calculate offset from right end of zone
//	and return suffix value

uint64_t zone64(uint8_t* key, uint32_t len, uint32_t zone);

// concatenate key with sortable 64 bit value
// returns number of bytes concatenated

uint32_t store64(uint8_t *key, uint32_t keyLen, int64_t value);

uint32_t size64(uint8_t *key, uint32_t keyLen);

