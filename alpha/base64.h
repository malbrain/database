#pragma once
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <time.h>
#include <memory.h>
#include <limits.h>
#include <string.h>
#include <assert.h>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

typedef enum {
	prngRandom,		// pure randomness all streams unique
	prngProcess,	// all streams are the same
	prngThread		// each thread has own stream, repeats across processes
} PRNG;

void mynrand48seed(uint16_t* nrandState, PRNG prng, int init);

int createB64(uint8_t* key, int size, unsigned short next[3]);

//  assemble binary values after key bytes

uint32_t append64(uint8_t *keyDest, int64_t *keyValues, uint8_t max, uint32_t avail);

//	fill in top down valueS from keyend
//	return number of values

uint8_t parse64(uint8_t *sourceKey, int64_t *keyValues, uint8_t max);
