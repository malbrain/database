#include "db.h"

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

//	return 64 bit suffix value from key

uint64_t get64(uint8_t *key, uint32_t len) {
int idx = 0, xtraBytes;
uint64_t result;

	xtraBytes = key[len - 1] & 7;

	//  get sign of the result
	// 	positive has bit set

	len -= xtraBytes + 2;

	if (key[len] & 0x80)
		result = 0;
	else
		result = -1;

	// get high order 4 bits
	//	from first byte

	result <<= 4;
	result |= key[len] & 0x0f;

	//	assemble complete bytes
	//	up to 56 bits

	while (idx++ < xtraBytes) {
	  result <<= 8;
	  result |= key[len + idx];
	}

	//	add in low order 4 bits

	result <<= 4;
	result |= key[len + xtraBytes + 1] >> 4;
	return result;
}

//	calculate offset from right end of zone
//	and return suffix value

uint64_t zone64(uint8_t* key, uint32_t len, uint32_t zone) {
uint32_t xtraBytes = (key[len - zone] & 0x70) >> 4;;

	zone -= xtraBytes + 2;
	return get64(key, len - zone);
}

// concatenate key with sortable 64 bit value
// returns number of bytes concatenated

uint32_t store64(uint8_t *key, uint32_t keyLen, int64_t value) {
int64_t tst64 = value >> 8;
uint32_t xtraBytes = 0;
uint32_t idx;
bool neg;

	neg = value < 0;

	while (tst64)
	  if (neg && tst64 == -1)
		break;
	  else
		xtraBytes++, tst64 >>= 8;

	//	store low order 4 bits of given
	//	value in final extended key byte
 
    key[keyLen + xtraBytes + 1] = (uint8_t)((value & 0xf) << 4 | xtraBytes | 8);

    value >>= 4;

	//	store complete value bytes

    for (idx = xtraBytes; idx; idx--) {
        key[keyLen + idx] = (value & 0xff);
        value >>= 8;
    }

	//	store high order 4 bits and
	//	the 3 bits of xtraBytes and
	//	the sign bit in the first byte

    key[keyLen] = (uint8_t)((value & 0xf) | (xtraBytes << 4) | 0x80);

	//	if neg, complement the sign bit & xtraBytes bits to
	//	make negative numbers lexically smaller than positive ones

	if (neg)
		key[keyLen] ^= 0xf0;

    return xtraBytes + 2;
}

//	calc space needed to store 64 bit value

uint32_t calc64 (int64_t value) {
int64_t tst64 = value >> 8;
uint32_t xtraBytes = 0;
bool neg;

	neg = value < 0;

	while (tst64)
	  if (neg && tst64 == -1)
		break;
	  else
		xtraBytes++, tst64 >>= 8;

    return xtraBytes + 2;
}

//  size of suffix at end of a key

uint32_t size64(uint8_t *key, uint32_t len) {
	return (key[len - 1] & 0x7) + 2;
}

//	generate random base64 string

const char* base64 = "0123456789@ABCDEFGHIJKLMNOPQRSTUVWXYZ`abcdefghijklmnopqrstuvwxyz";

long mynrand48(unsigned short xseed[3]);
 
int createB64(char *key, int size, unsigned short next[3])
{
uint64_t byte8 = 0;
int base;

	for( base = 0; base < size; base++ ) {
		if( base % 8 == 0 ) {
			byte8 = (uint64_t)mynrand48(next) << 32;
			byte8 |= mynrand48(next);
		}
		key[base] = base64[byte8 & 0x3f];
		byte8 >>= 6;
	}         	

	return base;
}
// random number implementations

//	implement reentrant nrand48

#define RAND48_SEED_0   (0x330e)
#define RAND48_SEED_1   (0xabcd)
#define RAND48_SEED_2   (0x1234)
#define RAND48_MULT_0   (0xe66d)
#define RAND48_MULT_1   (0xdeec)
#define RAND48_MULT_2   (0x0005)
#define RAND48_ADD      (0x000b)

unsigned short _rand48_add = RAND48_ADD;
unsigned short _rand48_seed[3] = {
    RAND48_SEED_0,
    RAND48_SEED_1,
    RAND48_SEED_2
};

unsigned short _rand48_mult[3] = {
    RAND48_MULT_0,
    RAND48_MULT_1,
    RAND48_MULT_2
};

void mynrand48seed(uint16_t* nrandState, int prng, uint16_t init) {
	time_t tod[1];

	time(tod);
#ifdef _WIN32
	* tod ^= GetTickCount64();
#else
	{ struct timespec ts[1];
	clock_gettimme(CLOCK_REALTIME, ts);
	*tod ^= ts->tv_sec << 32 | ts->tv_nsec;
	}
#endif
	nrandState[0] = RAND48_SEED_0;
	nrandState[1] = RAND48_SEED_1;
	nrandState[2] = RAND48_SEED_2;

	switch (prng) {
	case prngProcess:
		break;

	case prngThread:
		nrandState[0] ^= init;
		break;

	case prngRandom:
		nrandState[0] ^= (*tod & 0xffff);
		*tod >>= 16;
		nrandState[1] ^= (*tod & 0xffff);
		*tod >>= 16;
		nrandState[2] ^= (*tod & 0xffff);
		break;
	}
}

long mynrand48(unsigned short xseed[3]) 
{
unsigned short temp[2];
unsigned long accu;

    accu = (unsigned long)_rand48_mult[0] * (unsigned long)xseed[0] + (unsigned long)_rand48_add;
    temp[0] = (unsigned short)accu;        /* lower 16 bits */
    accu >>= sizeof(unsigned short) * 8;
    accu += (unsigned long)_rand48_mult[0] * (unsigned long)xseed[1]; 
	accu += (unsigned long)_rand48_mult[1] * (unsigned long)xseed[0];
    temp[1] = (unsigned short)accu;        /* middle 16 bits */
    accu >>= sizeof(unsigned short) * 8;
    accu += _rand48_mult[0] * xseed[2] + _rand48_mult[1] * xseed[1] + _rand48_mult[2] * xseed[0];
    xseed[0] = temp[0];
    xseed[1] = temp[1];
    xseed[2] = (unsigned short)accu;
    return ((long)xseed[2] << 15) + ((long)xseed[1] >> 1);
}

uint32_t lcg_parkmiller(uint32_t *state)
{
	if( !*state )
		*state = 0xdeadbeef;

    return *state = ((uint64_t)*state * 48271u) % 0x7fffffff;
}

/*
 * The package generates far better random numbers than a linear
 * congruential generator.  The random number generation technique
 * is a linear feedback shift register approach.  In this approach,
 * the least significant bit of all the numbers in the RandTbl table
 * will act as a linear feedback shift register, and will have period
 * of approximately 2^96 - 1.
 *
 */

#define RAND_order (7 * sizeof(unsigned))
#define RAND_size (96 * sizeof(unsigned))

unsigned char RandTbl[RAND_size + RAND_order];
int RandHead = 0;

/*
 * random: 	x**96 + x**7 + x**6 + x**4 + x**3 + x**2 + 1
 *
 * The basic operation is to add to the number at the head index
 * the XOR sum of the lower order terms in the polynomial.
 * Then the index is advanced to the next location cyclically
 * in the table.  The value returned is the sum generated.
 *
 */

unsigned xrandom (void)
{
register unsigned fact;

	if( (RandHead -= sizeof(unsigned)) < 0 ) {
		RandHead = RAND_size - sizeof(unsigned);
		memcpy (RandTbl + RAND_size, RandTbl, RAND_order);
	}

	fact = *(unsigned *)(RandTbl + RandHead + 7 * sizeof(unsigned));
	fact ^= *(unsigned *)(RandTbl + RandHead + 6 * sizeof(unsigned));
	fact ^= *(unsigned *)(RandTbl + RandHead + 4 * sizeof(unsigned));
	fact ^= *(unsigned *)(RandTbl + RandHead + 3 * sizeof(unsigned));
	fact ^= *(unsigned *)(RandTbl + RandHead + 2 * sizeof(unsigned));
	return *(unsigned *)(RandTbl + RandHead) += fact;
}

/*
 * mrandom:
 * 		Initialize the random number generator based on the given seed.
 *
 */

void mrandom (int len, char *ptr)
{
unsigned short rand = *ptr;
int idx, bit = len * 4;

	memset (RandTbl, 0, sizeof(RandTbl));
	RandHead = 0;

	while( rand *= 20077, rand += 11, bit-- )
		if( ptr[bit >> 2] & (1 << (bit & 3)) )
			for (idx = 0; idx < 5; idx++) {
				rand *= 20077, rand += 11;
				RandTbl[rand % 96 << 2] ^= 1;
			}

	for( idx = 0; idx < 96 * 63; idx++ )
		xrandom ();
}

#ifdef STANDALONE

unsigned short xseed[3];
unsigned int lcgState[1];

int bucket[16];
uint32_t hxgram();

int main (int argc, char **argv) {
uint8_t buff[128];
int i, idx;

	while((++argv)[0]) {
		uint64_t nxt = strtoll(argv[0], NULL, 0), conv;
		int len = store64(buff, 0, nxt);

		printf("calc64: %d\n", calc64(nxt));
		printf("store64:%d  ", len);

		for( i=0; i<len; i++) {
			printf(" %.2x", buff[i]);
		
			if( i % 16 == 15 || i+1==len)
				printf ("\n");
		}

		printf ("size64: %d\n", size64 (buff, len));

		conv = get64(buff, len);
		printf("get64: 0x%" PRIx64 "  %" PRId64 "\n", conv, conv);
	}

	mynrand48seed(xseed);

	for( idx = 0; idx < 65536; idx++ )
		bucket[hxgram()]++;

	for( idx = 0; idx < 16; idx++ )
		printf("%.2d: %.6d  ", idx, bucket[idx]);

	putchar(0x0a);
	return 0;
}

uint32_t hxgram() {
uint32_t nrand32 = mynrand48(xseed);
//uint32_t nrand32 = lcg_parkmiller(lcgState);

	nrand32 |= 0x10000;

#ifdef _WIN32
	return __lzcnt(nrand32);
#else
	return __builtin_clz(nrand32);
#endif
}
#endif
