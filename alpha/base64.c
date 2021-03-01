#include "base64.h"

//	fill in top down values from keyend
//	return number of values array used
//	array slot zero 

uint8_t parse64(uint8_t *sourceKey, int64_t *keyValues,  uint8_t max ) {
uint8_t cnt = 0;
uint8_t xtraBytes;
uint64_t result;

  if( max ) do {
	xtraBytes = *--sourceKey & 7;

    //  get sign of the result
	// 	positive has bit set

	if (*sourceKey & 0x80)
		result = 0;
	else
		result = -1;

	// get high order 4 bits of value
	//	from first byte

	result <<= 4;
	result |= *sourceKey & 0xf;

	//	assemble complete bytes
	//	up to 56 bits

	while( xtraBytes--) {
	  result <<= 8;
	  result |= *--sourceKey;
	}

	//	add in low order 4 bits from

	result <<= 4;
	keyValues[cnt] = result | *--sourceKey >> 4;
  }	while( ++cnt < max );

  return cnt;
}

// append key array from sortable 64 bit values
// returns number of bytes concatenated
// by additional key binary strings  

//	call with keyDest pointing at rightmost current end
//  plus one of key and avail bytes to remaining to left.

uint32_t append64(uint8_t *ptr, int64_t *suffix, uint8_t suffixCnt, uint32_t avail) {
uint8_t xtraBytes, cnt = 0;
uint32_t keyEnd = avail;
int64_t tst64;
bool neg;

  if( suffixCnt ) do {
	tst64 = suffix[cnt];
	neg = tst64 < 0;
	xtraBytes = 0;

	//	store low order 4 bits and
	//	the sign bit in the right most byte

	if( avail--)
		*--ptr = (uint8_t)(tst64 & 0xf | 0x80);
	else
		return 0;

	//	copy significant digits right to left
	//	and then discard leading zeros

	if( tst64 >>= 4 ) do {
	  if (neg && tst64 == -1)
		break;

	  if (avail--)
		  *--ptr = (uint8_t)(tst64 & 0xff);
	  else
		  return 0;

	  xtraBytes++;
	  tst64 >>= 8;
	} while (tst64 > 0xf);

	//	store high order 4 bits and
	//	the 3 bits of xtraBytes and
	//	the sign bit in the first byte

	if (avail--)
	  *--ptr = (uint8_t)(tst64 & 0xf) |(uint8_t) (xtraBytes << 4) | 0x80;
	else
	  return 0;

	//	if neg, complement the sign bit & xtraBytes bits
	//	make negative numbers lexically smaller than
	//  positive ones

	if (neg)
		*ptr ^= 0xf0;

  } while(++cnt < suffixCnt);

  return avail;
}

//  size of suffix at end of a key

uint32_t size64(uint8_t *keyDest) {
	return (*--keyDest & 0x7) + 2;
}

//	generate random base64 string

const char* base64 = "0123456789@ABCDEFGHIJKLMNOPQRSTUVWXYZ`abcdefghijklmnopqrstuvwxyz";

long mynrand48(unsigned short xseed[3]);
 
int createB64(uint8_t *key, int size, unsigned short next[3])
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

void mynrand48seed(uint16_t* nrandState, PRNG prng, int init) {
	time_t tod[1];

	time(tod);
#ifdef _WIN32
	* tod ^= GetTickCount64();
#else
	{ struct timespec ts[1];
	clock_gettime(CLOCK_REALTIME, ts);
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

