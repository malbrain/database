#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <inttypes.h>
#include <time.h>

const char* base64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

long mynrand48(unsigned short xseed[3]);
 
int createB64(char *key, int size, unsigned short next[3])
{
uint64_t byte8;
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

void mynrand48seed(uint16_t *nrandState) {
time_t tod[1];

	time(tod);
	nrandState[0] = RAND48_SEED_0; // ^ *tod & 0xffff;
	*tod >>= 16;
	nrandState[1] = RAND48_SEED_1; // ^ *tod & 0xffff;
	*tod >>= 16;
	nrandState[2] = RAND48_SEED_2; // ^ *tod & 0xffff;
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

//	return 64 bit suffix value from key

uint64_t get64(uint8_t *key, uint32_t len, bool binaryFlds) {
int idx = 0, xtraBytes = key[len - 1] & 0x7;
int off = binaryFlds ? 2 : 0;
uint64_t result;

	//	set len to the number start

	len -= xtraBytes + 2;

	//  get sign of the result
	// 	positive has high bit set

	if (key[len] & 0x80)
		result = 0;
	else
		result = -1;

	// get high order 4 bits

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

// concatenate key with sortable 64 bit value
// returns number of bytes concatenated

uint32_t store64(uint8_t *key, uint32_t keyLen, int64_t value, bool binaryFlds) {
int off = binaryFlds ? 2 : 0;
int64_t tst64 = value >> 8;
uint32_t xtraBytes = 0;
uint32_t idx, len;
bool neg;

	neg = value < 0;

	while (tst64)
	  if (neg && tst64 == -1)
		break;
	  else
		xtraBytes++, tst64 >>= 8;

	//	store low order 4 bits of given value
	//	in final key byte
 
    key[keyLen + xtraBytes + off + 1] = (uint8_t)((value & 0xf) << 4 | xtraBytes);
    value >>= 4;

	//	store complete value bytes

    for (idx = xtraBytes; idx; idx--) {
        key[keyLen + off + idx] = (value & 0xff);
        value >>= 8;
    }

	//	store high order 4 bits and
	//	the 3 bits of xtraBytes
	//	and the sign bit

    key[keyLen + off] = (uint8_t)((value & 0xf) | (xtraBytes << 4) | 0x80);

	//	if neg, complement the sign bit & xtraBytes bits to
	//	make negative numbers lexically smaller than positive ones

	if (neg)
		key[keyLen + off] ^= 0xf0;

    len = xtraBytes + 2;

	if (binaryFlds) {
		key[keyLen] = len >> 8;
		key[keyLen + 1] = len;
	}

	return len + off;
}

//	calc space needed to store 64 bit vsalue

uint32_t calc64 (int64_t value, bool binaryFlds) {
int off = binaryFlds ? 2 : 0;
int64_t tst64 = value >> 8;
uint32_t xtraBytes = 0;
bool neg;

	neg = value < 0;

	while (tst64)
	  if (neg && tst64 == -1)
		break;
	  else
		xtraBytes++, tst64 >>= 8;

    return off + xtraBytes + 2;
}

//  size of suffix at end of a key

uint32_t size64(uint8_t *key, uint32_t len) {
	return (key[len - 1] & 0x7) + 2;
}

#ifdef STANDALONE
int main (int argc, char **argv) {
uint8_t buff[128];
int i;

	while((++argv)[0]) {
		uint64_t nxt = strtoll(argv[0], NULL, 0), conv;
		int len = store64(buff, 0, nxt, false);

		printf("calc64: %d\n", calc64(nxt, false));
		printf("store64:%d  ", len);

		for( i=0; i<len; i++) {
			printf(" %.2x", buff[i]);
		
			if( i % 16 == 15 || i+1==len)
				printf ("\n");
		}

		printf ("size64: %d\n", size64 (buff, len));

		conv = get64(buff, len, false);
		printf("get64: 0x%" PRIx64 "  %" PRId64 "\n", conv, conv);
	}

	return 0;
}
#endif
