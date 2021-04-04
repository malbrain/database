#include <stdint.h>
#include <stdio.h>

typedef union {
	struct {
		uint32_t idx;		// record ID in the segment
		uint16_t seg;		// arena segment number
		union {
			enum TxnState step :8;
			uint16_t xtra[1];	// xtra bits 
		};
	};
	uint64_t addr : 48;		// address part of struct above
	uint64_t bits;
} ObjId;

typedef ObjId PageId;


typedef union {
	uint64_t bits[2];
	struct {
		uint32_t off : 29;	// key bytes offset
		uint32_t type : 2;	// type of key slot
		uint32_t dead : 1;	// dead/librarian slot
		uint32_t length;	// key length
	};
	union {
		PageId childId;	// page Id of next level to leaf
		ObjId payLoad;
	};
} Btree1Slot;

Btree1Slot librarian;
Btree1Slot slotx[2];

int main(int argc, char **argv)
{
	librarian.bits = 0;
	librarian.type = Btree1_librarian;
	librarian.dead = 1;

	printf("size: %lld\n", sizeof(Btree1Slot));

	slotx[0].bits[0] = 0x0123456789abcdef;
	slotx[1].bits[0] = 0xfedcba9876543210;
	printf("value:%llx %llx\n", slotx[0].bits[0], slotx[1].bits[1]);

	slotx->bits[0] = 0x0123456789abcdef;
	slotx->bits[1] = 0xfedcba9876543210;
	printf("value:%llx %llx\n", slotx[0].bits[0], slotx[1].bits[1]);
}