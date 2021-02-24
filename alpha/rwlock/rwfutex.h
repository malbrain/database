//  Brandenburg Phase-Fair FIFO reader-writer lock

typedef volatile union {
	struct {
	  uint16_t rin[1];
	  uint16_t rout[1];
	  uint16_t serving[1];
	  uint16_t ticket[1];
	};
	uint32_t rw[2];
} RWLock;

//	define rin bits

#define PHID 0x1	// phase ID
#define PRES 0x2	// writer is present
#define MASK 0x3
#define RINC 0x4	// reader count increment

//	Mutex based reader-writer lock

typedef enum {
	FREE = 0,
	LOCKED,
	CONTESTED
} MutexState;

typedef struct {
	volatile MutexState state[1];
} Mutex;

typedef struct {
  Mutex xcl[1];
  Mutex wrt[1];
  uint16_t readers[1];
} RWLock2;

//	mode & definition for lock implementation

enum {
	QueRd = 1,	// reader queue
	QueWr = 2	// writer queue
} RWQueue;

//	lite weight futex lock: Not phase-fair nor FIFO

typedef volatile union {
  struct {
	uint16_t read:1;	// one or more readers are sleeping
	uint16_t wrt:15;	// count of writers sleeping
	uint16_t xlock:1;	// one writer has exclusive lock
	uint16_t share:15;	// count of readers holding lock
  };
  uint16_t shorts[2];
  uint32_t longs[1];
} FutexLock;

#define READ 1
#define WRT 2
#define XCL 65536
#define SHARE (XCL * 2)
