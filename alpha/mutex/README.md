# mutex
Compact mutex latch and ticket latch for linux &amp; windows

The mutex latch uses one byte for the latch and is obtained and released by:

    mutex_lock(Mutex *latch);
    mutex_unlock(Mutex *latch);

The Mutex structure is defined as:

    typedef enum {
    	FREE = 0,
    	LOCKED,
    	CONTESTED
    } MutexState;

    typedef volatile union {
    	char state[1];
    } Mutex;

The ticket latch uses two 16 bit shorts and is obtained and released by:

    ticket_lock(Ticket *ticket);
    ticket_unlock(Ticket *ticket);
  
The Ticket structure is defined as:
  
    typedef struct {
      volatile uint16_t serving[1];
      volatile uint16_t next[1];
    } Ticket;

Initialize all latches to all-bits zero.  All latch types utilize exponential back-off during latch contention.

Compile with -D FUTEX to use futex contention on linux.

Define STANDLONE during compilation to perform basic benchmarks on your system:

    gcc -o mutex -g -O3 -D STANDALONE mutex.c -lpthread
    (or cl /Ox /D STANDALONE mutex.c)

    ./mutex <# threads> <mutex type> ...

	x64/release/mutex
	Usage: x64/release/mutex #threads #type ...

Usage: mutex.exe #threads #type ...
0: System Type 40 bytes
1: Mutex Type 1 bytes
2: Ticket Type 4 bytes
3: MCS Type 16 bytes
4: CAS Type 1 bytes
5: FAA64 type 8 bytes
6: FAA32 type 4 bytes
7: FAA16 type 2 bytes


Linux on windows 10 64 bit times per lock/unlock basic one byte mutex
from 1 to 8 threads

karl@DESKTOP-8G6NRQ9:/mnt/c/Users/Owner/Source/Repos/malbrain/mutex$ ./a.out 1 1
 real 7ns
 user 7ns
 sys  0ns
 nanosleeps 0
karl@DESKTOP-8G6NRQ9:/mnt/c/Users/Owner/Source/Repos/malbrain/mutex$ ./a.out 2 1
 real 7ns
 user 8ns
 sys  0ns
 nanosleeps 951
karl@DESKTOP-8G6NRQ9:/mnt/c/Users/Owner/Source/Repos/malbrain/mutex$ ./a.out 4 1
 real 7ns
 user 10ns
 sys  0ns
 nanosleeps 4272
karl@DESKTOP-8G6NRQ9:/mnt/c/Users/Owner/Source/Repos/malbrain/mutex$ ./a.out 8 1
 real 8ns
 user 27ns
 sys  1ns
 nanosleeps 10093


WIN64 CAS mutex times per lock/unlock basic one byte CAS mutex
from 1 to 8 threads

C:\Users\Owner\Source\Repos\malbrain\mutex>mutex.exe 1 4
 real 11ns
 user 11ns
 sys  0ns
 nanosleeps 0

C:\Users\Owner\Source\Repos\malbrain\mutex>mutex.exe 2 4
 real 11ns
 user 17ns
 sys  4ns
 nanosleeps 1172

C:\Users\Owner\Source\Repos\malbrain\mutex>mutex.exe 4 4
 real 11ns
 user 21ns
 sys  9ns
 nanosleeps 2135

C:\Users\Owner\Source\Repos\malbrain\mutex>mutex.exe 8 4
 real 14ns
 user 54ns
 sys  36ns
 nanosleeps 16149

WIN64 CAS mutex times per lock/unlock basic one byte CAS mutex
from 1 to 8 threads and FUTEX contention handling

karl@DESKTOP-8G6NRQ9:/mnt/c/Users/Owner/Source/Repos/malbrain/mutex$ ./a.out 1 1
 real 11ns
 user 11ns
 sys  0ns
 futex waits: 0
 nanosleeps 0
karl@DESKTOP-8G6NRQ9:/mnt/c/Users/Owner/Source/Repos/malbrain/mutex$ ./a.out 2 1
 real 11ns
 user 22ns
 sys  0ns
 futex waits: 825
 nanosleeps 0
karl@DESKTOP-8G6NRQ9:/mnt/c/Users/Owner/Source/Repos/malbrain/mutex$ ./a.out 4 1
 real 13ns
 user 48ns
 sys  0ns
 futex waits: 2471
 nanosleeps 0
karl@DESKTOP-8G6NRQ9:/mnt/c/Users/Owner/Source/Repos/malbrain/mutex$ ./a.out 8 1
 real 19ns
 user 131ns
 sys  0ns
 futex waits: 1869


Please address any questions or concerns to the program author: Karl Malbrain, malbrain@cal.berkeley.edu.
