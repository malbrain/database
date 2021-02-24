rwlock
======

Reader Writer Locks

Initialze locks to all bytes zero. Compile with -D STANDALONE to add benchmarking main module.  Run the benchmark with two parameters:  # of threads and lock-type.  The benchmark consists of 1000000 lock/work/unlock calls divided by the number of threads specified.  500x Ratio of Read Locks to Write Locks.

readerwriter.c: linux/Windows spinlock/phase-fair:

    0: type 0   pthread/SRW system rwlocks
    1: type 1	Phase-Fair FIFO simple rwlock
    2: type 2	Mutex based, neither FIFO nor Fair
    3: type 3	FIFO and Phase-Fair Brandenburg spin lock

    Usage: ./readerwriter #thrds lockType
    0: sizeof RWLock0: 56
    1: sizeof RWLock1: 8
    2: sizeof RWLock2: 4
    3: sizeof RWLock3: 8

sample Windows 10 64bit output:

    C:\Users\Owner\Source\Repos\malbrain\rwlock>readerwriter 8 3

    rwlock time/lock:
     real 0.041us
     user 0.250us
     sys  0.000us
     nanosleeps 34

    rwlock moderate load:
     real 0.252us
     user 3.313us
     sys  0.000us
     nanosleeps 330

    C:\Users\Owner\Source\Repos\malbrain\rwlock>readerwriter 8 2

    rwlock time/lock:
     real 0.052us
     user 0.329us
     sys  0.031us
     nanosleeps 1344

    rwlock moderate load:
     real 0.266us
     user 3.297us
     sys  0.000us
     nanosleeps 1555

    C:\Users\Owner\Source\Repos\malbrain\rwlock>readerwriter 8 1

    rwlock time/lock:
     real 0.044us
     user 0.360us
     sys  0.000us
     nanosleeps 22

    rwlock moderate load:
     real 0.272us
     user 3.313us
     sys  0.000us
     nanosleeps 2505

    C:\Users\Owner\Source\Repos\malbrain\rwlock>readerwriter 8 0

    rwlock time/lock:
     real 0.017us
     user 0.047us
     sys  0.000us
     nanosleeps 0

    rwlock moderate load:
     real 1.282us
     user 10.063us
     sys  0.000us
     nanosleeps 0

Please address any questions or concerns to the program author: Karl Malbrain, malbrain@cal.berkeley.edu.
