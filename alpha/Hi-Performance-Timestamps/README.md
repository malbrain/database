# Hi-Performance-Timestamps
Transaction Commitment Timestamp generator for a thousand cores

The basic idea is to create and deliver timestamps for client transactions on the order of a billion timestamp requests per second.  The generator must have an extremely short execution path that doesn't require atomic operations for every request.

````
#ifdef SCAN
    printf("Table Scan\n");
#endif
#ifdef QUEUE
    printf("FIFO Queue\n");
#endif
#ifdef ATOMIC
    printf("Atomic Incr\n");
#endif
#ifdef ALIGN
    printf("Atomic Aligned 64\n");
#endif
#ifdef RDTSC
    printf("TSC COUNT: New  Epochs = %" PRIu64 "\n", rdtscEpochs);
#endif
#ifdef CLOCK
    printf("Hi Res Timer\n");
#endif
````

As a baseline here are the timing results for one billion requests for a new timestamp using an atomic increment of a single common timestamp by threads 1 thru  7 concurrently.
````

No contention, 1 thread:
------------------------

Hi-Performance-Timestamps\timestamps>standalone.exe 1 1000000000
thread 1 launched for 1000000000 timestamps
Begin client 1
Atomic Incr
 real 0m6.260s -- 6ns per timestamp
 user 0m6.250s
 sys  0m0.000s

2 threads:
------------------------

Hi-Performance-Timestamps\timestamps>standalone.exe 2 500000000
thread 1 launched for 500000000 timestamps
thread 2 launched for 500000000 timestamps
Begin client 1
Begin client 2
Atomic Incr
 real 0m20.167s -- 20ns
 user 0m40.250s
 sys  0m0.000s

3 threads:
------------------------

Hi-Performance-Timestamps\timestamps>standalone.exe 3 333333333
thread 1 launched for 333333333 timestamps
thread 2 launched for 333333333 timestamps
Begin client 2
Begin client 3
thread 3 launched for 333333333 timestamps
Begin client 1
Atomic Incr
 real 0m25.531s
 user 1m16.437s
 sys  0m0.000s

4 threads:
------------------------

Hi-Performance-Timestamps\timestamps>standalone.exe 4 250000000
thread 1 launched for 250000000 timestamps
thread 2 launched for 250000000 timestamps
thread 3 launched for 250000000 timestamps
Begin client 2
Begin client 3
thread 4 launched for 250000000 timestamps
Begin client 1
Begin client 4
Atomic Incr
 real 0m18.416s
 user 1m12.437s
 sys  0m0.000s

5 threads:
------------------------

Hi-Performance-Timestamps\timestamps>standalone.exe 5 200000000
thread 1 launched for 200000000 timestamps
thread 2 launched for 200000000 timestamps
Begin client 1
Begin client 2
thread 3 launched for 200000000 timestamps
Begin client 3
thread 4 launched for 200000000 timestamps
Begin client 4
thread 5 launched for 200000000 timestamps
Begin client 5
Atomic Incr
 real 0m24.651s
 user 2m2.343s
 sys  0m0.000s

6 threads:
------------------------

Hi-Performance-Timestamps\timestamps>standalone.exe 6 166666666
thread 1 launched for 166666666 timestamps
thread 2 launched for 166666666 timestamps
Begin client 1
Begin client 2
thread 3 launched for 166666666 timestamps
Begin client 3
thread 4 launched for 166666666 timestamps
Begin client 4
Begin client 5
thread 5 launched for 166666666 timestamps
thread 6 launched for 166666666 timestamps
Begin client 6
Atomic Incr
 real 0m24.008s
 user 2m22.546s
 sys  0m0.000s

7 threads:
------------------------

Hi-Performance-Timestamps\timestamps>standalone.exe 7 142857145
thread 1 launched for 142857145 timestamps
thread 2 launched for 142857145 timestamps
Begin client 2
Begin client 1
thread 3 launched for 142857145 timestamps
Begin client 3
thread 4 launched for 142857145 timestamps
Begin client 4
thread 5 launched for 142857145 timestamps
Begin client 5
thread 6 launched for 142857145 timestamps
Begin client 6
thread 7 launched for 142857145 timestamps
Begin client 7
Atomic Incr
 real 0m25.329s
 user 2m53.109s
 sys  0m0.047s

````
One answer to this deteriorating situation is to derive each timestamp value from the RDTSC hardware counter, grouping them into 1 second long Epochs of 1 billion timestamps. 

````
C:cl /Oxib2 /D RDTSC standalone.c timestamps.c
Microsoft (R) C/C++ Optimizing Compiler Version 19.23.28106.4 for x64
Copyright (C) Microsoft Corporation.  All rights reserved.

standalone.c
timestamps.c
Generating Code...
Microsoft (R) Incremental Linker Version 14.23.28106.4
Copyright (C) Microsoft Corporation.  All rights reserved.

/out:standalone.exe
standalone.obj
timestamps.obj

C:standalone 1 1000000000
size of Timestamp = 8, TsEpoch = 16
RDTSC timing = 7ns, resolution = 25
thread 1 launched for 1000000000 timestamps
Begin client 1
client 1 count = 1000000000 Out of Order = 0 dups = 0
TSC COUNT: New  Epochs = 2
 real 0m13.127s
 user 0m13.125s
 sys  0m0.000s

C:standalone 2 500000000
size of Timestamp = 8, TsEpoch = 16
RDTSC timing = 7ns, resolution = 26
thread 1 launched for 500000000 timestamps
thread 2 launched for 500000000 timestamps
Begin client 1
Begin client 2
client 2 count = 500000000 Out of Order = 0 dups = 0
client 1 count = 500000000 Out of Order = 0 dups = 0
TSC COUNT: New  Epochs = 0
 real 0m6.659s
 user 0m13.296s
 sys  0m0.000s

C:standalone 3 333333333
size of Timestamp = 8, TsEpoch = 16
RDTSC timing = 7ns, resolution = 25
thread 1 launched for 333333333 timestamps
thread 2 launched for 333333333 timestamps
thread 3 launched for 333333333 timestamps
Begin client 2
Begin client 3
Begin client 1
client 3 count = 333333333 Out of Order = 0 dups = 0
client 2 count = 333333333 Out of Order = 0 dups = 0
client 1 count = 333333333 Out of Order = 0 dups = 0
TSC COUNT: New  Epochs = 6
 real 0m18.723s
 user 0m55.500s
 sys  0m0.000s

C:standalone 4 250000000
size of Timestamp = 8, TsEpoch = 16
RDTSC timing = 7ns, resolution = 25
thread 1 launched for 250000000 timestamps
thread 2 launched for 250000000 timestamps
thread 3 launched for 250000000 timestamps
Begin client 1
thread 4 launched for 250000000 timestamps
Begin client 2
Begin client 3
Begin client 4
client 1 count = 250000000 Out of Order = 0 dups = 0
client 4 count = 250000000 Out of Order = 0 dups = 0
client 2 count = 250000000 Out of Order = 0 dups = 0
client 3 count = 250000000 Out of Order = 0 dups = 0
TSC COUNT: New  Epochs = 7
 real 0m17.298s
 user 1m2.875s
 sys  0m0.000s

C:standalone 5 200000000
size of Timestamp = 8, TsEpoch = 16
RDTSC timing = 7ns, resolution = 25
thread 1 launched for 200000000 timestamps
thread 2 launched for 200000000 timestamps
thread 3 launched for 200000000 timestamps
Begin client 1
Begin client 2
thread 4 launched for 200000000 timestamps
Begin client 3
Begin client 4
thread 5 launched for 200000000 timestamps
Begin client 5
client 4 count = 200000000 Out of Order = 0 dups = 0
client 5 count = 200000000 Out of Order = 0 dups = 0
client 1 count = 200000000 Out of Order = 0 dups = 0
client 2 count = 200000000 Out of Order = 0 dups = 0
client 3 count = 200000000 Out of Order = 0 dups = 0
TSC COUNT: New  Epochs = 10
 real 0m13.627s
 user 1m7.109s
 sys  0m0.000s

C:standalone 6 166666666
size of Timestamp = 8, TsEpoch = 16
RDTSC timing = 7ns, resolution = 26
thread 1 launched for 166666666 timestamps
thread 2 launched for 166666666 timestamps
thread 3 launched for 166666666 timestamps
thread 4 launched for 166666666 timestamps
Begin client 1
thread 5 launched for 166666666 timestamps
Begin client 3
Begin client 2
thread 6 launched for 166666666 timestamps
Begin client 4
Begin client 5
Begin client 6
client 2 count = 166666666 Out of Order = 0 dups = 0
client 1 count = 166666666 Out of Order = 0 dups = 0
client 3 count = 166666666 Out of Order = 0 dups = 0
client 5 count = 166666666 Out of Order = 0 dups = 0
client 4 count = 166666666 Out of Order = 0 dups = 0
client 6 count = 166666666 Out of Order = 0 dups = 0
TSC COUNT: New  Epochs = 6
 real 0m12.804s
 user 1m13.750s
 sys  0m0.000s

C:standalone 7 142800000
size of Timestamp = 8, TsEpoch = 16
RDTSC timing = 7ns, resolution = 25
thread 1 launched for 142800000 timestamps
thread 2 launched for 142800000 timestamps
Begin client 1
Begin client 2
thread 3 launched for 142800000 timestamps
Begin client 3
thread 4 launched for 142800000 timestamps
Begin client 4
thread 5 launched for 142800000 timestamps
Begin client 5
thread 6 launched for 142800000 timestamps
Begin client 6
thread 7 launched for 142800000 timestamps
Begin client 7
client 1 count = 142800000 Out of Order = 0 dups = 0
client 2 count = 142800000 Out of Order = 0 dups = 0
client 4 count = 142800000 Out of Order = 0 dups = 0
client 7 count = 142800000 Out of Order = 0 dups = 0
client 6 count = 142800000 Out of Order = 0 dups = 0
client 3 count = 142800000 Out of Order = 0 dups = 0
client 5 count = 142800000 Out of Order = 0 dups = 0
TSC COUNT: New  Epochs = 12
 real 0m14.039s
 user 1m27.031s
 sys  0m0.000s

````
