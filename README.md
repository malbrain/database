malbrain/database
==========================

A working project for High-concurrency B-tree/ARTree Database source code in C.  This project was created as a sub-module for the /www.github.com/malbrain/javascript-database project, but it can also be used by itself as a database/indexing library. The API is documented on the Wiki pages.

The standalone.c test program exercises the basic functionality of the database, and can be used as a sample database application.  For a more complete example of how MVCC and transactions can be implemented on the database core functionality, please see the javascript-database project, partcularly the js_db*.c files.

Compile the database library and standalone test module with ./build or build.bat.

```
Usage: dbtest db_name -cmds=[crwsdf]... -idxType=[012] -bits=# -xtra=# -inMem -noIdx -noDocs -maxKey=# -minKey=# -keyLen=# src_file1 src_file2 ... ]
      where db_name is the prefix name of the database file
      cmds is a string of (c)ount/(r)ev scan/(w)rite/(s)can/(d)elete/(f)ind, with a one character command for each input src_file, or a no-input command.
      idxType is the type of index: 0 = ART, 1 = btree1, 2 = btree2
      keyLen is key size, zero for whole line
      bits is the btree page size in bits
      xtra is the btree leaf page extra bits
      inMem specifies no disk files
      noDocs specifies keys only
      src_file1 thru src_filen are files of keys/documents separated by newline.  Each is given to a new thread.
```
Linux compilation command:

    [karl@test7x64 xlink]# cc -std=c11 -O2 -g -o dbtest standalone.c base64.c db*.c artree/*.c btree1/*.c btree2/*.c -lpthread

```
Sample single thread output from indexing 40M 10 byte pennysort keys:

**********************************************************************
** Visual Studio 2019 Developer Command Prompt v16.2.3
** Copyright (c) 2019 Microsoft Corporation
**********************************************************************
[vcvarsall.bat] Environment initialized for: 'x64'

c:\Users\Owner\Source\Repos\malbrain\database>testfiles\test2

standalone.exe testdb -stats -cmds=w -summary=vc -pipeline -idxType=2 -bits=16  -inMem -noDocs -pennysort 40000000

thrd:0 cmd:w Btree2 paged skiplists: begin
thrd:0 cmd:w random keys:40000000
thrd:0 cmd:w file:40000000 records processed: 40000000
thrd:0 cmd:w end
 real 1m12.629s
 user 1m12.468s
 sys  0m0.156s

Index Btree2 paged skiplists summary scan:
 forward index cursor
 key order verification
 index key count
 Index scan complete
 Total keys 40000000

Total memory allocated: 2125.543 MB
Bytes per key: 55

standalone.exe testdb -stats -cmds=w -summary=vc -pipeline -idxType=1 -bits=16  -inMem -noDocs -pennysort 40000000

thrd:0 cmd:w Btree1 paged arrays of keys: begin
thrd:0 cmd:w random keys:40000000
thrd:0 cmd:w file:40000000 records processed: 40000000
thrd:0 cmd:w end
 real 0m52.582s
 user 0m52.500s
 sys  0m0.078s

Index Btree1 paged arrays of keys summary scan:
 forward index cursor
 key order verification
 index key count
 Index scan complete
 Total keys 40000000

Total memory allocated: 1027.947 MB
Bytes per key: 26

standalone.exe testdb -stats -cmds=w -summary=vc -pipeline -idxType=0 -inMem -noDocs -pennysort 40000000

thrd:0 cmd:w Adaptive Radix Tree: begin
thrd:0 cmd:w random keys:40000000
thrd:0 cmd:w file:40000000 records processed: 40000000
thrd:0 cmd:w end
 real 0m27.674s
 user 0m27.313s
 sys  0m0.359s

Index Adaptive Radix Tree summary scan:
 forward index cursor
 key order verification
 index key count
 Index scan complete
 Total keys 40000000

Total memory allocated: 3163.828 MB
Bytes per key: 82

Adaptive Radix Tree Index type 1 blks allocated: 12469156
Adaptive Radix Tree Index type 2 blks allocated: 01688965
Adaptive Radix Tree Index type 3 blks allocated: 00266305
Adaptive Radix Tree Index type 6 blks allocated: 40000000
Adaptive Radix Tree Index type 8 blks allocated: 92558902
Adaptive Radix Tree Index type 9 blks allocated: 00000065

Adaptive Radix Tree Index type 1 blks freed    : 01688965
Adaptive Radix Tree Index type 2 blks freed    : 00266305
Adaptive Radix Tree Index type 8 blks freed    : 12469091
Adaptive Radix Tree Index type 9 blks freed    : 00000065

c:\Users\Owner\Source\Repos\malbrain\database>
```
```
Sample four thread output from indexing into Btree1 index 40M pennysort 10 byte keys:
c:\Users\Owner\Source\Repos\malbrain\database>testfiles\test3

c:\Users\Owner\Source\Repos\malbrain\database>ECHO "40M random keys, multi-threaded insertions into btree1 index""
"40M random keys, multi-threaded insertions into btree1 index""
thread 0 launched for file 10000000
thread 1 launched for file 10000000
thread 2 launched for file 10000000
thread 3 launched for file 10000000

thrd:2 cmd:w Adaptive Radix Tree: begin
thrd:2 cmd:w random keys:10000000
thrd:2 cmd:w file:10000000 records processed: 10000000
thrd:2 cmd:w end
 real 0m8.635s
 user 0m33.875s
 sys  0m0.688s

thrd:1 cmd:w Adaptive Radix Tree: begin
thrd:1 cmd:w random keys:10000000
thrd:1 cmd:w file:10000000 records processed: 10000000
thrd:1 cmd:w end
 real 0m8.657s
 user 0m33.921s
 sys  0m0.688s

thrd:0 cmd:w Adaptive Radix Tree: begin
thrd:0 cmd:w random keys:10000000
thrd:0 cmd:w file:10000000 records processed: 10000000
thrd:0 cmd:w end
 real 0m8.710s
 user 0m34.015s
 sys  0m0.688s

thrd:3 cmd:w Adaptive Radix Tree: begin
thrd:3 cmd:w random keys:10000000
thrd:3 cmd:w file:10000000 records processed: 10000000
thrd:3 cmd:w end
 real 0m8.717s
 user 0m34.031s
 sys  0m0.688s

Index Adaptive Radix Tree summary scan:
 forward index cursor
 key order verification
 index key count
 Index scan complete
 Total keys 40000000

Total memory allocated: 3165.296 MB
Bytes per key: 82

Adaptive Radix Tree Index type 1 blks allocated: 12468041
Adaptive Radix Tree Index type 2 blks allocated: 01688500
Adaptive Radix Tree Index type 3 blks allocated: 00266306
Adaptive Radix Tree Index type 6 blks allocated: 40000000
Adaptive Radix Tree Index type 8 blks allocated: 92557948
Adaptive Radix Tree Index type 9 blks allocated: 00000065

Adaptive Radix Tree Index type 1 blks freed    : 01688500
Adaptive Radix Tree Index type 2 blks freed    : 00266306
Adaptive Radix Tree Index type 8 blks freed    : 12467976
Adaptive Radix Tree Index type 9 blks freed    : 00000065


CWD: c:\Users\Owner\Source\Repos\malbrain\database PageSize: 4096, # Processors: 8, Allocation Granularity: 65536


Sample output from writing then finding 10M 10 byte pennysort keys:

c:\Users\Owner\Source\Repos\malbrain\database>standalone db -debug -stats -cmds=wf -summary=vc -idxType=0 -bits=16  -inMem -noDocs -pennysort 10000000

thrd:0 cmd:w Adaptive Radix Tree: begin
thrd:0 cmd:w random keys:10000000
thrd:0 cmd:w file:10000000 records processed: 10000000
thrd:0 cmd:w end
 real 0m5.918s
 user 0m5.765s
 sys  0m0.140s

thrd:0 cmd:f Adaptive Radix Tree: begin
thrd:0 cmd:f random keys:10000000
thrd:0 cmd:f file:10000000 records processed: 10000000
thrd:0 cmd:f end
 real 0m4.459s
 user 0m4.469s
 sys  0m0.000s

Index Adaptive Radix Tree summary scan:
 forward index cursor
 key order verification
 index key count
 Index scan complete
 Total keys 10000000

Total memory allocated: 868.279 MB
Bytes per key: 91

Adaptive Radix Tree Index type 1 blks allocated: 02309674
Adaptive Radix Tree Index type 2 blks allocated: 00271812
Adaptive Radix Tree Index type 3 blks allocated: 00266282
Adaptive Radix Tree Index type 6 blks allocated: 10000000
Adaptive Radix Tree Index type 8 blks allocated: 22336158
Adaptive Radix Tree Index type 9 blks allocated: 00000065

Adaptive Radix Tree Index type 1 blks freed    : 00271812
Adaptive Radix Tree Index type 2 blks freed    : 00266282
Adaptive Radix Tree Index type 8 blks freed    : 02309609
Adaptive Radix Tree Index type 9 blks freed    : 00000065


CWD: c:\Users\Owner\Source\Repos\malbrain\database PageSize: 4096, # Processors: 8, Allocation Granularity: 65536


c:\Users\Owner\Source\Repos\malbrain\database>

Sample cursor scan with min and max values:

    [karl@test7x64 xlink]$ ./dbtest tstdb -cmds=s -minKey=aaaA -maxKey=aaaK -noDocs
    started scanning min key: aaaA max key: aaaK
    aaaATP)O4j
    aaaBx&\7,4
    aaaE%(2YNR
    aaaF~E 1:w
    aaaG?n!En5
    aaaGQBoH:`
    aaaGtu4)28
    aaaGy2q2wI
    aaaH8EX{6k
    aaaJb'}Wk_
     Total keys read 10
     real 0m0.000s
     user 0m0.000s
     sys  0m0.001s

Sample 4 thread indexing and finding of 40M keys into a btree index:

    [karl@test7x64 xlink]$ ./dbtest tstdb -cmds=w -keyLen=10 -idxType=1 -noDocs pennykey[0123]
    started indexing for pennykey0
    started indexing for pennykey1
    started indexing for pennykey2
    started indexing for pennykey3
     Total keys indexed 10000000
     Total keys indexed 10000000
     Total keys indexed 10000000
     Total keys indexed 10000000
     real 0m26.707s
     user 1m38.649s
     sys  0m1.701s

    [karl@test7x64 xlink]$ ./dbtest tstdb -cmds=f -keyLen=10 -idxType=1 -noDocs pennykey[0123]
    started finding keys for pennykey0
    started finding keys for pennykey1
    started finding keys for pennykey2
    started finding keys for pennykey3
    finished pennykey1 for 10000000 keys, found 10000000
    finished pennykey2 for 10000000 keys, found 10000000
    finished pennykey3 for 10000000 keys, found 10000000
    finished pennykey0 for 10000000 keys, found 10000000
     real 0m25.969s
     user 1m38.494s
     sys  0m0.694s

Please address any concerns problems, or suggestions to the program author, Karl Malbrain, malbrain@cal.berkeley.edu
