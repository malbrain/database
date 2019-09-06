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


c:\Users\Owner\Source\Repos\malbrain\database>testfiles\test6
"40M random 10 byte keys, multi-threaded insertions then searches in Adaptive Radix Tree index"

standalone testdb -debug -stats -cmds=w -summary=vc -idxType=0 -bits=16 -noDocs -pennysort -threads=4 10000000
thread 0 launched for file 10000000 cmds w
thread 1 launched for file 10000000 cmds w
thread 2 launched for file 10000000 cmds w
thread 3 launched for file 10000000 cmds w

thrd:1 cmd:w Adaptive Radix Tree: begin
thrd:1 cmd:w random keys:10000000
thrd:1 cmd:w file:10000000 records processed: 10000000
thrd:1 cmd:w end
 real 0m9.595s
 user 0m35.000s
 sys  0m2.625s

thrd:2 cmd:w Adaptive Radix Tree: begin
thrd:2 cmd:w random keys:10000000
thrd:2 cmd:w file:10000000 records processed: 10000000
thrd:2 cmd:w end
 real 0m9.613s
 user 0m35.046s
 sys  0m2.625s

thrd:3 cmd:w Adaptive Radix Tree: begin
thrd:3 cmd:w random keys:10000000
thrd:3 cmd:w file:10000000 records processed: 10000000
thrd:3 cmd:w end
 real 0m9.630s
 user 0m35.078s
 sys  0m2.625s

thrd:0 cmd:w Adaptive Radix Tree: begin
thrd:0 cmd:w random keys:10000000
thrd:0 cmd:w file:10000000 records processed: 10000000
thrd:0 cmd:w end
 real 0m9.676s
 user 0m35.125s
 sys  0m2.625s

Index Adaptive Radix Tree summary scan:
 forward index cursor
 key order verification
 index key count
 Index scan complete
 Total keys 40000000

Total memory allocated: 3165.304 MB
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

now find those 40M keys with 4 threads

standalone testdb -debug -stats -cmds=f -summary=vc -idxType=0 -bits=16 -noDocs -pennysort -threads=4 10000000
thread 0 launched for file 10000000 cmds f
thread 1 launched for file 10000000 cmds f
thread 2 launched for file 10000000 cmds f
thread 3 launched for file 10000000 cmds f

thrd:1 cmd:f Adaptive Radix Tree: begin
thrd:1 cmd:f random keys:10000000
thrd:1 cmd:f file:10000000 records processed: 10000000
thrd:1 cmd:f end
 real 0m6.157s
 user 0m23.359s
 sys  0m1.203s

thrd:0 cmd:f Adaptive Radix Tree: begin
thrd:0 cmd:f random keys:10000000
thrd:0 cmd:f file:10000000 records processed: 10000000
thrd:0 cmd:f end
 real 0m6.212s
 user 0m23.500s
 sys  0m1.234s

thrd:3 cmd:f Adaptive Radix Tree: begin
thrd:3 cmd:f random keys:10000000
thrd:3 cmd:f file:10000000 records processed: 10000000
thrd:3 cmd:f end
 real 0m6.230s
 user 0m23.531s
 sys  0m1.203s

thrd:2 cmd:f Adaptive Radix Tree: begin
thrd:2 cmd:f random keys:10000000
thrd:2 cmd:f file:10000000 records processed: 10000000
thrd:2 cmd:f end
 real 0m6.274s
 user 0m23.578s
 sys  0m1.203s

Index Adaptive Radix Tree summary scan:
 forward index cursor
 key order verification
 index key count
 Index scan complete
 Total keys 40000000

Total memory allocated: 0.443 MB
Bytes per key: 0

Sample cursor scan with min and max values:

c:\Users\Owner\Source\Repos\malbrain\database>testfiles\test7

"40M random 10 byte keys, multi-threaded insertions then subset cursor over btree1 index"

c:\Users\Owner\Source\Repos\malbrain\database>standalone testdb -debug -stats -cmds=w -summary=k -idxType=1 -bits=16 -minKey=aaaA -maxKey=aaaK -noDocs -pennysort -threads=4 10000000
thread 0 launched for file 10000000 cmds w
thread 1 launched for file 10000000 cmds w
thread 2 launched for file 10000000 cmds w
thread 3 launched for file 10000000 cmds w

thrd:0 cmd:w Btree1 paged arrays of keys: begin
thrd:0 cmd:w random keys:10000000
thrd:0 cmd:w file:10000000 records processed: 10000000
thrd:0 cmd:w end

 real 0m16.306s
 user 1m4.500s
 sys  0m0.750s

thrd:1 cmd:w Btree1 paged arrays of keys: begin
thrd:1 cmd:w random keys:10000000
thrd:1 cmd:w file:10000000 records processed: 10000000
thrd:1 cmd:w end

 real 0m16.314s
 user 1m4.500s
 sys  0m0.750s

thrd:3 cmd:w Btree1 paged arrays of keys: begin
thrd:3 cmd:w random keys:10000000
thrd:3 cmd:w file:10000000 records processed: 10000000
thrd:3 cmd:w end

 real 0m16.380s
 user 1m4.625s
 sys  0m0.750s

thrd:2 cmd:w Btree1 paged arrays of keys: begin
thrd:2 cmd:w random keys:10000000
thrd:2 cmd:w file:10000000 records processed: 10000000
thrd:2 cmd:w end

 real 0m16.464s
 user 1m4.719s
 sys  0m0.750s

Index Btree1 paged arrays of keys summary scan:
 forward index cursor
 min key: <aaaA>
 max key: <aaaK>
aaaA`5Jc0s
aaaBEN26y@
aaaBTpPoRX
aaaBZ9vZ2y
aaaB`loMtO
aaaBmxfvnU
aaaCT`xBrD
aaaCTk7MB0
aaaC`03VXi
aaaCscM`2F
aaaDBNWovP
aaaEWtt9xl
aaaEfZaWm9
aaaGEopxiW
aaaGFG75Tl
aaaGWJNVn8
aaaGsBXyDw
aaaHIkrQtV
aaaHJWG6gO
aaaHQwZJ@N
aaaHbJmmbz
aaaI7BS6cG
aaaIqwxAAp
aaaJ4hyIxf
aaaJ9407ya
aaaJoVKLk9
aaaKroIRhc
 Index scan complete
 Total keys 27

 real 0m0.017s
 user 0m0.000s
 sys  0m0.000s

Total memory allocated: 1034.197 MB
Bytes per key: 40164238

CWD: c:\Users\Owner\Source\Repos\malbrain\database PageSize: 4096, # Processors: 8, Allocation Granularity: 65536


c:\Users\Owner\Source\Repos\malbrain\database>