malbrain/database
==========================

A working project for High-concurrency B-tree/ARTree Database source code in C.  This project was created as a sub-module for the /www.github.com/malbrain/javascript-database project, but it can also be used by itself as a database/indexing library. The API is documented on the Wiki pages.

The testfiles directory exercises the basic functionality of the database, and can be used as a sample database application.  For a more complete example of how MVCC and transactions can be implemented on the database core functionality, please see the javascript-database project, partcularly the js_db*.c files.

Clone the database project:
```
git clone --recursive https://github.com/malbrain/database.git
```
Compile the database library and standalone test module with ./build or build.bat.

```
Usage: standalone db_name -cmds=[wdf] -summary=[csrvikdn] -idxType=[012] -bits=# -xtra=# -inMem -stats -prng=# -debug -monitor -uniqueKeys -noDocs -noIdx -keyLen=# -minKey=abcd -maxKey=abce -drop -idxBinary=. -pipeline src_file1 src_file2 ... ]
  where db_name is the prefix name of the database document and index files
  cmds is a string of (w)rite/(d)elete/(f)ind commands, to run sequentially on each input src_file.
  summary scan is a string of (c)ount/(r)everse scan/(s)can/(v)erify/(i)terate/(k)ey list(d)ump doc(n)umber flags for a final scan after all threads have quit
  pennysort creates random 100 byte B64 input lines, sets keyLen to 10, line count from the file name
  idxType is the type of index: 0 = ART, 1 = btree1, 2 = btree2
  keyLen is key size, zero for whole input file line
  bits is the btree page size in bits
  xtra is the btree leaf page extra bits
  inMem specifies no disk files
  noDocs specifies keys only
  noIdx specifies documents only
  minKey specifies beginning cursor key
  maxKey specifies ending cursor key
  drop will initially drop database
  idxBinary utilize length counted fields separated by  the given deliminator in keys
  use prng number stream [012] for pennysort keys
  stats keeps performance and debugging totals and prints them
  uniqueKeys ensure keys are unique
  run cmds in a single threaded  pipeline using one input file at a time
  src_file1 thru src_filen are files of keys/documents separated by newline

```
Linux compilation command:

    [karl@test7x64 xlink]# cc -std=c11 -O2 -g -o standalone standalone.c base64.c db*.c artree/*.c btree1/*.c btree2/*.c mutex/mutex.c rwlock/readerwriter.c -lpthread

```
**********************************************************************
** Visual Studio 2019 Developer Command Prompt v16.2.3
** Copyright (c) 2019 Microsoft Corporation
**********************************************************************
[vcvarsall.bat] Environment initialized for: 'x64'


c:\Users\Owner\Source\Repos\malbrain\database>testfiles\test1
Alpha testing of binary string fields
expect 13 sorted keys

standalone testdb -cmds=w -summary=skn -idxBinary=: testfiles/test1

thrd:0 cmd:w ARTree: begin
thrd:0 cmd:w file:testfiles/test1 load records
thrd:0 cmd:w file:testfiles/test1 records processed: 13
thrd:0 cmd:w end

 real 0m0.002s
 user 0m0.000s
 sys  0m0.000s

Index ARTree summary scan:
 forward index cursor
*}-Wz1;TD-
0fssx}~[oB
5HA\z%qt{%
AsfAGHM5om
Q)JN)R9z-L
abc:def
abcd:ef
abcde:f
my+=5r7(N|
mz4VCN@a#"
o4FoBkqERn
uI^EYm8s=|
~sHd0jDv6X
 Index scan complete
 Total keys 13

 real 0m0.005s
 user 0m0.000s
 sys  0m0.015s

Total memory allocated: 3.616 MB
Bytes per key: 291692

ARTree Index type 1 blks allocated: 00000002
ARTree Index type 2 blks allocated: 00000001
ARTree Index type 5 blks allocated: 00000016
ARTree Index type 6 blks allocated: 00000013
ARTree Index type 8 blks allocated: 00000022
ARTree Index type 9 blks allocated: 00000010

ARTree Index type 1 blks freed    : 00000001
ARTree Index type 8 blks freed    : 00000001
ARTree Index type 9 blks freed    : 00000002


**********************************************************************


c:\Users\Owner\Source\Repos\malbrain\database>testfiles\test2
"40M random 10 byte keys, single threaded insertions into each index type"

standalone.exe testdb -stats -cmds=w -summary=vc -idxType=2 -bits=16  -inMem -noDocs -pennysort 40000000

thrd:0 cmd:w Btree2: begin
thrd:0 cmd:w random keys:40000000
thrd:0 cmd:w file:40000000 records processed: 40000000
thrd:0 cmd:w end

 real 1m14.259s
 user 1m13.843s
 sys  0m0.406s

Index Btree2 summary scan:
 forward index cursor
 key order verification
 index key count
 Index scan complete
 Total keys 40000000

 real 0m2.508s
 user 0m2.500s
 sys  0m0.000s

Total memory allocated: 2154.231 MB
Bytes per key: 56

standalone.exe testdb -stats -cmds=w -summary=vc -idxType=1 -bits=16  -inMem -noDocs -pennysort 40000000

thrd:0 cmd:w Btree1: begin
thrd:0 cmd:w random keys:40000000
thrd:0 cmd:w file:40000000 records processed: 40000000
thrd:0 cmd:w end

 real 0m53.759s
 user 0m53.531s
 sys  0m0.218s

Index Btree1 summary scan:
 forward index cursor
 key order verification
 index key count
 Index scan complete
 Total keys 40000000

 real 0m2.383s
 user 0m2.375s
 sys  0m0.000s

Total memory allocated: 1027.947 MB
Bytes per key: 26


**********************************************************************


c:\Users\Owner\Source\Repos\malbrain\database>testfiles\test3
"40M random 10 byte keys, multi-threaded insertions into Adaptive Radix Tree index""

standalone testdb -prng=2 -stats -cmds=w -summary=vc -idxType=0 -bits=16  -inMem -noDocs -pennysort -threads=4 10000000
thread 0 launched for file 10000000 cmds w
thread 1 launched for file 10000000 cmds w
thread 2 launched for file 10000000 cmds w
thread 3 launched for file 10000000 cmds w

thrd:3 cmd:w ARTree: begin
thrd:3 cmd:w random keys:10000000
thrd:3 cmd:w file:10000000 records processed: 10000000
thrd:3 cmd:w end

 real 0m8.588s
 user 0m33.812s
 sys  0m0.546s

thrd:1 cmd:w ARTree: begin
thrd:1 cmd:w random keys:10000000
thrd:1 cmd:w file:10000000 records processed: 10000000
thrd:1 cmd:w end

 real 0m8.592s
 user 0m33.812s
 sys  0m0.546s

thrd:0 cmd:w ARTree: begin
thrd:0 cmd:w random keys:10000000
thrd:0 cmd:w file:10000000 records processed: 10000000
thrd:0 cmd:w end

 real 0m8.596s
 user 0m33.812s
 sys  0m0.546s

thrd:2 cmd:w ARTree: begin
thrd:2 cmd:w random keys:10000000
thrd:2 cmd:w file:10000000 records processed: 10000000
thrd:2 cmd:w end

 real 0m8.615s
 user 0m33.843s
 sys  0m0.546s

Index ARTree summary scan:
 forward index cursor
 key order verification
 index key count
 Index scan complete
 Total keys 40000000

 real 0m18.537s
 user 0m18.532s
 sys  0m0.000s

Total memory allocated: 3164.795 MB
Bytes per key: 82

ARTree Index type 1 blks allocated: 12468681
ARTree Index type 2 blks allocated: 01688726
ARTree Index type 3 blks allocated: 00266305
ARTree Index type 6 blks allocated: 40000000
ARTree Index type 8 blks allocated: 92558615
ARTree Index type 9 blks allocated: 00000065

ARTree Index type 1 blks freed    : 01688726
ARTree Index type 2 blks freed    : 00266305
ARTree Index type 8 blks freed    : 12468616
ARTree Index type 9 blks freed    : 00000065


**********************************************************************


c:\Users\Owner\Source\Repos\malbrain\database>testfiles\test4
"40M random 10 byte keys, multi-threaded insertions into btree1 index"

standalone testdb -prng=2 -stats -cmds=w -summary=vc -idxType=1 -bits=16  -inMem -noDocs -pennysort -threads=4 10000000
thread 0 launched for file 10000000 cmds w
thread 1 launched for file 10000000 cmds w
thread 2 launched for file 10000000 cmds w
thread 3 launched for file 10000000 cmds w

thrd:1 cmd:w Btree1: begin
thrd:1 cmd:w random keys:10000000
thrd:1 cmd:w file:10000000 records processed: 10000000
thrd:1 cmd:w end

 real 0m16.332s
 user 1m5.046s
 sys  0m0.250s

thrd:0 cmd:w Btree1: begin
thrd:0 cmd:w random keys:10000000
thrd:0 cmd:w file:10000000 records processed: 10000000
thrd:0 cmd:w end

 real 0m16.359s
 user 1m5.140s
 sys  0m0.250s

thrd:3 cmd:w Btree1: begin
thrd:3 cmd:w random keys:10000000
thrd:3 cmd:w file:10000000 records processed: 10000000
thrd:3 cmd:w end

 real 0m16.383s
 user 1m5.171s
 sys  0m0.250s

thrd:2 cmd:w Btree1: begin
thrd:2 cmd:w random keys:10000000
thrd:2 cmd:w file:10000000 records processed: 10000000
thrd:2 cmd:w end

 real 0m16.460s
 user 1m5.250s
 sys  0m0.265s

Index Btree1 summary scan:
 forward index cursor
 key order verification
 index key count
 Index scan complete
 Total keys 40000000

 real 0m2.383s
 user 0m2.375s
 sys  0m0.000s

Total memory allocated: 1032.647 MB
Bytes per key: 27


**********************************************************************


c:\Users\Owner\Source\Repos\malbrain\database>testfiles\test5
"10M random 10 byte keys, insertions then searches in ARTree index"

standalone testdb -prng=2 -stats -cmds=wf -summary=vc -idxType=0 -bits=16  -inMem -noDocs -pennysort 10000000

thrd:0 cmd:w ARTree: begin
thrd:0 cmd:w random keys:10000000
thrd:0 cmd:w file:10000000 records processed: 10000000
thrd:0 cmd:w end

 real 0m5.872s
 user 0m5.750s
 sys  0m0.125s

thrd:0 cmd:f ARTree: begin
thrd:0 cmd:f random keys:10000000
thrd:0 cmd:f file:10000000 records processed: 10000000
thrd:0 cmd:f end

 real 0m4.297s
 user 0m4.297s
 sys  0m0.000s

Index ARTree summary scan:
 forward index cursor
 key order verification
 index key count
 Index scan complete
 Total keys 10000000

 real 0m3.682s
 user 0m3.672s
 sys  0m0.000s

Total memory allocated: 868.290 MB
Bytes per key: 91

ARTree Index type 1 blks allocated: 02309998
ARTree Index type 2 blks allocated: 00271819
ARTree Index type 3 blks allocated: 00266275
ARTree Index type 6 blks allocated: 10000000
ARTree Index type 8 blks allocated: 22336227
ARTree Index type 9 blks allocated: 00000065

ARTree Index type 1 blks freed    : 00271819
ARTree Index type 2 blks freed    : 00266275
ARTree Index type 8 blks freed    : 02309933
ARTree Index type 9 blks freed    : 00000065


**********************************************************************


c:\Users\Owner\Source\Repos\malbrain\database>testfiles\test5
"10M random 10 byte keys, insertions then searches in ARTree index"

standalone testdb -prng=2 -stats -cmds=wf -summary=vc -idxType=0 -bits=16  -inMem -noDocs -pennysort 10000000

thrd:0 cmd:w ARTree: begin
thrd:0 cmd:w random keys:10000000
thrd:0 cmd:w file:10000000 records processed: 10000000
thrd:0 cmd:w end

 real 0m5.901s
 user 0m5.703s
 sys  0m0.203s

thrd:0 cmd:f ARTree: begin
thrd:0 cmd:f random keys:10000000
thrd:0 cmd:f file:10000000 records processed: 10000000
thrd:0 cmd:f end

 real 0m4.336s
 user 0m4.343s
 sys  0m0.000s

Index ARTree summary scan:
 forward index cursor
 key order verification
 index key count
 Index scan complete
 Total keys 10000000

 real 0m4.278s
 user 0m4.266s
 sys  0m0.000s

Total memory allocated: 868.853 MB
Bytes per key: 91

ARTree Index type 1 blks allocated: 02309998
ARTree Index type 2 blks allocated: 00271819
ARTree Index type 3 blks allocated: 00266275
ARTree Index type 6 blks allocated: 10000000
ARTree Index type 8 blks allocated: 22336227
ARTree Index type 9 blks allocated: 00000065

ARTree Index type 1 blks freed    : 00271819
ARTree Index type 2 blks freed    : 00266275
ARTree Index type 8 blks freed    : 02309933
ARTree Index type 9 blks freed    : 00000065


**********************************************************************


c:\Users\Owner\Source\Repos\malbrain\database>testfiles\test6
"40M random 10 byte keys, multi-threaded insertions then searches in btree1 index"

standalone testdb -prng=2 -stats -cmds=w -summary=vc -idxType=0 -bits=16 -noDocs -pennysort -threads=4 10000000
thread 0 launched for file 10000000 cmds w
thread 1 launched for file 10000000 cmds w
thread 2 launched for file 10000000 cmds w
thread 3 launched for file 10000000 cmds w

thrd:1 cmd:w ARTree: begin
thrd:1 cmd:w random keys:10000000
thrd:1 cmd:w file:10000000 records processed: 10000000
thrd:1 cmd:w end

 real 0m9.490s
 user 0m34.671s
 sys  0m2.562s

thrd:0 cmd:w ARTree: begin
thrd:0 cmd:w random keys:10000000
thrd:0 cmd:w file:10000000 records processed: 10000000
thrd:0 cmd:w end

 real 0m9.514s
 user 0m34.718s
 sys  0m2.562s

thrd:2 cmd:w ARTree: begin
thrd:2 cmd:w random keys:10000000
thrd:2 cmd:w file:10000000 records processed: 10000000
thrd:2 cmd:w end

 real 0m9.522s
 user 0m34.750s
 sys  0m2.562s

thrd:3 cmd:w ARTree: begin
thrd:3 cmd:w random keys:10000000
thrd:3 cmd:w file:10000000 records processed: 10000000
thrd:3 cmd:w end

 real 0m9.578s
 user 0m34.781s
 sys  0m2.578s

Index ARTree summary scan:
 forward index cursor
 key order verification
 index key count
 Index scan complete
 Total keys 40000000

 real 0m18.846s
 user 0m18.828s
 sys  0m0.015s

Total memory allocated: 3165.380 MB
Bytes per key: 82

ARTree Index type 1 blks allocated: 12468681
ARTree Index type 2 blks allocated: 01688726
ARTree Index type 3 blks allocated: 00266305
ARTree Index type 6 blks allocated: 40000000
ARTree Index type 8 blks allocated: 92558615
ARTree Index type 9 blks allocated: 00000065

ARTree Index type 1 blks freed    : 01688726
ARTree Index type 2 blks freed    : 00266305
ARTree Index type 8 blks freed    : 12468616
ARTree Index type 9 blks freed    : 00000065


standalone testdb -prng=2 -stats -cmds=f -summary=vc -idxType=0 -bits=16 -noDocs -pennysort -threads=4 10000000
thread 0 launched for file 10000000 cmds f
thread 1 launched for file 10000000 cmds f
thread 2 launched for file 10000000 cmds f
thread 3 launched for file 10000000 cmds f

thrd:1 cmd:f ARTree: begin
thrd:1 cmd:f random keys:10000000
thrd:1 cmd:f file:10000000 records processed: 10000000
thrd:1 cmd:f end

 real 0m6.206s
 user 0m23.578s
 sys  0m1.234s

thrd:2 cmd:f ARTree: begin
thrd:2 cmd:f random keys:10000000
thrd:2 cmd:f file:10000000 records processed: 10000000
thrd:2 cmd:f end

 real 0m6.289s
 user 0m23.812s
 sys  0m1.234s

thrd:0 cmd:f ARTree: begin
thrd:0 cmd:f random keys:10000000
thrd:0 cmd:f file:10000000 records processed: 10000000
thrd:0 cmd:f end

 real 0m6.357s
 user 0m23.937s
 sys  0m1.234s

thrd:3 cmd:f ARTree: begin
thrd:3 cmd:f random keys:10000000
thrd:3 cmd:f file:10000000 records processed: 10000000
thrd:3 cmd:f end

 real 0m6.393s
 user 0m23.984s
 sys  0m1.234s

Index ARTree summary scan:
 forward index cursor
 key order verification
 index key count
 Index scan complete
 Total keys 40000000

 real 0m18.281s
 user 0m18.281s
 sys  0m0.000s

Total memory allocated: 0.443 MB
Bytes per key: 0


**********************************************************************


c:\Users\Owner\Source\Repos\malbrain\database>testfiles\test7
"40M random 10 byte keys, multi-threaded insertions then subset cursor over btree1 index"

standalone testdb -prng=2 -stats -cmds=w -summary=k -idxType=1 -bits=16 -minKey=aaaA -maxKey=aaaK -noDocs -pennysort -threads=4 10000000
thread 0 launched for file 10000000 cmds w
thread 1 launched for file 10000000 cmds w
thread 2 launched for file 10000000 cmds w
thread 3 launched for file 10000000 cmds w

thrd:3 cmd:w Btree1: begin
thrd:3 cmd:w random keys:10000000
thrd:3 cmd:w file:10000000 records processed: 10000000
thrd:3 cmd:w end

 real 0m16.527s
 user 1m5.546s
 sys  0m0.562s

thrd:0 cmd:w Btree1: begin
thrd:0 cmd:w random keys:10000000
thrd:0 cmd:w file:10000000 records processed: 10000000
thrd:0 cmd:w end

 real 0m16.563s
 user 1m5.640s
 sys  0m0.562s

thrd:1 cmd:w Btree1: begin
thrd:1 cmd:w random keys:10000000
thrd:1 cmd:w file:10000000 records processed: 10000000
thrd:1 cmd:w end

 real 0m16.583s
 user 1m5.703s
 sys  0m0.562s

thrd:2 cmd:w Btree1: begin
thrd:2 cmd:w random keys:10000000
thrd:2 cmd:w file:10000000 records processed: 10000000
thrd:2 cmd:w end

 real 0m16.675s
 user 1m5.796s
 sys  0m0.562s

Index Btree1 summary scan:
 forward index cursor
 min key: <aaaA>
 max key: <aaaK>
aaaBCw`d42
aaaBONxqOK
aaaBm90ePd
aaaCctIYQw
aaaCgdnoHA
aaaCpkDRZ6
aaaD@tGrtK
aaaDL4uFgu
aaaDihSWuY
aaaDyhKhZg
aaaE0tam4P
aaaFiGHVmr
aaaG5S2Vod
aaaHQtKXpk
aaaHyG0MNI
aaaI4R9OlN
aaaIJp7aPf
aaaJBwd`ex
aaaJjg1Fhs
aaaKQGRIk0
aaaKcxeoHS
aaaKfOd4i3
aaaKshGObR
 Index scan complete
 Total keys 23

 real 0m0.019s
 user 0m0.000s
 sys  0m0.000s

Total memory allocated: 1031.715 MB
Bytes per key: 47036151

