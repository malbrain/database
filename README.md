malbrain/database
==========================

A working project for High-concurrency B-tree/ARTree Database source code in C.  This project was created as a sub-module for the /www.github.com/malbrain/javascript-database project, but it can also be used by itself as a database/indexing library.

Compile with ./build or build.bat

The runtime options are:

    Usage: dbtest db_name -cmds=[crwsdf]... -idxType=[012] -bits=# -xtra=# -inMem -txns -noDocs -keyLen=# src_file1 src_file2 ... ]
      where db_name is the prefix name of the database file
      cmds is a string of (c)ount/(r)ev scan/(w)rite/(s)can/(d)elete/(f)ind, with a one character command for each input src_file, or a no-input command.
      idxType is the type of index: 0 = ART, 1 = btree1, 2 = btree2
      keyLen is key size, zero for whole line
      bits is the btree page size in bits
      xtra is the btree leaf page extra bits
      inMem specifies no disk files
      noDocs specifies keys only
      txns indicates use of transactions
      src_file1 thru src_filen are files of keys/documents separated by newline

Linux compilation command:

    [karl@test7x64 xlink]# cc -std=c11 -O2 -g -o dbtest standalone.c db*.c artree/*.c btree1/*.c -lpthread

Sample single thread output from indexing 40M pennysort keys:

    [karl@test7x64 xlink]# ./dbtest tstdb -cmds=w -noDocs -keyLen=10 pennykey0-3
    started indexing for pennykey0-3
     Total keys indexed 40000000
     real 0m35.022s
     user 0m31.067s
     sys  0m4.048s

    -rw-rw-r-- 1 karl engr    1048576 Oct 18 09:16 tstdb
    -rw-rw-r-- 1 karl engr 2147483648 Oct 18 09:16 tstdb.ARTreeIdx

Sample four thread output from indexing 40M pennysort keys:

    [karl@test7x64 xlink]# ./dbtest tstdb -cmds=w -noDocs -keyLen=10 pennykey[0123]
    started indexing for pennykey0
    started indexing for pennykey1
    started indexing for pennykey2
    started indexing for pennykey3
     Total keys indexed 10000000
     Total keys indexed 10000000
     Total keys indexed 10000000
     Total keys indexed 10000000
     real 0m12.176s
     user 0m41.103s
     sys  0m4.849s

Sample four thread output from finding 40M pennysort keys:
    [karl@test7x64 xlink]$ ./dbtest tstdb -cmds=f -keyLen=10 -idxType=0 -noDocs pennykey[0123]
    started finding keys for pennykey2
    started finding keys for pennykey0
    started finding keys for pennykey3
    started finding keys for pennykey1
    finished pennykey0 for 10000000 keys, found 10000000
    finished pennykey2 for 10000000 keys, found 10000000
    finished pennykey1 for 10000000 keys, found 10000000
    finished pennykey3 for 10000000 keys, found 10000000
     real 0m9.049s
     user 0m35.146s
     sys  0m0.905s

Sample single thread output from indexing 80M pennysort keys:

    [karl@test7x64 xlink]# ./dbtest tstdb -cmds=w -noDocs -keyLen=10 pennykey0-7
    started indexing for pennykey0-7
     Total keys indexed 80000000
     real 1m26.262s
     user 1m15.104s
     sys  0m10.763s

Sample eight thread output from indexing 80M pennysort keys:

    [karl@test7x64 xlink]# ./dbtest tstdb -cmds=w -noDocs -keyLen=10 pennykey[01234567]
    started indexing for pennykey0
    started indexing for pennykey1
    started indexing for pennykey2
    started indexing for pennykey3
    started indexing for pennykey4
    started indexing for pennykey5
    started indexing for pennykey6
    started indexing for pennykey7
     Total keys indexed 10000000
     Total keys indexed 10000000
     Total keys indexed 10000000
     Total keys indexed 10000000
     Total keys indexed 10000000
     Total keys indexed 10000000
     Total keys indexed 10000000
     Total keys indexed 10000000
     real 0m21.129s
     user 1m37.937s
     sys  0m19.619s

    -rw-rw-r-- 1 karl engr    1048576 Oct 18 06:22 tstdb
    -rw-rw-r-- 1 karl engr 4294967296 Oct 18 06:22 tstdb.ARTreeIdx

Sample output from finding 80M pennysort records:

    [karl@test7x64 xlink]$ ./dbtest tstdb -cmds=f -keyLen=10 -noDocs pennykey[01234567]
    started finding keys for pennykey1
    started finding keys for pennykey2
    started finding keys for pennykey0
    started finding keys for pennykey4
    started finding keys for pennykey3
    started finding keys for pennykey5
    started finding keys for pennykey6
    started finding keys for pennykey7
    finished pennykey5 for 10000000 keys, found 10000000
    finished pennykey2 for 10000000 keys, found 10000000
    finished pennykey4 for 10000000 keys, found 10000000
    finished pennykey0 for 10000000 keys, found 10000000
    finished pennykey3 for 10000000 keys, found 10000000
    finished pennykey1 for 10000000 keys, found 10000000
    finished pennykey6 for 10000000 keys, found 10000000
    finished pennykey7 for 10000000 keys, found 10000000
     real 0m12.355s
     user 1m32.415s
     sys  0m1.861s

Sample output from storing/indexing/persisting 40M pennysort records (4GB):

    [karl@test7x64 xlink]# ./dbtest tstdb -cmds=w -keyLen=10 penny0-3
    started indexing for penny0-3
     Total keys indexed 40000000
     real 4m38.547s
     user 1m6.353s
     sys  0m19.409s

    -rw-rw-r-- 1 karl karl     1048576 Oct 18 06:57 tstdb
    -rw-rw-r-- 1 karl karl 17179869184 Oct 18 07:01 tstdb.documents
    -rw-rw-r-- 1 karl karl  4294967296 Oct 18 07:01 tstdb.documents.ARTreeIdx

Sample output with four concurrent threads each storing 10M pennysort records:

    [karl@test7x64 xlink]# ./dbtest tstdb -cmds=w -keyLen=10 -idxType=1 penny[0123]
    started pennysort insert for penny0
    started pennysort insert for penny1
    started pennysort insert for penny2
    started pennysort insert for penny3
     real 0m42.312s
     user 2m20.475s
     sys  0m12.352s
 
    -rw-r--r-- 1 karl engr    1048576 Sep 16 22:15 tstdb
    -rw-r--r-- 1 karl engr 8589934592 Sep 16 22:16 tstdb.documents
    -rw-r--r-- 1 karl engr 2147483648 Sep 16 22:16 tstdb.documents.Btree1Idx

Sample cursor scan output and sort check of 40M pennysort records:

    [karl@test7x64 xlink]# export LC_ALL=C
    [karl@test7x64 xlink]# ./dbtest tstdb -cmds=s
    started scanning
     Total keys read 40000000
     real 0m28.190s
     user 0m22.578s
     sys  0m4.005s

Sample cursor scan count of 40M pennysort records:

    [karl@test7x64 xlink]# ./dbtest tstdb -cmds=c
    started counting
     Total keys counted 40000000
     real 0m20.568s
     user 0m18.457s
     sys  0m2.123s

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
