@ECHO on
ECHO "40M random keys, multi-threaded insertions into btree1 index""

@ECHO off
del testdb*

standalone testdb -debug -stats -cmds=w -summary=vc -idxType=0 -bits=16  -inMem -noDocs -pennysort -threads=4 10000000
