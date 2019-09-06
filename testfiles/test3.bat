@ECHO on
ECHO "40M random 10 byte keys, multi-threaded insertions into Adaptive Radix Tree index""

@ECHO off
del testdb*

@ECHO on
standalone testdb -debug -stats -cmds=w -summary=vc -idxType=0 -bits=16  -inMem -noDocs -pennysort -threads=4 10000000
