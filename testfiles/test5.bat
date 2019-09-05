@ECHO on
ECHO "10M random 10 byte keys, insertions then searches in btree1 index"

@ECHO off
del testdb*

standalone testdb -debug -stats -cmds=wf -summary=vc -idxType=0 -bits=16  -inMem -noDocs -pennysort 10000000

