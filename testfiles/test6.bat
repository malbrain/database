@ECHO off
@ECHO "40M random 10 byte keys, multi-threaded insertions then searches in btree1 index"

del testdb*

@ECHO on
standalone testdb -prng=2 -stats -cmds=w -summary=vc -idxType=0 -bits=16 -noDocs -pennysort -threads=4 10000000

standalone testdb -prng=2 -stats -cmds=f -summary=vc -idxType=0 -bits=16 -noDocs -pennysort -threads=4 10000000

