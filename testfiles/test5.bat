@ECHO off
@ECHO "10M random 10 byte keys, insertions then searches in ARTree index"

del testdb*

@ECHO on
standalone testdb -prng=2 -stats -cmds=wf -summary=vc -idxType=0 -bits=16  -inMem -noDocs -pennysort 10000000

