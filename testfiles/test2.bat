@ECHO on
ECHO "40M random 10 byte keys, single threaded insertions into each index type"

@ECHO off
del testdb*

@ECHO on
standalone.exe testdb -stats -cmds=w -summary=vc -pipeline -idxType=2 -bits=16  -inMem -noDocs -pennysort 40000000

standalone.exe testdb -stats -cmds=w -summary=vc -pipeline -idxType=1 -bits=16  -inMem -noDocs -pennysort 40000000

standalone.exe testdb -stats -cmds=w -summary=vc -pipeline -idxType=0 -inMem -noDocs -pennysort 40000000
