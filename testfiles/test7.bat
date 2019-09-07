@ECHO off
@ECHO "40M random 10 byte keys, multi-threaded insertions then subset cursor over btree1 index"

del testdb*

@ECHO on
standalone testdb -prng=2 -stats -cmds=w -summary=k -idxType=1 -bits=16 -minKey=aaaA -maxKey=aaaK -noDocs -pennysort -threads=4 10000000
