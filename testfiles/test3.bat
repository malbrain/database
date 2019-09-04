rem multi-threaded  random key insert

standalone db -debug -stats -cmds=w -summary=vc -idxType=0 -bits=16  -inMem -noDocs -pennysort -threads=4 10000000
