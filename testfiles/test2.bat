rem "40M random keys, single threaded"

standalone.exe db -stats -cmds=w -summary=vc -pipeline -idxType=2 -bits=16  -inMem -noDocs -pennysort 40000000

standalone.exe db -stats -cmds=w -summary=vc -pipeline -idxType=1 -bits=16  -inMem -noDocs -pennysort 40000000

standalone.exe db -stats -cmds=w -summary=vc -pipeline -idxType=0 -inMem -noDocs -pennysort 40000000
