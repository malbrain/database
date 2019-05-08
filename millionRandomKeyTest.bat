standalone.exe db -cmds=w -summary=vc -pipeline -idxType=2 -bits=16  -inMem -noDocs -pennysort 40000000
standalone.exe db -cmds=w -summary=vc -threads=4 -pipeline -idxType=1 -bits=16  -inMem -noDocs -pennysort 40000000
standalone.exe db -cmds=w -summary=vc -threads=4 -pipeline -idxType=0 -bits=16  -inMem -noDocs -pennysort 40000000
