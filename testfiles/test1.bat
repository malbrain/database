@ECHO on
ECHO Alpha testing of binary string fields

@ECHO off
del testdb*

@ECHO on
ECHO expect 13 sorted keys
@ECHO off
standalone testdb -cmds=w -keyList -summary=s -idxBinary=: testfiles/test1
