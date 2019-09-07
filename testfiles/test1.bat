@ECHO off
@ECHO Alpha testing of binary string fields

del testdb*

@ECHO expect 13 sorted keys
@ECHO on

standalone testdb -cmds=w -summary=skn -idxBinary=: testfiles/test1
