@ECHO on
ECHO Alpha testing of binary string fields

@ECHO off
del testdb*
standalone testdb -cmds=w -noDocs -idxBinary testfiles/test1

@ECHO on
ECHO expect 13 sorted keys
@ECHO off
standalone testdb -cmds=s -noDocs -idxBinary testfiles/test1
