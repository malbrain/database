echo on

cl /Oxi /D CLOCK standalone.c timestamps.c
standalone 1 100000000
standalone 2 50000000
standalone 3 33333333
standalone 4 25000000
standalone 5 20000000
standalone 6 16666666
standalone 7 14280000

cl /Oxi /D RDTSC standalone.c timestamps.c
standalone 1 100000000
standalone 2 50000000
standalone 3 33333333
standalone 4 25000000
standalone 5 20000000
standalone 6 16666666
standalone 7 14280000
