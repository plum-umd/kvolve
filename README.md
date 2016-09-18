# See our [Kvolve paper](https://www.cs.umd.edu/~mwh/papers/saur16kvolve.html) for full description #

## Kvolve files:  (See redis-2.8.17/src/)  ##
* **kvolve.c**: code for kvolve wrapper functions, command table, and command parsing
* **[kvolve.h](https://github.com/plum-umd/kvolve/blob/master/redis-2.8.17/src/kvolve.h)**: header listing all implemented wrapper functions
* **kvolve_internal.c**: update mechanics, internal state tracking, version tracking
* **[kvolve_internal.h](https://github.com/plum-umd/kvolve/blob/master/redis-2.8.17/src/kvolve_internal.h)**: header/documentation for update mechanics
* **[kvolve_upd.h](https://github.com/plum-umd/kvolve/blob/master/redis-2.8.17/src/kvolve_upd.h)**: header/documentation for user-supplied update functions
* **uthash.h**: a hashtable to help with data tracking

## Getting started: ##
Tested on Ubuntu 12.04, 14.04, RHEL 6.5
#### Making redis: ####
`make` in  redis-2.8.17  
If you get an error about [jemalloc](https://github.com/antirez/redis/issues/722): `cd deps; make jemalloc`

#### To run: ####
`./redis-2.8.17/src/redis-server`

#### To run tests: ####
`tests/redis_server_tests$ ./run_all_tests.sh`  
(tests in parent directory, not tests in redis directory)

## Changes made to redis-2.8.17 source code  (7 lines of code in redis-2.8.17/src/): ##

#### 4 lines of code to implement the version tag: ####

* **networking.c**  (2 changes)  
`< #include "kvolve.h"  `  
` <             kvolve_process_command(c);`

* **redis.h**   (1 change)  
`<    int vers;`

* **object.c**  (1 change)  
`<     o->vers = -1;`


## 3 lines of code so version data can be stored to the database: ##
* **rdb.c**:  
`int vers;`  
`if ((vers = rdbLoadType(&rdb)) == -1) goto eoferr;`  
`val->vers = vers;`  


