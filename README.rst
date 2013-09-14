mongomigrate
============

ms.py (migrate between sharding clusters)
-----------------------------------------

steps: 

- do_pre_check, 
    - disable balancer
    - src/dest count
    - print src/dest chunks 
- do_getlastop, 
    - check if oplog is large enough for this migration 
    - save ``lastop`` timestamp
- do_export_parallel, 
    - export, chunk by chunk
- do_shard, 
    - enable sharding for dest db/collection. 
    - pre-split and moveChunk (for balance in dest cluster)
- do_copyindex, 
    - copy index
- do_import_parallel, 
    - import, chunk by chunk. 
    - we can use a safe version of mongoimport, and retry 
- do_oplogreplay, 
    - replay oplog, from ``lastop``. 
- do_post_check, 
    - enable balancer again
    - src/dest count
    - print src/dest chunks 

dependency
----------

- pymongo: https://github.com/mongodb/mongo-python-driver
- pcl: https://github.com/idning/pcl

config
------

conf/ms_conf.py::

    #mongo binary dir
    MONGO_PATH = '/home/ning/bin'
    
    #set a name for sharding clusters 
    shard_online_0 = ('10.23.250.156:7333', 'user', 'passwd')
    shard_offline_0 = ( '10.65.19.52:37333', 'user', 'passwd')

basic usage
-----------

::

    ./bin/ms.py --src shard_online_0 --dest shard_offline_0 --src_ns db.collection --dest_ns db.collection -v --stop_balancer


replay oplog only
-----------------

if something bad intercept the oplogreplay, we can just replay oplog from some timestamp::

    ./bin/ms.py --src shard_online_0 --dest shard_offline_0 --src_ns db.collection --dest_ns db.collection -v --replay_from 1379127546

set worker num
--------------

::

    --worker 5


