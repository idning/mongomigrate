#!/usr/bin/env python
#coding: utf-8
#file   : migrate.py
#author : ning
#date   : 2013-06-14 16:23:48

import urllib, urllib2
import os, sys
import re, time
import logging
from pprint import pprint
import argparse
import threading
import collections
import socket
import shutil

socket.setdefaulttimeout(10)

from pcl.thread_pool import WorkerManager

import random
import pymongo
import bson
from pcl import common

PWD = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(PWD, '../lib/'))
sys.path.append(os.path.join(PWD, '../conf/'))

import ms_conf as conf



datalogger = logging.getLogger('datalogger')


class MException(Exception):
    def __init__(self, msg=None):
        Exception.__init__(self, msg)

class Mongo(pymongo.Connection):
    '''
    a sub class of pymongo Connection, add auth support.
    TODO: 这里应该保证所有操作在master 上进行.
    '''
    def __init__(self, host, user, passwd, replset=None):
        #logging.debug("init Mongo: host: %(host)s user: %(user)s passwd: %(passwd)s replset: %(replset)s" % locals())
        if replset:
            pymongo.Connection.__init__(self, host, replicaSet=replset)
        else:
            pymongo.Connection.__init__(self, host)

        db = self['admin']
        db.authenticate(user, passwd)
        db.read_preference = pymongo.ReadPreference.PRIMARY

#utils 
def _mongoimport(host, user, passwd, db_name, collection_name, data_file, callback=None, reimport_data_file=None):
    (host, port) = host.split(':')
    port = int(port)
    mongo_path = conf.MONGO_PATH

    cmd = '''%(mongo_path)s/mongoimport
    -u %(user)s -p %(passwd)s
    --host %(host)s --port %(port)d 
    --db %(db_name)s --collection %(collection_name)s
    --file %(data_file)s 
     ''' % locals()

    cmd = re.sub('\n *', ' ', cmd)
    r = common.system(cmd, logging.debug)
    logging.debug(r)


    #callback 
    m = re.search(r'imported (\d+) objects', r, re.DOTALL)
    if not m:
        info = "error on import: " + r
        logging.error(info)
        raise MException(info)

    logging.debug(m.group(0)) 
    records_import = int(m.group(1))

    if callback:
        callback(records_import)

    if reimport_data_file:
        reimport_fd = file(reimport_data_file, 'a')
        #write it to the reimport records file
        for line in r.split('\n'):
            pos = line.find('error_on_import:')
            if pos != -1:
                pos += len('error_on_import:')
                line = line[pos:]
                print >> reimport_fd, line

        reimport_fd.close()

def _mongoexport(host, user, passwd, db_name, collection_name, data_file, query='', callback=None):
    (host, port) = host.split(':')
    port = int(port)

    mongo_path = conf.MONGO_PATH

    cmd = '''%(mongo_path)s/mongoexport
    -u %(user)s -p %(passwd)s
    --host %(host)s --port %(port)d 
    --db %(db_name)s --collection %(collection_name)s
    %(query)s
    >> %(data_file)s 
    ''' % locals()

    cmd = re.sub('\n *', ' ', cmd)
    r = common.system(cmd, logging.debug)
    logging.debug(r)

    #callback 
    m = re.search(r'exported (\d+) records', r, re.DOTALL)
    if not m:
        info = "error on export, " + r
        logging.error(info)
        raise MException(info)
    records_export = int(m.group(1))

    if callback:
        callback(records_export)


class OplogReplayer(threading.Thread):
    def __init__(self, src, dest, start_ts, src_ns, dest_ns): 
        '''
        src: 源 (host, user, pass[, replset]) 
        dest: 目的(host, user, pass[, replset])  
        start_ts: 从哪里开始
        src_ns: 重放的时候，要求ns匹配 prefix
        dest_ns: 重放的时候，重放到这个ns去
        '''

        threading.Thread.__init__ (self, name = 'T' + src[0])
        self.src = src
        self.dest = dest
        self.src_ns = src_ns
        self.dest_ns = dest_ns

        self.start_ts = start_ts
        self.stop_ts = sys.maxint

        self.syncdelay = 1000000        # default sync delay,  > 10days
        self.records_applied = 0
        self.records_scaned = 0
        self.records_apply_stat = {'i': 0, 'u': 0, 'd': 0, 'n': 0, 'c': 0}
        self.log_precision = 10 # 每多少条打印一条日志


    def run(self):
        try: 
            self._doit()
        except KeyboardInterrupt:
            print("Got Ctrl+C, exiting...")
        except MException, e:
            logging.error("got exception on oplogreplay, " + str(e))
            return
        pass

    def check_stop(self, ts):
        if ts < self.stop_ts: 
            return False
        ## it's done
        logging.info("oplog replay in time [%d - %d], current op.ts: %d stop it!" % (self.start_ts, self.stop_ts, ts))
        for k,v in self.records_apply_stat.items():
            logging.info('oplog %s: %d'% (k, v))
        return True

    def _doit(self):
        logging.info("starting " + str(self))

        src_conn = Mongo(*self.src)
        dest_conn = Mongo(*self.dest)
        db_name, collection_name = self.dest_ns.split('.', 1)
        dest_collection = dest_conn[db_name][collection_name]

        #check if oplog overrun
        oplog = src_conn.local['oplog.rs'].find().sort("$natural", pymongo.ASCENDING)
        op = oplog.next()
        firstop = op['ts'].time 
        if firstop > self.start_ts:
            raise MException("oplog overrun, cancel it!!!!")

        start_ts = bson.timestamp.Timestamp(self.start_ts, 0)
        q = {"ts": {"$gte": start_ts}}

        sort = [("$natural", pymongo.ASCENDING)]
        oplog = src_conn.local['oplog.rs'].find(q, tailable=True, await_data=True, timeout=False, sort=sort)
        oplog.add_option(pymongo.cursor._QUERY_OPTIONS['oplog_replay']) # do not need to patch pymongo

        records_applied = 0
        records_scaned = 0

        while oplog.alive:
            #Get Op
            try:
                if records_scaned == 0: logging.debug("waiting for first oplog")
                op = oplog.next()
                if records_scaned == 0: logging.debug("get first oplog")
                records_scaned += 1
                #logging.debug('getop: ' + str(op))
            except StopIteration:
                logging.debug("[records_scaned:%d][records_applied:%d] waiting for new data..." % (records_scaned, records_applied))
                self.syncdelay = 0
                if self.check_stop(time.time()):
                    return 
                time.sleep(1)
                continue
            except bson.errors.InvalidDocument as e:
                logging.warn(src + repr(e))
                continue

            ts = op['ts']
            if self.check_stop(ts.time):
                return 

            ts_time_str = common.format_time(ts.time)
            delay = time.time() - ts.time
            self.syncdelay = delay

            if not op['ns'].startswith(self.src_ns):
                continue

            records_applied += 1
            if 0 == (records_applied % self.log_precision):
                if self.log_precision == 1: # 此时，用户已经切换了写的集群, 理论上应该不再有oplog
                    log_fun = logging.warn 
                else:
                    log_fun = logging.debug

                log_fun("[records_scaned: %d] [records_applied:%d] \t%s\t%s -> %s \t[t:%d(%s)][delay:%d]" %
                             (records_scaned, records_applied, 
                             op.get('op'), op.get('ns'), self.dest_ns, 
                              ts.time, ts_time_str, delay )  )

            self.records_scaned = records_scaned
            self.records_applied = records_applied
            # Apply operation
            try:
                self.apply_op(dest_collection, op)
            except pymongo.errors.OperationFailure as e:
                logging.warn(repr(e))
                raise

    def apply_op(self, dest_coll, raw):
        ns = raw['ns']
        op = raw['op']
        self.records_apply_stat[op] += 1
        if op == 'i':
            try:
                dest_coll.insert(raw['o'], safe=True)
            except pymongo.errors.DuplicateKeyError, e:
                datalogger.warn(str(e) + str(raw))
        elif op == 'u':
            try:
                dest_coll.update(raw['o2'], raw['o'], safe=True)
            except pymongo.errors.DuplicateKeyError, e:
                datalogger.error(str(e) + str(raw))

        elif op == 'd':
            dest_coll.remove(raw['o'], safe=True)
        #elif op == 'c':
            #command(ns=ns, raw=raw)
        #elif op == 'db':
            #db_declare(ns=ns, raw=raw)
        elif op == 'n':
            pass
        else:
            msg = "ignore Unknown op: " + str(raw)
            logging.error(msg)
            #raise(Exception(msg))

    def __str__(self):
        return 'OplogReplayer[%s(%s) => %s(%s)]' % (self.src[0], self.src_ns, self.dest[0], self.dest_ns)
            
    def set_stop_ts(self, stop_ts):
        self.stop_ts = stop_ts

    def set_log_precision(self, log_precision):
        self.log_precision = log_precision

class Migrator:
    def __init__(self, src, dest, src_ns, dest_ns, args):
        self.src = src
        self.dest  = dest
        self.src_ns = src_ns
        self.dest_ns = dest_ns
        self.args = args
        self.src_chunks = None

        # stat info
        self.pre_src_count = 0              #预检查时src count
        self.pre_dest_count = 0             #预检查时dest count
        self.post_src_count = 0
        self.post_dest_count = 0

        self.records_export = 0
        self.records_import = 0
        self.records_oplogreplay = 0

        self.lastop_ts = 0

        self.orig_src_balancer_disabled = None
        self.orig_dest_balancer_disabled = None

        # src/dest conn
        self.src_conn = Mongo(*self.src)
        self.dest_conn = Mongo(*self.dest)

        self.data_file = 'data/%s.data' % (self.src_ns)
        self.reimport_data_file = 'data/%s.data/reimport' % (self.src_ns)

    def run(self):
        logging.info("migrate start: " + str(self) )

        steps = [
            self.do_pre_check, 
            self.do_getlastop, 
            self.do_export_parallel, 
            self.do_shard, 
            self.do_copyindex, 
            #self.do_export, 
            #self.do_import, 
            self.do_import_parallel, 
            self.do_oplogreplay, 
            self.do_post_check, 
        ]

        if self.args.steps:
            lst = self.args.steps.split(',')
            steps = [eval('self.do_%s' % i) for i in lst]
        if self.args.replay_from: 
            self.lastop_ts = self.args.replay_from
            steps = [
                self.do_pre_check, 
                self.do_oplogreplay, 
                self.do_post_check, 
            ]

        #run steps 
        timecost = {}
        for (step, func) in enumerate(steps):
            logging.info('[%d] run %s .........................................................' % (step, func.__name__))
            t1 = time.time()

            func()

            t2 = time.time()
            timecost[func] = t2-t1
            if self.args.stepbystep:
                raw_input("Press Enter to continue...")

        self.do_summary(steps, timecost)

    def do_summary(self, steps, timecost):
        logging.info('###################  Summary #####################')
        for k in steps:
            logging.info('time of %s : %.2f s' % (k.__name__, timecost[k]))
        all_time = sum(timecost.values())

        logging.info('%s [pre_src_count: %d] [pre_dest_count: %d] [post_src_count: %d] [post_dest_count: %d]' % 
                     (str(self), self.pre_src_count, self.pre_dest_count, self.post_src_count, self.post_dest_count))
        logging.info('%s [records_export: %d] [records_import: %d][records_oplogreplay: %d] in [all_time_cost:%.2fs]' % 
                     (str(self), self.records_export, self.records_import, self.records_oplogreplay, all_time))

        logging.info('###################   END   #####################')

    def do_shard(self):
        '''
        sh.enableSharding('report')
        sh.shardCollection('report.xxxx', {uuid:1})
        '''

        shardkey = self._get_src_shardkey()
        if not shardkey:
            raise MException("src is not sharded!")

        dest_db, dest_collection = self.dest_ns.split('.', 1)
        #1. enablesharding
        try:
            self.dest_conn.admin.command('enablesharding', dest_db)
        except  pymongo.errors.OperationFailure, e:
            if str(e).find('already enabled') >= 0:
                logging.warn('sharding already enabled on db: ' + dest_db)
            else:
                logging.error('error on enableSharding ' + dest_db)
                raise

        #2. shardCollection
        try:
            #根据源上的shardkey 进行 shard
            self.dest_conn.admin.command('shardCollection', self.dest_ns, key=shardkey) 
        except  pymongo.errors.OperationFailure, e:
            if str(e).find('already sharded') >= 0:
                logging.warn('already sharded on collection: ' + dest_collection)
            else:
                logging.error('error on shardCollection ' + dest_collection)
                raise

        shards = self._get_dest_shard_names()

        self.ii = 0 # we can not use nolocal in python 2.7...
        def random_shard():
            s = shards[self.ii]
            self.ii += 1
            self.ii %= len(shards)
            return s


        #3. Pre-Splitting
        for c in self.src_chunks:
            split = c['max']
            if str(c['max']).find('MaxKey') != -1:
                continue
            try:
                logging.info('split chunk %s @ %s' % (self.dest_ns, str(split))  )
                self.dest_conn.admin.command('split', self.dest_ns, middle=split) 
            except Exception as e:
                logging.warn('exception: ' + str(e))

        #4. moveChunk
        for c in self.src_chunks:
            split = c['max']
            if str(c['max']).find('MaxKey') != -1:
                continue
            to_shard = random_shard()
            try:
                logging.info('move chunk %s [find:%s] [dest:%s]' % (self.dest_ns, str(split), to_shard ))
                self.dest_conn.admin.command('moveChunk', self.dest_ns, find=split, to=to_shard) 
            except Exception as e:
                logging.warn('exception: ' + str(e))

    def do_pre_check(self):
        mongo_path = conf.MONGO_PATH
        cmd = '''%(mongo_path)s/mongoimport --version ''' % locals()
        r = common.system(cmd, logging.debug)
    
        self.orig_src_balancer_disabled = self._is_balancer_disabled(self.src_conn)
        self.orig_dest_balancer_disabled = self._is_balancer_disabled(self.dest_conn)

        #check if balance stopped
        if self.args.stop_balancer:
            if not self.orig_src_balancer_disabled:
                logging.info("disable balancer for %s !!! " % str(self.src_conn))
                self._disable_balancer(self.src_conn)
            if not self.orig_dest_balancer_disabled:
                logging.info("disable balancer for %s !!! " % str(self.dest_conn))
                self._disable_balancer(self.dest_conn)

        self._check_balancer_disabled(self.src_conn)
        self._check_balancer_disabled(self.dest_conn)

        #check count
        src_cnt = self._src_data_conn().count()
        dest_cnt = self._dest_data_conn().count()
        self.pre_src_count = src_cnt
        self.pre_dest_count = dest_cnt
        if dest_cnt:
            if not self.args.force:
                raise MException("in do_pre_check: dest already has %d records " % dest_cnt)
            logging.warn("befor migrate, dest collection got records: %d" % dest_cnt)
        logging.info("in pre_check [src_count: %d] [dest_cnt: %d]" % (src_cnt, dest_cnt))

        #check replset
        logging.info("in pre_check src_replsets : %s " % (str(self._get_src_replsets())))

        #check shard info
        self._print_src_shard_info()
        self._print_dest_shard_info()

        if os.path.exists(self.data_file):
            if not self.args.force:
                raise MException("in do_export data file: %s already exist"%self.data_file)

            logging.warn("data file: %s already exist, we clean it" % self.data_file)
            shutil.rmtree(self.data_file)
        os.makedirs(self.data_file) #作为目录

        #save info

        self.src_chunks = self._get_src_chunks()

    def do_post_check(self):
        #enable balancer
        if self.args.stop_balancer:
            if not self.orig_src_balancer_disabled:
                logging.info("enable balancer for %s !!! " % str(self.src_conn))
                self._enable_balancer(self.src_conn)
            if not self.orig_dest_balancer_disabled:
                logging.info("enable balancer for %s !!! " % str(self.dest_conn))
                self._enable_balancer(self.dest_conn)

        #check count
        src_cnt = self._src_data_conn().count()
        dest_cnt = self._dest_data_conn().count()

        logging.info("in post_check, [src_count:%d] [dest_cnt: %d]" % (src_cnt, dest_cnt))
        self.post_src_count = src_cnt
        self.post_dest_count = dest_cnt

        self._print_src_shard_info()
        self._print_dest_shard_info()

    def do_copyindex(self):
        '''
        创建索引, 包括 name, uniq 选项.
        '''

        indexes = self._get_src_indexes()

        for name, idx in indexes.items():
            logging.info('ensure_index: %s %s' %(name, idx))
            keys = idx['key']
            keys = [(a, int( b)) for a,b in keys]

            unique =  False
            if 'unique' in idx:
                unique = idx['unique']

            self._dest_data_conn().ensure_index(keys, name=name, unique = unique)

    def do_export_parallel(self):
        host, user, passwd = self.src
        db_name, collection_name = self.src_ns.split('.')


        shardkey = self.src_chunks[0]['min'].keys()[0]
        shardkey_type = type(self.src_chunks[0]['max'][shardkey])

        wm = WorkerManager(self.args.worker)  
        for c in self.src_chunks:
            r1 = str(c['min'][shardkey]).replace('()', '')
            r2 = str(c['max'][shardkey]).replace('()', '')

            #为每个chunk计算一个文件名
            this_data_file = '%s-%s' % (r1, r2)
            c['data_file'] = self.data_file + '/' + this_data_file

            logging.debug("add export task %s -- %s @ %s -> %s " % (c['min'], c['max'], c['shard'], c['data_file']))

            cond = []
            if r1 != 'MinKey':
                if shardkey_type in [int, long]:
                    cond.append('$gte: %s' % r1)
                elif shardkey_type in [str, unicode]:
                    cond.append('$gte: "%s"' % r1)
                else:
                    raise MException("unknown shardkey_type: %s" % shardkey_type)
            if r2 != 'MaxKey':
                if shardkey_type in [int, long]:
                    cond.append('$lt: %s' % r2)
                elif shardkey_type in [str, unicode]:
                    cond.append('$lt: "%s"' % r2)
            cond = ','.join(cond)
            query = "-q '{ $query: {%(shardkey)s: {%(cond)s}}}'" % locals()
            wm.add_job(_mongoexport, host, user, passwd, db_name, collection_name, c['data_file'], query, self.export_callback)

        wm.start()  
        wm.wait_for_complete()  

        wc = common.system('wc -l %s/* | tail -1' % self.data_file, logging.debug)
        wc_records_export = int(wc.split()[0])
        logging.debug("wc_records_export: %d" % wc_records_export)

        if wc_records_export != self.records_export:
            info = "wc_records_export != self.records_export"
            raise MException(info)

    def export_callback(self, records_export):
        self.records_export += records_export
        if self.pre_src_count:
            logging.info("%d/%d (%.2f%%) exported"%(self.records_export, self.pre_src_count, 100.*self.records_export/self.pre_src_count))


    def import_callback(self, records_import):
        self.records_import += records_import
        if self.pre_src_count:
            logging.info("%d/%d (%.2f%%) imported"%(self.records_import, self.pre_src_count, 100.*self.records_import/self.pre_src_count))

    def do_import_parallel(self):
        host, user, passwd = self.dest
        db_name, collection_name = self.dest_ns.split('.')

        wm = WorkerManager(self.args.worker)  
        for c in self.src_chunks:
            logging.debug("import %s -- %s @ %s from %s " % (c['min'], c['max'], c['shard'], c['data_file']))
            wm.add_job(_mongoimport, host, user, passwd, db_name, collection_name, c['data_file'], self.import_callback, self.reimport_data_file)

        wm.start()  
        wm.wait_for_complete()  

        #重插入失败条目
        while os.path.exists(self.reimport_data_file) and os.path.getsize(self.reimport_data_file):
            tmp_file = self.reimport_data_file + '.' + str(random.randint(1000000000, 3000000000))
            if os.path.exists(tmp_file): 
                continue
            os.rename(self.reimport_data_file, tmp_file)
            _mongoimport(host, user, passwd, db_name, collection_name, tmp_file, self.import_callback, self.reimport_data_file)

        logging.info("import done [records_import: %d]" % (self.records_import))
        if self.records_import != self.records_export:
            logging.warning('self.records_import != self.records_export')

        logging.info('after import: %s [pre_src_count: %d] [pre_dest_count: %d] [post_src_count: %d] [post_dest_count: %d]' % 
                     (str(self), self.pre_src_count, self.pre_dest_count, self.post_src_count, self.post_dest_count))
        logging.info('after import: %s [records_export: %d] [records_import: %d][records_oplogreplay: %d] ' % 
                     (str(self), self.records_export, self.records_import, self.records_oplogreplay ))

    def do_export(self):
        host, user, passwd = self.src

        if os.path.exists(self.data_file):
            if not self.args.force:
                raise MException("in do_export data file: %s already exist"%self.data_file)

            logging.warn("data file: %s already exist, we clean it" % self.data_file)
            shutil.rmtree(self.data_file)

        logging.info("time estimate for export : %d min (5000q/s)" % (self.pre_src_count/5000/60))
        db_name, collection_name = self.src_ns.split('.')

        _mongoexport(host, user, passwd, db_name, collection_name, self.data_file)

        size = os.path.getsize(self.data_file)
        wc = common.system('wc -l %s ' % self.data_file, logging.debug)
        self.records_export = int(wc.split()[0])

        logging.info("export done [data_file:%s] [size:%d] [records:%d]" % (self.data_file, size, self.records_export))
        logging.info("time estimate for import: %d min (5000/s)" % (self.records_export/5000/60))

    def do_import(self):
        host, user, passwd = self.dest

        db_name, collection_name = self.dest_ns.split('.')
        _mongoimport(host, user, passwd, db_name, collection_name, self.data_file)

    def do_getlastop(self):
        '''
        保存迁移开始时各个set 的 ts
        '''

        _host, user, passwd = self.src

        self.lastop_ts = sys.maxint
        for host, replset in self._get_src_replsets():
            conn = Mongo(host, user, passwd, replset)
            oplog = conn.local['oplog.rs'].find().sort("$natural", pymongo.DESCENDING)
            op = oplog.next()
            lastop = op['ts'].time 
            if lastop < self.lastop_ts:
                self.lastop_ts = lastop

            oplog = conn.local['oplog.rs'].find().sort("$natural", pymongo.ASCENDING)
            op = oplog.next()
            firstop = op['ts'].time 
            logging.info("firstop: %d(%s) - lastop: %d(%s) time: %.2f hours" % (firstop, common.format_time(firstop), lastop, common.format_time(lastop), (lastop-firstop)/3600.))

        self.lastop_ts -= 10
        logging.info("lastop_ts : %d (%s)" % (self.lastop_ts, common.format_time(self.lastop_ts)) )

    def do_oplogreplay(self):
        _host, user, passwd = self.src

        self.replayers = []
        for host, replset in self._get_src_replsets():
            _src = (host, user, passwd, replset)
            replayer = OplogReplayer(_src, self.dest, self.lastop_ts, self.src_ns, self.dest_ns) 
            replayer.start()
            self.replayers.append(replayer)

        # check if it's done!
        while True:
            max_syncdelay = self.print_oplogreplayer_info()

            if max_syncdelay < 2: 
                #askuser 
                can_stop = common.input_with_timeout("All delays in 2 seconds, you got 10 second to type 'yes' for stop replayer:", 10, 'no') 
                if can_stop != 'yes':
                    continue

                #####2. while run check !!!
                logging.info("we will replay for 60 more seconds. do not ctrl^C it")
                stop_ts = time.time() + 60 # delay 60 seconds
                for replayer in self.replayers:
                    replayer.set_stop_ts(stop_ts)
                    replayer.set_log_precision(1) # 之后每条oplog 都打印

                for replayer in self.replayers:
                    replayer.join()
                    self.records_oplogreplay += replayer.records_applied 
                return 

            time.sleep(10)
            
    def print_oplogreplayer_info(self):
        alives = [p.isAlive() for p in self.replayers]
        #if sum(alives) == 0:
            #logging.info('all replayer is done!! ')
            #return -1
            
        arr = [p.syncdelay for p in self.replayers]
        msg = [('%.2fs' % i) for i in arr]
        msg = ', '.join(msg)
        logging.info("delays: " + msg)
        max_syncdelay = max(arr)
        return max_syncdelay

    def __str__(self):
        return 'Migrator [%s(%s) => %s(%s)]' % (str(self.src[0]), self.src_ns, str(self.dest[0]), self.dest_ns)


    #data connections 
    def __data_conn(self, conn, ns):
        db_name, collection_name = ns.split('.')
        return conn[db_name][collection_name]
    def _src_data_conn(self):
        return self.__data_conn(self.src_conn, self.src_ns)
    def _dest_data_conn(self):
        return self.__data_conn(self.dest_conn, self.dest_ns)

    #shardkey info
    def __get_shardkey(self, conn, ns):
        lst = list(conn['config']['collections'].find({'_id': ns}))
        if not lst:
            return None
        return lst[0]['key'] #TODO: 多个shardkey 的情况.
    def _get_src_shardkey(self):
        return self.__get_shardkey(self.src_conn, self.src_ns)
    def _get_dest_shardkey(self):
        return self.__get_shardkey(self.dest_conn, self.dest_ns)

    #chunks info
    def __get_chunks(self, conn, ns):
        lst = list(conn['config']['chunks'].find({'ns': ns}))
        return lst
    def _get_src_chunks(self):
        return self.__get_chunks(self.src_conn, self.src_ns)
    def _get_dest_chunks(self):
        return self.__get_chunks(self.dest_conn, self.dest_ns)

    #index info
    def _get_src_indexes(self):
        return self._src_data_conn().index_information()
    def _get_dest_indexes(self):
        return self._dest_data_conn().index_information()

    def _disable_balancer(self, conn):
        return conn['config']['settings'].update({'_id': 'balancer'}, {'$set': { 'stopped': True }}, upsert=True)
    def _enable_balancer(self, conn):
        return conn['config']['settings'].update({'_id': 'balancer'}, {'$set': { 'stopped': False}}, upsert=True)

    def _is_balancer_disabled(self, conn):
        b = list(conn['config']['settings'].find({'_id': 'balancer'}))

        logging.debug("balancer info of %s : %s" % (str(conn), str(b)))
        if b and ('stopped' in b[0]) and (b[0]['stopped'] == True):
            return True
        return False

    def _check_balancer_disabled(self, conn):
        if self._is_balancer_disabled(conn):
            pass 
        else:
            if not self.args.force:
                raise MException("balance is not disabled on " + str(conn) + 'you can use --stop_balancer to disable it')

    #print shard info
    def __print_shard_info(self, chunks, msg):
        logging.info(msg + ":: ")
        shards_stat = collections.defaultdict(int)
        for c in chunks:
            logging.debug("%s -- %s @ %s" % (c['min'], c['max'], c['shard']))
            shards_stat[c['shard']] += 1

        for s in shards_stat:
            logging.info('%s: %d chunks' % (s, shards_stat[s]))

    def _print_src_shard_info(self):
        chunks = self._get_src_chunks()
        self.__print_shard_info(chunks, 'src_shard_info')
    def _print_dest_shard_info(self):
        chunks = self._get_dest_chunks()
        self.__print_shard_info(chunks, 'dest_shard_info')

    #misc
    #def _mongo_sh_status(self):
        #'''
        #目标sharding上执行sh.status()
        #'''
        #host, user, passwd = self.dest
        #mongo_path = conf.MONGO_PATH

        #cmd = '''%(mongo_path)s/mongo
        #-u %(user)s -p %(passwd)s
        #%(host)s/admin
        #--eval 'sh.status()'
         #''' % locals()

        #cmd = re.sub('\n *', ' ', cmd)
        #r = common.system(cmd, logging.debug)
        #logging.debug(r)

    def _get_src_replsets(self): 
        '''
        从这个sharing的config 里面，拿到后面有多少个 replset, 返回每个replset 的host, port

        mongos> db.shards.find()
        { "_id" : "cluster1", "host" : "cluster1/10.23.250.155:7112,10.23.250.156:7112,10.23.250.157:7112" }
        { "_id" : "cluster0", "host" : "cluster0/10.23.250.155:7111,10.23.250.156:7111,10.23.250.157:7111" }

        '''

        def get_setinfo(shard_info):
            '''
            return (host, replset)
            '''
            replset, hosts = shard_info['host'].split('/')
            host = hosts.split(',')[0]
            return host, replset
            
        sets = list(self.src_conn['config']['shards'].find())
        sets = [get_setinfo(i) for i in sets]
        return sets

    def _get_dest_shard_names(self):
        lst = list(self.dest_conn['config']['shards'].find())
        lst = [i['_id'] for i in lst]
        return lst

def discover_shard():
    lst = [s for s in dir(conf) if s.startswith('shard') ]
    return lst

def main():
    default_logfile = 'log/ms.log'

    parser= argparse.ArgumentParser()
    parser.add_argument('--src', choices=discover_shard(), required=True)
    parser.add_argument('--dest', choices=discover_shard(), required=True)
    parser.add_argument('--src_ns', required=True)
    parser.add_argument('--dest_ns')
    parser.add_argument('--worker', type=int, default = 5, help='num of worker for export/import')

    parser.add_argument('--force', help='allow data_file exist, allow dest has records, allow balancer not disable', 
        action='store_true')

    parser.add_argument('--steps')
    parser.add_argument('--replay_from', type=int)
    parser.add_argument('--stepbystep', help='will ask user permit every step', action='store_true')

    parser.add_argument('--stop_balancer', help='disable balancer on start and reenable on finish', action='store_true')

    args = common.parse_args2(default_logfile, parser)
    logging.info("running: " + ' '.join(sys.argv))

    
    if not args.dest_ns:
        args.dest_ns = args.src_ns

    if args.src == args.dest and args.src_ns == args.dest_ns: 
        logging.error("src = dest")
        exit()

    common.init_logging(datalogger, console=False, log_file_path="log/datalogger.%s" % args.src_ns)
    src = eval('conf.%s' % args.src)
    dest = eval('conf.%s' % args.dest)

    try:
        m = Migrator(src, dest, args.src_ns, args.dest_ns, args)
        m.run()
    except MException as e:
        #raise
        logging.error(str(e))
        exit()

if __name__ == "__main__":
    main()

