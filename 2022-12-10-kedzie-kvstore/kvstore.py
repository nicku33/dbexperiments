#!/usr/bin/env python
# coding: utf-8

import random 
import uuid
import time
import sys
import queue
import multiprocessing as mp
import dbm
import os
import pathlib
import hashlib
import sqlite3

import filelock    # pip install filelock

class KVTest:
    
    def __init__(self, dict_like_under_test):
        self.kv = dict_like_under_test
        
    def test01(self):
        assert self.kv.get("XYZ") is None
        self.kv["abc"] = "def"
        assert self.kv.get("abc") == "def"
        self.kv["abc"] = "ghi"
        assert self.kv.get("abc") == "ghi"
        
    def run_tests(self):
        self.test01()
        print("test01 passed")
        

# d = dict()
# tester = KVTest(d)
# tester.run_tests()


def single_test_simulator(q, kv_class, kv_config, iterations, key_range, runner_id=-1):
    
    total_time_ns = 0
    hits = 0
    bytes_stored = 0
    kv = kv_class(kv_config)

    r = random.Random()
    for i in range(iterations):
        k1 = str(r.randint(0,key_range))
        v1 = uuid.uuid4().hex
        
        t0 = time.time_ns()
        r1 = kv.get(k1)
        if r1 is not None:
            hits += 1
        else:
            kv.put(k1, v1)
            bytes_stored += len(k1) + len(v1)
        t1 = time.time_ns()
        total_time_ns += (t1 - t0);
        if (runner_id == 1) and (i % 5000 == 4999):
            print(f"at i={i}: avg_time_ms {(total_time_ns / i) / 1000000:.2f}")


    if q:
        q.put( (total_time_ns, hits, iterations, bytes_stored) )

class DummyLocalDictKV:
    
    def __init__(self, config):
        self.kv = {}
    
    def get(self, k):
        if k not in self.kv:
            return None
        return self.kv[k]
    
    def put(self, k, v):
        self.kv[k] = v



def process_results(res):
    time_taken,hits,iterations,bytes_stored  = res
    hit_rate = hits/iterations
    time_ms = time_taken / 1000000

    res = {'time_ms': time_ms, 
       'avg_cycle_ms': round(time_ms / iterations,4), 
       'hit_rate': round(hit_rate,6),
       'bytes_stored_mb': round(bytes_stored / (1024 ** 2),2)
      }
    return res



class Sharded:

    def __init__(self, config):
        self.base_path = config['base_path']
        self.levels = config['levels']

    def path_to_db(self, key: str):
        if self.levels == 0:
            return self.base_path + "/" + "single.dbm"
        # get hash
        s = hashlib.md5(key.encode("utf8")).hexdigest()
        path_parts = "/".join([s[(2*x):(2*x)+2] for x in range(self.levels)])
        db_path = self.base_path + "/" + path_parts + "_dbm"
        return db_path

class ShardedDBMLocked(Sharded):
    
    def __init__(self, config):
        super().__init__(config)

    def get(self, k):
        db_path = self.path_to_db(k)
        if not (os.path.exists(db_path)):
            return None
        self.lock = filelock.FileLock(db_path + ".lock")
        with self.lock:
            with dbm.open(db_path, 'c') as db:
                if k not in db:
                    return None
                return db[k].decode('utf8')

    def put(self, k, v):
        db_path = self.path_to_db(k)
        dir_path = pathlib.Path(os.path.dirname(db_path))
        dir_path.mkdir(parents=True, exist_ok=True)    # make if we need to
         
        lock = filelock.FileLock(db_path + ".lock")
        with lock:
            with dbm.open(db_path, 'c') as db:
                # print ("put into file " + db_path)
                db[k] = v.encode("utf8")

# in this one we rely on sqlite's own mechanisms
# https://www.sqlite.org/faq.html#q5
class SingleSQLite():

    def __init__(self, config):
        db_path = config['path']
        use_wal = config.get("use_wal", False)

        # if we are making the db from scratch we need to use our own locking
        # mechanism since sqlite doesn't write until first commit, 
        # alternatively, one could just prep the db in a single process, but
        # in a multiprocess setting we should do this
        lock = filelock.FileLock(db_path + ".lock")
        with lock:
            con = sqlite3.connect(db_path, timeout=20)
            # try new wal mechanism ?
            if use_wal:
                con.execute('PRAGMA journal_mode=wal')
            res = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='kv'").fetchall()
            if not res:
                con.execute("create table kv (k varchar, v varchar)")
                con.execute("create unique index k_uniq on kv(k)")
                con.commit()
            self.con = con
        
    def get(self, k):
        con = self.con
        res = con.execute("select v from kv where k=?", (k,)).fetchall()
        if not res:
            return None
        else:
            return res[0][0]

    def put(self, k, v):
        con=self.con
        con.execute("insert into kv (k, v) values (?, ?) on conflict(k) do update set v = excluded.v", (k, v))
        con.commit()
    

def driver(kv_class, kv_config):
    parallelism = 8
    iterations = 50000
    key_range = 100000
    preload = 20000
    print(f"\n\nMultiple Process ({parallelism}) Testing: " + str(kv_class.__name__) + "\n" + str(kv_config))

    # first preload 
    mem_q = queue.SimpleQueue()
    print("Preloading")
    single_test_simulator(mem_q, kv_class, kv_config, preload, key_range)
    print(process_results(mem_q.get()))

    with mp.Pool(parallelism) as p:
        processes = []
        q = mp.Queue()
        for i in range(parallelism):
            pr = mp.Process(target=single_test_simulator, 
                            args=(q, kv_class, kv_config, iterations, key_range, i))
            pr.start()
            processes.append(pr)

        while processes:
            x = processes.pop()
            x.join()

        while not q.empty():
            res = process_results(q.get())
            print(res)

if __name__ == '__main__':

    # in mem dict per process for baseline
    driver(DummyLocalDictKV, {})

    # single dbm file with locks. This test can run minutes
    driver(ShardedDBMLocked, {'base_path': './data/sharded_l0', "levels": 0} )  # after 1 min killed it

    # dbm sharded into 256 files, nice balance, not too much bloat
    driver(ShardedDBMLocked, {'base_path': './data/sharded_l1', "levels": 1} )

    # dmb sharded into 256 ** 2 files
    # this reates lots of overhead, balooning on disk footprint to 500MB
    driver(ShardedDBMLocked, {'base_path': './data/sharded_l2', "levels": 2} )

    # sqllite with classic locks, keeping connection alive
    driver(SingleSQLite, {'path': './data/sqlite_single/single.sqlite', "use_wal": False} )

    # sqllite with WAL file, better under high contention theoretically
    driver(SingleSQLite, {'path': './data/sqlite_single_wal/single.sqlite', "use_wal": True} )
