import redis
import signal
import time
import os
import threading
from threading import Thread
import multiprocessing
from multiprocessing import Process
import subprocess
from subprocess import Popen
from time import sleep

kvolve_loc = "../../redis-2.8.17/src/"
amico_12_loc = "amico_12.rb"
amico_20_loc = "amico_20.rb"
upd_code = os.path.abspath("amico_v12v20.so")
trials = 1
runtime = 20 
beforeupd = 10 

def popen(args):
  print "$ %s" % args
  return Popen(args.split(" "))

def do_stats(r, run):
  name = "amico_upd_stats"+str(run)+".txt"
  f = open(name, 'a')
  f.write("\n")
  i = 0
  while i<runtime:
    queries = r.info()["instantaneous_ops_per_sec"]
    f.write(str(queries-1) + "\n")
    time.sleep(1)
    i = i + 1 
    if (i%20 == 0):
      f.flush()


def kv():
  print("______________KVOLVE_____________")
  f2 = open('amico_upd_count.txt', 'a')
  for i in range (trials):
    print "KVOLVE " + str(i)
    redis_server = popen(kvolve_loc +"redis-server " + kvolve_loc +"../redis.conf")
    sleep(1)
    r = redis.StrictRedis()
    r.client_setname("amico:followers@12,amico:following@12,amico:blocked@12,amico:reciprocated@12,amico:pending@12")
    amico12 = subprocess.Popen(["ruby", amico_12_loc, "kvolve"])
    sleep(1)
    # This thread prints the "queries per second"
    stats = Thread(target=do_stats, args=(r,i))
    stats.start()
    print "Waiting for " + str(beforeupd) + " seconds"
    sleep(beforeupd)
    allkeys = r.keys('*')
    orig = len(allkeys)
    f2.write(str(orig) + ",")
    print "UPDATING, have " + str(orig) + " Keys to update" 
    r.client_setname("update/"+upd_code)
    amico12.terminate() # the connection will be automatically killed, this just kills the process
    amico20 = subprocess.Popen(["ruby", amico_20_loc, "kvolve"])
    print "Running post-update for " + str(runtime - beforeupd) + " seconds"
    sleep(runtime - beforeupd)
    amico20.terminate()
    stats.join()
    ke = r.keys('kvolve')
    notupd = 0
    for k in ke:
      t = r.object("idletime", k)
      if t > beforeupd:
         notupd = notupd + 1
    print "UPDATED: " + str(orig -notupd)
    f2.write(str(orig -notupd) + "\n")
    print r.info()
    redis_server.terminate() 
    sleep(1)
    f2.flush()
  f2.close()


def main():
  if not os.path.exists("WikiTalk.txt"):
    print ("Please first download test data: https://snap.stanford.edu/data/wiki-Talk.html")
    sys.exit() 
  if os.path.exists("dump.rdb"):
    subprocess.call(["rm", "dump.rdb"])
  kv()


if __name__ == '__main__':
  main()
