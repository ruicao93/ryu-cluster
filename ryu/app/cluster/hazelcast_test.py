import hazelcast_client
import logging
import time
import thread
import random
from distributed_topo_base import *

def main():
    # basic logging setup to see client logs
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    hazelcastManager = hazelcast_client.HazelcastManager()
    hazelcastManager.init_client("127.0.0.1:5701")
    map = hazelcastManager.get_distributed_map("test-map")
    dflow = DFlow(1,1,1,1,1,"123",None,"flow-mod",None)
    #map.put(1,dflow)
    df = map.get(1)
    print df.data

def main2():
    # basic logging setup to see client logs
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    hazelcastManager = hazelcast_client.HazelcastManager()
    hazelcastManager.init_client()
    thread.start_new_thread(update_map, (hazelcastManager,"test-map"))
    while (True):
        for key in hazelcastManager.map_keys:
            show_map(hazelcastManager, key)
        time.sleep(5)
        print "-------------------"

def show_map(hazelcastManager, map_name):
    test_map = hazelcastManager.get_map(map_name)
    #print type(test_map)
    for k in test_map:
        print "key:%d  --- value: %s" % (k, test_map.get(k))

def update_map(hazelcastManager, map_name):
    i = 0;
    while True:
        hazelcastManager.update_dmap(map_name, 1, i)
        i += 1
        print "i:%d" % i
        time.sleep(random.randint(0,5))

if __name__ == "__main__":
    main()