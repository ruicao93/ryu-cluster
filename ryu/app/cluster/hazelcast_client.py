import hazelcast, logging
from hazelcast.proxy.base import Proxy, EntryEvent, EntryEventType
from Queue import Queue
import thread
import time
import threading
from ryu.app.cluster.distributed_topo_base import *
LOG = logging.getLogger(__name__)

TEST_MAP = "test-map"
DSWITCH_MAP = "distibuted-dswitch-map"
DPORT_MAP = "distributed-dport-map"
DLINK_MAP = "distributed-dlink-map"
DHOST_MAP = "distributed-dhost-map"
DFLOW_MAP = "distributed-dflow-map"

UPDATE = "update"
REMOVE = "remove"

class HazelcastManager(object):
    def __init__(self):
        super(HazelcastManager, self).__init__()
        self.config = None
        self.HazelCast_Address = "127.0.0.1:5701"
        self.hazelcast_client = None
        self.map_keys = [TEST_MAP,DSWITCH_MAP,DPORT_MAP,DLINK_MAP,DHOST_MAP,DFLOW_MAP]
        #distributed map reference to local hazelcast node
        self.d_maps = {}
        #local map cache
        self.local_maps = {}
        self.queue = Queue()
        self.flow_queue = Queue()
        #self.condition = Condition()
        self.cid = None
        self.dmap_update_thread = thread.start_new_thread(self._dmap_update_thread, ())


    def init_client(self, HazelCast_Address):
        LOG.info("init hazelcast client...")
        self.HazelCast_Address = HazelCast_Address
        self.config = hazelcast.ClientConfig()
        self.config.serialization_config.portable_factories[FACTORY_ID] = {DFlow.CLASS_ID: DFlow}
        self.config.network_config.addresses.append(self.HazelCast_Address)
        self.hazelcast_client = hazelcast.HazelcastClient(self.config)
        LOG.info("init maps...")
        #init maps:d_maps and local_maps
        for key in self.map_keys:
            self._init_map(key)
        #add listener to d_maps:dswitch,dport,dlink,dhost's d_map
        self.d_maps[DSWITCH_MAP].add_entry_listener(include_value=True, added=self._dswitch_map_changed,
                                                    updated=self._dswitch_map_changed, removed=self._dswitch_map_changed)
        self.d_maps[DPORT_MAP].add_entry_listener(include_value=True, added=self._dport_map_changed,
                                                updated=self._dport_map_changed, removed=self._dport_map_changed)
        self.d_maps[DLINK_MAP].add_entry_listener(include_value=True, added=self._dlink_map_changed,
                                                updated=self._dlink_map_changed, removed=self._dlink_map_changed)
        self.d_maps[DHOST_MAP].add_entry_listener(include_value=True, added=self._dhost_map_changed,
                                                updated=self._dhost_map_changed, removed=self._dhost_map_changed)
        self.d_maps[DFLOW_MAP].add_entry_listener(include_value=True, added=self._dflow_map_changed,
                                                updated=self._dflow_map_changed, removed=self._dhost_map_changed)

    def _init_map(self, map_name):
        d_map = self.hazelcast_client.get_map(map_name).blocking()
        self.d_maps[map_name] = d_map
        local_map = {}
        for key in d_map.key_set():
            local_map[key] = d_map.get(key)
        self.local_maps[map_name] = local_map

    #get local map
    def get_map(self, map_name):
        return self.local_maps.get(map_name)

    def get_distributed_map(self,map_name):
        return self.d_maps.get(map_name)

    def update_local_map_value(self, map_name, key, value):
        self.local_maps[map_name][key] = value

    def remove_local_map_value(self, map_name, key):
        type(key)
        print key
        print map_name
        self.local_maps[map_name].pop(key)

    def _dswitch_map_changed(self, event):
        if event.event_type == EntryEventType.added:
            self.update_local_map_value(DSWITCH_MAP, event.key, event.value)
        elif event.event_type == EntryEventType.removed:
            self.remove_local_map_value(DSWITCH_MAP, event.key,)
        elif event.event_type == EntryEventType.updated:
            self.update_local_map_value(DSWITCH_MAP, event.key, event.value)

    def _dport_map_changed(self, event):
        if event.event_type == EntryEventType.added:
            self.update_local_map_value(DPORT_MAP, event.key, event.value)
        elif event.event_type == EntryEventType.removed:
            self.remove_local_map_value(DPORT_MAP, event.key)
        elif event.event_type == EntryEventType.updated:
            self.update_local_map_value(DPORT_MAP, event.key, event.value)

    def _dlink_map_changed(self, event):
        if event.event_type == EntryEventType.added:
            self.update_local_map_value(DLINK_MAP, event.key, event.value)
        elif event.event_type == EntryEventType.removed:
            self.remove_local_map_value(DLINK_MAP, event.key)
        elif event.event_type == EntryEventType.updated:
            self.update_local_map_value(DLINK_MAP, event.key, event.value)

    def _dhost_map_changed(self, event):
        if event.event_type == EntryEventType.added:
            self.update_local_map_value(DHOST_MAP, event.key, event.value)
        elif event.event_type == EntryEventType.removed:
            self.remove_local_map_value(DHOST_MAP, event.key)
        elif event.event_type == EntryEventType.updated:
            self.update_local_map_value(DHOST_MAP, event.key, event.value)

    def _dflow_map_changed(self, event):
        if event.event_type == EntryEventType.added:
            #self.update_local_map_value(DHOST_MAP, event.key, event.value)
            dflow = event.value
            if dflow ==self.cid:
                self.flow_queue.put(dflow)
            pass
        elif event.event_type == EntryEventType.removed:

            pass
            #self.remove_local_map_value(DHOST_MAP, event.key)
        elif event.event_type == EntryEventType.updated:
            pass
            #self.update_local_map_value(DHOST_MAP, event.key, event.value)

    #update map value
    def update_map_value(self, map_name, key, value):
        LOG.info("update data to local...")
        #self.update_local_map_value(map_name, key, value)
        #TODO update distributed map value
        data = ConsumerData(map_name, key, value, UPDATE)
        self.queue.put(data)

    #remove map value
    def remove_map_value(self, map_name, key):
        LOG.info("remove data to local...")
        #self.remove_local_map_value(map_name, key)
        # TODO update distributed map value
        data = ConsumerData(map_name, key, 0, REMOVE)
        self.queue.put(data)



    #thread to synchronize local map update data to hazelcast
    def _dmap_update_thread(self):
        LOG.info("_dmap_update_thread start...............................................")
        while True:
            data = self.queue.get()
            LOG.info("update data to cluster................................................")
            if data.type == UPDATE:
                self.d_maps[data.map_name].put(data.key, data.value)
            elif data.type == REMOVE:
                self.d_maps[data.map_name].remove(data.key)



class ConsumerData(object):
    def __init__(self, map_name, key, value,type=UPDATE):
        self.map_name = map_name
        self.key = key
        self.value = value
        self.type = type



