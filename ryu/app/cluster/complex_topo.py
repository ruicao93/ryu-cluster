#!/usr/bin/python

"""
    This example create 7 sub-networks to connect 7  domain controllers.
    Each domain network contains at least 5 switches.
    For an easy test, we add 2 hosts per switch.

    So, in our topology, we have at least 35 switches and 70 hosts.
    Hope it will work perfectly.

"""

from mininet.net import Mininet
from mininet.node import Controller, RemoteController, OVSSwitch
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import Link, Intf, TCLink
from mininet.topo import Topo
import logging
import os


def multiControllerNet():
    "Create a network from semi-scratch with multiple controllers."
    controller_conf = [{"name":"ryu1","ip":"127.0.0.1","port":6653},
                       {"name": "ryu2", "ip": "127.0.0.1", "port": 6654}]
    controller_list = []
    switch_list = []
    host_list = []

    inter_bw = 500

    logger = logging.getLogger('ryu.openexchange.test.multi_network')

    net = Mininet(controller=None,
                  switch=OVSSwitch, link=TCLink, autoSetMacs=True)


    for controller in controller_conf:
        name = controller["name"]
        ip = controller["ip"]
        port = controller["port"]
        c = net.addController(name,controller=RemoteController,port=port,ip=ip)
        controller_list.append(c)
        logger.debug("*** Creating %s" % name)

    logger.debug("*** Creating switches")

    switch_list = [net.addSwitch('s%d' % n) for n in xrange(1, 7)]

    logger.debug("*** Creating hosts")
    host_list = [net.addHost('h%d' % n) for n in xrange(1,7)]

    logger.debug("*** Creating links of host2switch.")
    net.addLink(switch_list[0], host_list[0])
    net.addLink(switch_list[1], host_list[1])
    net.addLink(switch_list[2], host_list[2])
    net.addLink(switch_list[3], host_list[3])
    net.addLink(switch_list[4], host_list[4])
    net.addLink(switch_list[5], host_list[5])

    logger.debug("*** Creating intra links of switch2switch.")
    net.addLink(switch_list[0], switch_list[1])
    net.addLink(switch_list[0], switch_list[2])
    net.addLink(switch_list[3], switch_list[4])
    net.addLink(switch_list[3], switch_list[5])
    net.addLink(switch_list[0], switch_list[3])
    net.addLink(switch_list[2], switch_list[5])

    net.build()
    for c in controller_list:
        c.start()

    _No = 0
    switch_list[0].start([controller_list[0]])
    switch_list[1].start([controller_list[0]])
    switch_list[2].start([controller_list[0]])
    switch_list[3].start([controller_list[1]])
    switch_list[4].start([controller_list[1]])
    switch_list[5].start([controller_list[1]])
    logger.info("*** Setting OpenFlow version")

    logger.info("*** Running CLI")
    CLI(net)

    logger.info("*** Stopping network")
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    multiControllerNet()
