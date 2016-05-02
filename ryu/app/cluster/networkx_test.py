import hazelcast_client
import logging
import time
import thread
import random
import networkx as nx

def main():
    graph = nx.DiGraph()
    list = [(0,1),(1,2),(2,3)]
    graph.add_edges_from(list)
    path = nx.dijkstra_path(graph,3,0)
    print type(path)
    print type(path[0])
    print path

    pass

if __name__ == "__main__":
    main()