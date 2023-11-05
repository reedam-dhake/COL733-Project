import socket
import threading
import os
import math
import sys
import pickle
from socket_class import SocketClass

class ChunkServer(object):
    def __init__(self):
        return
    
    # RECVS
    def listen(self):
        return

    def listen_client(self):
        return
    
    def listen_master(self):
        return
 
    def listen_chunkservers(self):
        return
    
    # SENDS
    
    def send_heartbeat(self):
        # chunks holded by it to master periodically 
        # Request for extension of lease period corresponding to a chunk if operation is going on 
        return
    
    def send_client(self):
        return
    
    def send_chunkservers(self):
        return
    
    # GFS functions

    def read(self):
        return
    
    def append(self):
        return