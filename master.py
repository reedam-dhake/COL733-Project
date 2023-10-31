import socket
import threading
import os
import math
import sys
import pickle
from typing import Optional
from constants import Chunk,File

class MasterServer(object):
    def __init__(self):
        # file and chunk namespace 
        # mapping from files to chunks
        # location of each chunk replica
        # on lease chunkserver corresponding to each chunk (timeout 60s)
        self.files: Optional[File] = []
        return
    
    # RECVS
    def listen(self):
        return

    def listen_client(self):
        # recieve request for lease and location of replica(s) for a chunk
        # recieve filename and chunk number
        return
    
    def listen_heartbeat(self):
        return
    
    # SENDS
    def send_client(self):
        # identity of primary and location of all replica(s) for a requested chunk
        # chunk handle 
        return
    
    def send_chunkservers(self):
        return
    
    # MASTER FUNCTIONS

    def alloc_chunks(self):
        return