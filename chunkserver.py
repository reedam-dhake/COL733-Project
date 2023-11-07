from socket_class import SocketClass
from kafka_class import KafkaClient
from mrds import MyRedis
import json

class LRUCache:
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.cache = {}
        self.queue = []
        self.size = 0

    def get(self, key: str) -> str:
        if key not in self.cache:
            return None
        self.queue.remove(key)
        self.queue.append(key)
        return self.cache[key]

    def put(self, key: str, value: str) -> None:
        if key in self.cache:
            self.queue.remove(key)
        elif self.size == self.capacity:
            del self.cache[self.queue.pop(0)]
            self.size -= 1
        self.cache[key] = value
        self.queue.append(key)
        self.size += 1

class ChunkServer(object):
    def __init__(self,host,port,chunkgrpid, primary_ip, ip_list, version_number, lease_time):
        self.host = host
        self.port = port
        self.connection = KafkaClient(host,port,f"{chunkgrpid}:{host}:{port}")
        self.rds = MyRedis(host, port)
        self.chunkgrpid = chunkgrpid
        self.primary_ip = primary_ip
        self.ip_list = ip_list
        self.version_number = version_number
        self.lease_time = lease_time
        self.buffer = LRUCache(10)
        return
    
    # RECVS
    def listen(self):
        recv_req = self.connection.receive()
        # Example json request
        # {
        #     "type": "1",
        #     "sender_ip_port":"localhost:8080",
        #     "sender_version": "1.0",
        #     "request": "data",
        # }
        # Type can be 2,5,6,9,10
        recv_req = json.loads(recv_req)
        if recv_req["type"] == "4":
            self.listen_heartbeat(recv_req)
        elif recv_req["type"] == "5":
            self.listen_client(recv_req)
        elif recv_req["type"] == "6":
            self.listen_master(recv_req)
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