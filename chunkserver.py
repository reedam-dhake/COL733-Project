from socket_class import SocketClass
from kafka_class import KafkaClient
from mrds import MyRedis
import json, threading

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
        self.data_connection = KafkaClient(host,port+10,f"{chunkgrpid}:{host}:{port+10}")
        self.rds = MyRedis(host, port)
        self.chunkgrpid = chunkgrpid
        self.primary_ip = primary_ip
        self.ip_list = ip_list
        self.version_number = version_number
        self.lease_time = lease_time
        self.buffer = LRUCache(10)
        listen_thread = threading.Thread(target=self.listen)
        listen_thread.start()
    
    # RECVS
    def listen(self):
        recv_req = self.connection.receive()

        recv_req = json.loads(recv_req)
        if recv_req["type"] == "2":
            self.master_update_handler(recv_req)
        elif recv_req["type"] == "5":
            self.write_fwd_handler(recv_req)
        elif recv_req["type"] == "6":
            self.write_fwd_resp_handler(recv_req)
        elif recv_req["type"] == "9":
            self.client_write_handler(recv_req)
        elif recv_req["type"] == "10":
            self.client_read_handler(recv_req)

        return

    '''
    Write request format: 
    {
        "type": 9,
        "sender_ip_port":"localhost:8080",
        "sender_version": 1\,
        "chunk_handle": "100", 
        "req_id": "666666666666666"
    }
    '''
    def client_write_handler(self, req):
        return

    '''
    Read request format: 
    {
        "type": 10,
        "sender_ip_port":"localhost:8080",
        "sender_version": 1,
        "chunk_handle": "100", 
        "byte_range": "8:24",
        "req_id": "666666666666666"
    }
    '''
    def client_read_handler(self, req):
        return

    '''
    Read request format: 
    {
        "type": 5,
        "sender_ip_port":"localhost:8080",
        "sender_version": 1,
        "chunk_handle": "100", 
        "req_id": "666666666666666"
    }
    '''
    def write_fwd_handler(self, req):
        return 

    '''
    Write Fwd Resp request format: 
    {
        "type": 6,
        "sender_ip_port":"localhost:8080",
        "sender_version": 1,
        "chunk_handle": "100", 
        "req_id": "666666666666666"
    }
    '''
    def write_fwd_resp_handler(self, req):
        return 


    '''
    Master Update Format
    {
        "type": 2,
        "sender_ip_port":"localhost:8080",
        "sender_version": 2,
        "req_id": "666666666666666"
        "primary_ip": "100", 
        "lease_time": "5" (None),
        "version_number": 2,
        "ip_list" : [...],
    }
    '''
    def master_update_handler(self, req):
        return
