from socket_class import TCPSocketClass
from kafka_class import KafkaClient
from mrds import MyRedis
import json, threading, ping3
from constants import *

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
    def __init__(self,host,port,redis_port,chunkgrpid, primary_ip, ip_list, version_number, lease_time, master_ip):
        self.host = host
        self.port = port
        self.connection = KafkaClient(host,port,f"ChunkServer:{host}:{port}")
        self.data_connection = KafkaClient(host,port+10,f"ChunkServer:{host}:{port+10}")
        self.rds = MyRedis(host, redis_port)
        self.chunkgrpid = chunkgrpid
        self.primary_ip = primary_ip
        self.ip_list = ip_list
        self.version_number = version_number
        self.lease_time = lease_time
        self.buffer = LRUCache(10)
        self.pending_wrt_fwd : dict[str,int] = {}
        listen_thread = threading.Thread(target=self.listen)
        listen_thread.start()
        # For SWIM
        self.master_ip = master_ip
        # SWIM SOCKET NEEDED 
        self.swim_sock = TCPSocketClass(self.swim_port,self.host)
        # self.pending_swim = {}

    
    # RECVS
    def listen(self):
        while(True):
            recv_req = self.connection.receive()
            if recv_req == None:
                continue
            recv_req = json.loads(recv_req)
            if recv_req["type"] == 2:
                self.master_update_handler(recv_req)
            elif recv_req["type"] == 5:
                self.write_fwd_handler(recv_req)
            elif recv_req["type"] == 6:
                self.write_fwd_resp_handler(recv_req)
            elif recv_req["type"] == 8:
                self.client_write_handler(recv_req)
            elif recv_req["type"] == 9:
                self.client_read_handler(recv_req)
            elif recv_req["type"] == 10:
                # TODOLIST: Create separate thread for this 
                self.swim_start(recv_req)
            elif recv_req["type"] == 11: 
                self.swim_handler(recv_req)
            elif recv_req["type"] == 12:
                self.swim_resp_handler(recv_req)
            else:
                print("FATAL ERROR WORLD WILL END SOON")

    '''
    Write request format: 
    {
        "type": 8,
        "sender_ip_port":"localhost:8080",
        "sender_version": 1,
        "chunk_handle": "100", 
        "req_id": "666666666666666"
    }
    '''
    def client_write_handler(self, req):
        if req["sender_version"] < self.version_number:
            self.connection.send(
                json.dumps(
                    {
                        "type": 3,
                        "sender_ip_port": req["sender_ip_port"],
                        "req_id": req["req_id"],
                        "status": 1,
                        "message": "Stale Version",
                        "offset": 0,
                        "chunk_handle": req["chunk_handle"]
                    }
                ),
                f"Client:{req['sender_ip_port']}"
            )
        elif req["sender_version"] > self.version_number:
            self.connection.send(
                json.dumps(
                    {
                        "type": 3,
                        "sender_ip_port": req["sender_ip_port"],
                        "req_id": req["req_id"],
                        "status": 2,
                        "message": "Future Version",
                        "offset": 0,
                        "chunk_handle": req["chunk_handle"]
                    }
                ),
                f"Client:{req['sender_ip_port']}"
            )
        else:
            if self.primary_ip != f"{self.host}:{self.port}":
                self.connection.send(json.dumps(req), f"ChunkServer:{self.primary_ip}")
                print("Forwarding write request to primary")
            else:
                self.pending_wrt_fwd[req["req_id"]] = 3
                req2 = req.copy()
                req2["type"] = 5
                data = self.buffer.get(req["req_id"])
                if data == None:
                    self.connection.send(
                        json.dumps(
                            {
                                "type": 3,
                                "sender_ip_port": req["sender_ip_port"],
                                "req_id": req["req_id"],
                                "status": 3,
                                "message": "Data Not Recieved",
                                "offset": 0,
                                "chunk_handle": req["chunk_handle"]
                            }
                        ),
                        f"Client:{req['sender_ip_port']}"
                    )
                    return
                new_offset = self.rds.record_append(req["chunk_handle"], data, None)
                start_offset = new_offset - len(data)
                req2["primary_offset"] = start_offset
                for ip in self.ip_list:
                    if ip != f"{self.host}:{self.port}":
                        self.connection.send(json.dumps(req2), f"ChunkServer:{ip}")
                self.pending_wrt_fwd[req["req_id"]] = 2
                
        return

    '''
    Read request format: 
    {
        "type": 9,
        "sender_ip_port":"localhost:8080",
        "sender_version": 1,
        "chunk_handle": "100", 
        "byte_range": "8:24",
        "req_id": "666666666666666"
    }
    '''
    def client_read_handler(self, req):
        if req["sender_version"] != self.version_number:
            self.connection.send(
                json.dumps(
                    {
                        "type": 4,
                        "sender_ip_port": req["sender_ip_port"],
                        "req_id": req["req_id"],
                        "status": 1,
                        "message": "Version Missmatch",
                        "data": "",
                    }
                ),
                f"Client:{req['sender_ip_port']}"
            )
        else:
            data = self.rds.read_record(req["chunk_handle"], req["byte_range"])
            self.connection.send(
                json.dumps(
                    {
                        "type": 4,
                        "sender_ip_port": req["sender_ip_port"],
                        "req_id": req["req_id"],
                        "status": 0,
                        "message": "Success",
                        "data": data,
                    }
                ),
                f"Client:{req['sender_ip_port']}"
            )
        return

    '''
    Write fwd request format: 
    {
        "type": 5,
        "sender_ip_port":"localhost:8080",
        "sender_version": 1,
        "chunk_handle": "100", 
        "req_id": "666666666666666"
        "primary_offset": 0,
    }
    '''
    def write_fwd_handler(self, req):
        if req["sender_version"] != self.version_number:
            return
        data = self.buffer.get(req["req_id"])
        req2 = req.copy()
        req2["type"] = 6
        if data == None:
            req2["status"] = 1
            req2["message"] = "Data Not Recieved"
            self.connection.send(json.dumps(req2), f"ChunkServer:{self.primary_ip}")
        else:
            req2["status"] = 0
            req2["message"] = "Success"
            self.rds.record_append(req["chunk_handle"], data, req["primary_offset"])
            self.connection.send(json.dumps(req2), f"ChunkServer:{self.primary_ip}")
        return
        
    '''
    Write Fwd Resp request format: 
    {
        "type": 6,
        "sender_ip_port":"localhost:8080",
        "sender_version": 1,
        "chunk_handle": "100", 
        "req_id": "666666666666666",
        "status": 0,
        "message": "Success",
        "primary_offset": 0,
    }
    '''
    def write_fwd_resp_handler(self, req):
        if req["sender_version"] != self.version_number:
            return
        if req["status"] == 0:
            if req["req_id"] not in self.pending_wrt_fwd:
                return
            self.pending_wrt_fwd[req["req_id"]] -= 1
            if self.pending_wrt_fwd[req["req_id"]] == 0:
                self.connection.send(
                    json.dumps(
                        {
                            "type": 3,
                            "sender_ip_port": req["sender_ip_port"],
                            "req_id": req["req_id"],
                            "status": 0,
                            "message": "Success",
                            "offset": req["primary_offset"],
                            "chunk_handle": req["chunk_handle"]
                        }
                    ),
                    f"Client:{req['sender_ip_port']}"
                )
        else:
            self.connection.send(
                json.dumps(
                    {
                        "type": 3,
                        "sender_ip_port": req["sender_ip_port"],
                        "req_id": req["req_id"],
                        "status": 3,
                        "message": "Data Not Recieved",
                        "offset": 0,
                        "chunk_handle": req["chunk_handle"]
                    }
                ),
                f"Client:{req['sender_ip_port']}"
            )
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
        self.primary_ip = req["primary_ip"]
        self.ip_list = req["ip_list"]
        self.version_number = req["version_number"]
        self.lease_time = req["lease_time"]
        return

    '''
    Start swim request format:
        {
        "type": "10",
        "sender_ip_port":"localhost:8080",
        "sender_version": 1,
		"suspicious_ip": "localhost:2020"
        }
    '''
    def swim_start(self, req):
        # Start SWIM by sending a SWIM ping request to suspicious_ip
        sus_ip = req["suspicious_ip"].split(":")
        self.sock.send(json.dumps({"type": 11, "sender_ip_port": f"{self.host}:{self.port}"}), 
            sus_ip[1], sus_ip[0])
        return
    
    '''
    Swim request format:
        {
        "type": "11",
        "sender_ip_port":"localhost:8080",
        }
    '''
    def swim_handler(self, req):
        ip = req["sender_ip_port"].split(":")
        self.sock.send(
            json.dumps(
                {
                    "type": 12,
                    "sender_ip_port": f"{self.host}:{self.port}",
                }
            ), ip[1], ip[0])
        return

    '''
    Swim response format:
        {
        "type": "12",
        "sender_ip_port":"localhost:8080",
        }
    '''
    def swim_resp_handler(self, req):
        # Send a response to master that we have got another path to the chunkserver
        # We have received from sus IP
        sus_ip = req["sender_ip_port"]
        self.sock.send(
            json.dumps(
                {
                    "type": 13,
                    "sender_ip_port": f"{self.host}:{self.port}",
                    "sus_ip": f"{sus_ip[0]}:{sus_ip[1]}"
                }
            ), self.master_ip[1], self.master_ip[0])
        return