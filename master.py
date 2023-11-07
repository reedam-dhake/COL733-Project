from socket_class import SocketClass
from kafka_class import KafkaClient
from mrds import MyRedis
from constants import *
import json, threading, ping3

class Master(object):
	def __init__(self,host,port,redis_port,available_ips):
		self.host = host
		self.port = port
		self.files : dict[tuple(str,str), tuple(str,str)] = {}
		self.chunkgrp_map : dict[str, tuple(list(str), str, int, int)] = {}
		self.connection = KafkaClient(host,port,f"Master:{host}:{port}")
		self.rds = MyRedis(host, redis_port)
		self.available_ips = available_ips
		listen_thread = threading.Thread(target=self.listen)
		listen_thread.start()
		heartbeat_thread = threading.Thread(target=self.heartbeat)
		heartbeat_thread.start()
		
	def listen(self):
		recv_req = self.connection.receive()

		recv_req = json.loads(recv_req)
		if recv_req["type"] == 7:
			self.metadata_request_handler(recv_req)
		else:
			print("FATAL ERROR IN MASTER WORLD WILL END SOON")
		return

	'''
    Metadata request format: 
    {
        "type": 7,
        "sender_ip_port":"localhost:8080",
        "req_id": "666666666666666",
		"filename": "file.txt",
		"chunk_number": 53
    }
    '''
	def metadata_request_handler(self, req):
		return
	
	'''
	Iterate over chunk group and in each chunk group over the CS 
	Send ping to all
	If some server misses 3 pings:
	-> Make new chunkserver
	-> If primary: choose new primary, update version numbers, send update
	-> Else send update (list of ips in chunk group)
	'''
	def heartbeat(self):
		while(True):
			# Example
			ping3.ping('google.com', timeout=100)
