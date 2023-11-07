from socket_class import SocketClass
from kafka_class import KafkaClient
from mrds import MyRedis
from constants import *
import json, threading, ping3

class Client(object):
	def __init__(self,host,port,master_ip):
		self.host = host
		self.port = port
		self.master_ip = master_ip
		self.cache : dict[tuple(str,str),tuple(str,list(str),str,int,int)] = {}
		self.connection = KafkaClient(host,port,f"Client:{host}:{port}")
		listen_thread = threading.Thread(target=self.listen)
		listen_thread.start()
	
	def listen(self):
		while(True):
			recv_req = self.connection.receive()
			if recv_req == None:
				continue
			recv_req = json.loads(recv_req)
			if recv_req["type"] == 1:
				self.metadata_handler(recv_req)
			elif recv_req["type"] == 3:
				self.write_response_handler(recv_req)
			elif recv_req["type"] == 4:
				self.read_response_handler(recv_req)
			else:
				print("FATAL ERROR IN CLIENT WORLD WILL END SOON")
    
	'''
    Metadata response format: 
    {
        "type": 1,
        "sender_ip_port":"localhost:8080",
        "req_id": "666666666666666",
        "filename": "file.txt",
        "chunk_number": 53,
		"chunk_handle": "100",
		"version_number": 1,
		"primary_ip": "localhost:8080",
		"ip_list": ["localhost:8080","localhost:8081","localhost:8082"]
		"lease_time": 100
    }
    '''
	def metadata_handler(self, req):
		return

 
	'''
	Write response format: 
	{
		"type": 3,
		"sender_ip_port":"localhost:8080",
		"req_id": "666666666666666",
		"status": 1
		"message": "Success"
	} 
 
	'''
	def write_response_handler(self, req):
		return



	'''
 	Read response format:
	{
		"type": 4,
		"sender_ip_port":"localhost:8080",
		"req_id": "666666666666666",
		"status": 1
		"message": "Success"
		"data": "Hello World"
	} 
  	'''
	def read_response_handler(self, req):
		return