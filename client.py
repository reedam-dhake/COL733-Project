from socket_class import SocketClass
from kafka_class import KafkaClient
from mrds import MyRedis
from constants import *
import json, threading, uuid, time

class Client(object):
	def __init__(self,host,port,master_ip):
		self.host = host
		self.port = port
		self.master_ip = master_ip
		self.cache : dict[tuple(str,str),tuple(str,list(str),str,int,int)] = {}
		self.connection = KafkaClient(host,port,f"Client:{host}:{port}")
		self.pending_responses : dict[str, int] = {}
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
		del self.pending_responses[req["req_id"]]
		self.cache[(req["filename"],req["chunk_number"])] = (req["chunk_handle"],req["ip_list"],req["primary_ip"],req["version_number"],req["lease_time"])
		return

 
	'''
	Write response format: 
	{
		"type": 3,
		"sender_ip_port":"localhost:8080",
		"req_id": "666666666666666",
		"status": 1
		"message": "Success"
		"offset": 1000
	} 
 
	'''
	def write_response_handler(self, req):
		del self.pending_responses[req["req_id"]]
		if req["status"] == 1:
			return f"Sccessful write at offset: {req['offset']}"
		else:
			return f"Unsuccessful write with message: {req['message']}"


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
		del self.pending_responses[req["req_id"]]
		if req["status"] == 1:
			return f"Sccessful read with data: {req['data']}"
		else:
			return f"Unsuccessful read with message: {req['message']}"
		return

	def metadata_request(self, filename, chunk_number):
		unique_req_id = str(uuid.uuid4())
		req = {
			"type": 7,
			"sender_ip_port": f"{self.host}:{self.port}",
			"req_id": unique_req_id,
			"filename": filename,
			"chunk_number": chunk_number
		}
		self.pending_responses[unique_req_id] = time.time()
		self.connection.send(json.dumps(req),f"Master:{self.master_ip}")

	def write_request(self, filename, chunk_number, data):
		if (filename,chunk_number) not in self.cache or self.cache[(filename,chunk_number)][4] < time.time():
			self.metadata_request(filename,chunk_number)
		while (filename,chunk_number) not in self.cache:
			continue
		
		unique_req_id = str(uuid.uuid4())
		data_tuple = self.cache[(filename,chunk_number)] 
		req = {
			"type": 8,
			"sender_ip_port": f"{self.host}:{self.port}",
			"sender_version": data_tuple[3],
			"chunk_handle": data_tuple[0],
			"req_id": unique_req_id,
		}
		self.pending_responses[unique_req_id] = time.time()
		self.connection.send(json.dumps(req),f"ChunkServer:{data_tuple[2]}")

	def read_request(self, filename, chunk_number, byte_range):
		if (filename,chunk_number) not in self.cache or self.cache[(filename,chunk_number)][4] < time.time():
			self.metadata_request(filename,chunk_number)
		while (filename,chunk_number) not in self.cache:
			continue
		
		unique_req_id = str(uuid.uuid4())
		data_tuple = self.cache[(filename,chunk_number)] 
		req = {
			"type": 9,
			"sender_ip_port": f"{self.host}:{self.port}",
			"sender_version": data_tuple[3],
			"req_id": unique_req_id,
			"chunk_handle": data_tuple[0],
			"byte_range": byte_range
		}
		self.pending_responses[unique_req_id] = time.time()
		self.connection.send(json.dumps(req),f"ChunkServer:{data_tuple[2]}")
