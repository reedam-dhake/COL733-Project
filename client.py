from socket_class import TCPSocketClass
from kafka_class import KafkaClient
from mrds import MyRedis
from constants import *
import json, threading, uuid, time

class Client(object):
	def __init__(self,host,port,master_ip, master_port):
		self.host = host
		self.port = port
		self.master_ip = master_ip
		self.master_port = master_port
		self.cache : dict[tuple(str,str),tuple(str,list(tuple(str,int)),tuple(str,int),int,int)] = {}
		self.rds = MyRedis(host, self.port+500)
		self.tcp_socket = TCPSocketClass(self.port,self.host)
		self.pending_responses : dict[str, int] = {}
		listen_thread = threading.Thread(target=self.listen)
		listen_thread.start()
	
	def listen(self):
		while(True):
			recv_req = self.tcp_socket.receive()
			if recv_req == None or len(recv_req) == 0:
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
        "sender_ip": "localhost",
        "sender_port": 8080,
        "req_id": "666666666666666",
        "filename": "file.txt",
        "chunk_number": 53,
		"chunk_handle": "100",
		"version_number": 1,
		"primary_ip": ("localhost",8000)
		"ip_list": [("localhost",8080),("localhost",8081),("localhost",8082)]
		"lease_time": 100
    }
    '''
	def metadata_handler(self, req):
		del self.pending_responses[req["req_id"]]
		self.cache[(req["filename"],req["chunk_number"])] = (req["chunk_handle"],req["ip_list"],req["primary_ip"],req["version_number"],req["lease_time"])

 
	'''
	Write response format: 
	{
		"type": 3,
		"sender_ip":"localhost",
		"sender_port":8080,
		"req_id": "666666666666666",
		"status": 0
		"message": "Success"
		"offset": 1000
		"chunk_handle": 100
	} 
 
	'''
	def write_response_handler(self, req):
		del self.pending_responses[req["req_id"]]
		if req["status"] == 0:
			return f"Sccessful write at offset: {req['offset']}"
		elif req["status"] == 1 or req["status"] == 2:
			filename,chunk_num = req["chunk_handle"].split("::")
			chunk_num = int(chunk_num)
			self.metadata_request(filename,chunk_num)
			return f"Write failed due to version mismatch. Error: {req['message']}"
		elif req["status"] == 3:
			return f"Write failed due to data not recieved. Error: {req['message']}"
		else:
			return f"Unsuccessful write with message: {req['message']}"


	'''
 	Read response format:
	{
		"type": 4,
		"sender_ip":"localhost",
		"sender_port":8080,
		"req_id": "666666666666666",
		"status": 0
		"message": "Success"
		"data": "Hello World"
	} 
  	'''
	def read_response_handler(self, req):
		del self.pending_responses[req["req_id"]]
		if req["status"] == 0:
			return f"Sccessful read with data: {req['data']}"
		else:
			return f"Unsuccessful read with message: {req['message']}"

	def metadata_request(self, filename, chunk_number):
		unique_req_id = str(uuid.uuid4())
		req = {
			"type": 7,
			"sender_ip":self.host,
			"sender_port":self.port,
			"req_id": unique_req_id,
			"filename": filename,
			"chunk_number": chunk_number
		}
		self.pending_responses[unique_req_id] = time.time()
		self.tcp_socket.send(json.dumps(req),self.master_ip,self.master_port)

	def write_request(self, filename, chunk_number, data_req_id):
		if (filename,chunk_number) not in self.cache:
			self.metadata_request(filename,chunk_number)
		elif self.cache[(filename,chunk_number)][4] < time.time():
			del self.cache[(filename,chunk_number)]
			self.metadata_request(filename,chunk_number)
   
		while (filename,chunk_number) not in self.cache:
			continue
		
		unique_req_id = str(uuid.uuid4())
		data_tuple = self.cache[(filename,chunk_number)] 
		req = {
			"type": 8,
			"sender_ip":self.host,
			"sender_port":self.port,
			"sender_version": data_tuple[3],
			"chunk_handle": data_tuple[0],
			"req_id": unique_req_id,
			"data_req_id": data_req_id
		}
		# Send Data to buffer here
		self.pending_responses[unique_req_id] = time.time()
		self.tcp_socket.send(json.dumps(req),data_tuple[2][0],data_tuple[2][1])

	def read_request(self, filename, chunk_number, byte_range):
		if (filename,chunk_number) not in self.cache:
			self.metadata_request(filename,chunk_number)
		elif self.cache[(filename,chunk_number)][4] < time.time():
			del self.cache[(filename,chunk_number)]
			self.metadata_request(filename,chunk_number)
   
		while (filename,chunk_number) not in self.cache:
			continue
		
		unique_req_id = str(uuid.uuid4())
		data_tuple = self.cache[(filename,chunk_number)] 
		req = {
			"type": 9,
			"sender_ip":self.host,
			"sender_port":self.port,
			"sender_version": data_tuple[3],
			"req_id": unique_req_id,
			"chunk_handle": data_tuple[0],
			"byte_range": byte_range
		}
		self.pending_responses[unique_req_id] = time.time()
		self.tcp_socket.send(json.dumps(req),data_tuple[2][0],data_tuple[2][1])
  

	def data_forward(self, filename, chunk_number, start_offset):
		# forward data to chunkserver by writing directly 
		if (filename,chunk_number) not in self.cache:
			self.metadata_request(filename,chunk_number)
		elif self.cache[(filename,chunk_number)][4] < time.time():
			del self.cache[(filename,chunk_number)]
			self.metadata_request(filename,chunk_number)
   
		while (filename,chunk_number) not in self.cache:
			continue

		ip_list = self.cache[(filename,chunk_number)][1]
		data_req_id = str(uuid.uuid4())
		lua_script = """
			local start_offset = tonumber(ARGV[1])
			local data_read_size = tonumber(ARGV[2])
			local send_ip = ARGV[3]
			local send_port = tonumber(ARGV[4])
			local new_key = ARGV[5]
			local filename = ARGV[6]
			local data = redis.call('getrange',filename,start_offset,start_offset+data_read_size-1)
			redis.call('set',new_key,data)
			redis.call('migrate',send_ip,send_port,keys={new_key},destination_db=0,copy=True,replace=True,timeout=1000000,auth='')
			redis.call('del',new_key)
		"""
		for ip in ip_list:
			self.rds.rds.eval(lua_script, 0, start_offset, DATA_SIZE, ip[0], ip[1]+500, data_req_id, filename)
		return data_req_id

	def write_data(self, filename):
		total_bytes = self.rds.strlen(filename)
		chunk_number = 0
		curr_offset = 0
		while curr_offset < total_bytes:
			data_req_id = self.data_forward(filename, chunk_number, curr_offset)
			self.write_request(filename, chunk_number, data_req_id)
			curr_offset += DATA_SIZE
			chunk_number = curr_offset // CHUNK_SIZE
		return