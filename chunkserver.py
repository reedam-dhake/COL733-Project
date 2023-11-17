from socket_class import TCPSocketClass
from mrds import MyRedis
import json, threading
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
	def __init__(self,host,port,chunkgrpid, primary_ip_port, ip_list, version_number, lease_time, master_ip):
		self.host = host
		self.port = port
		self.master_ip = master_ip
		
		self.tcp_socket = TCPSocketClass(self.port,self.host)
		self.heartbeat_socket = TCPSocketClass(self.port+1000,self.host)
		
		self.rds = MyRedis(host, self.port+500)
		
		self.chunkgrpid = chunkgrpid
		self.primary_ip_port = primary_ip_port
		self.ip_list = ip_list
		self.version_number = version_number
		self.lease_time = lease_time
		
		self.buffer = LRUCache(10)
		self.pending_wrt_fwd : dict[str,int] = {}
		
		listen_thread = threading.Thread(target=self.listen)
		listen_thread.start()
		heartbeat_thread = threading.Thread(target=self.heartbeat)
		heartbeat_thread.start()

	
	# RECVS
	def listen(self):
		while(True):
			recv_req = self.tcp_socket.receive()
			if recv_req == None or len(recv_req) == 0:
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
			else:
				print("FATAL ERROR WORLD WILL END SOON")
				
	'''
	Read request format: 
	{
		"type": 9,
		"sender_ip":"localhost",
		"sender_port":8080,
		"sender_version": 1,
		"chunk_handle": "100", 
		"byte_range": "8:24",
		"req_id": "666666666666666"
	}
	'''
	def client_read_handler(self, req):
		if req["sender_version"] != self.version_number:
			self.tcp_socket.send(
				json.dumps(
					{
						"type": 4,
						"sender_ip": req["sender_ip"],
						"sender_port": req["sender_port"],
						"req_id": req["req_id"],
						"status": 1,
						"message": "Version Missmatch",
						"data": "",
					}
				),
				req["sender_ip"],req["sender_port"]
			)
		else:
			data = self.rds.read_record(req["chunk_handle"], req["byte_range"])
			self.tcp_socket.send(
				json.dumps(
					{
						"type": 4,
						"sender_ip": req["sender_ip"],
						"sender_port": req["sender_port"],
						"req_id": req["req_id"],
						"status": 0,
						"message": "Success",
						"data": data,
					}
				),
				req["sender_ip"],req["sender_port"]
			)
		return
	
 
	'''
	Write request format: 
	{
		"type": 8,
		"sender_ip":"localhost",
		"sender_port":8080,
		"sender_version": 1,
		"chunk_handle": "100", 
		"req_id": "666666666666666",
		"data_req_id": "666666666666666"
	}
	'''
	def client_write_handler(self, req):
		if req["sender_version"] < self.version_number:
			self.tcp_socket.send(
				json.dumps(
					{
						"type": 3,
						"sender_ip": req["sender_ip"],
						"sender_port": req["sender_port"],
						"req_id": req["req_id"],
						"status": 1,
						"message": "Stale Version",
						"offset": 0,
						"chunk_handle": req["chunk_handle"]
					}
				),
				req["sender_ip"],req["sender_port"]
			)
		elif req["sender_version"] > self.version_number:
			self.tcp_socket.send(
				json.dumps(
					{
						"type": 3,
						"sender_ip": req["sender_ip"],
						"sender_port": req["sender_port"],
						"req_id": req["req_id"],
						"status": 2,
						"message": "Future Version",
						"offset": 0,
						"chunk_handle": req["chunk_handle"]
					}
				),
				req["sender_ip"],req["sender_port"]
			)
		else:
			if self.primary_ip_port != (self.host,self.port):
				self.tcp_socket.send(json.dumps(req), self.primary_ip_port[0], self.primary_ip_port[1])
				print("Forwarding write request to primary")
			else:
				self.pending_wrt_fwd[req["req_id"]] = 3
				req2 = req.copy()
				req2["type"] = 5
				data = self.rds.get(req["data_req_id"])
				if data == None:
					self.tcp_socket.send(
						json.dumps(
							{
								"type": 3,
								"sender_ip": req["sender_ip"],
								"sender_port": req["sender_port"],
								"req_id": req["req_id"],
								"status": 3,
								"message": "Data Not Recieved",
								"offset": 0,
								"chunk_handle": req["chunk_handle"]
							}
						),
						req["sender_ip"],req["sender_port"]
					)
					return
				new_offset = self.rds.record_append(req["chunk_handle"], data, None)
				start_offset = new_offset - len(data)
				req2["primary_offset"] = start_offset
				for ip in self.ip_list:
					if ip != (self.host,self.port):
						self.tcp_socket.send(json.dumps(req2), ip[0], ip[1])
				self.pending_wrt_fwd[req["req_id"]] = 2
				
		return


	'''
	Write fwd request format: 
	{
		"type": 5,
		"sender_ip":"localhost",
		"sender_port":8080,
		"sender_version": 1,
		"chunk_handle": "100", 
		"req_id": "666666666666666"
		"data_req_id": "666666666666666"
		"primary_offset": 0,
	}
	'''
	def write_fwd_handler(self, req):
		if req["sender_version"] != self.version_number:
			return
		data = self.rds.get(req["data_req_id"])
		req2 = req.copy()
		req2["type"] = 6
		if data == None:
			req2["status"] = 1
			req2["message"] = "Data Not Recieved"
			self.tcp_socket.send(json.dumps(req2), self.primary_ip_port[0], self.primary_ip_port[1])
		else:
			req2["status"] = 0
			req2["message"] = "Success"
			self.rds.record_append(req["chunk_handle"], data, req["primary_offset"])
			self.tcp_socket.send(json.dumps(req2), self.primary_ip_port[0], self.primary_ip_port[1])
		return
		
	'''
	Write Fwd Resp request format: 
	{
		"type": 6,
		"sender_ip":"localhost",
		"sender_port":8080,
		"sender_version": 1,
		"chunk_handle": "100", 
		"req_id": "666666666666666",
		"data_req_id": "666666666666666",
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
				self.tcp_socket.send(
					json.dumps(
						{
							"type": 3,
							"sender_ip": req["sender_ip"],
							"sender_port": req["sender_port"],
							"req_id": req["req_id"],
							"status": 0,
							"message": "Success",
							"offset": req["primary_offset"],
							"chunk_handle": req["chunk_handle"]
						}
					),
					req["sender_ip"],req["sender_port"]
				)
		else:
			self.tcp_socket.send(
				json.dumps(
					{
						"type": 3,
						"sender_ip": req["sender_ip"],
						"sender_port": req["sender_port"],
						"req_id": req["req_id"],
						"status": 3,
						"message": "Data Not Recieved",
						"offset": 0,
						"chunk_handle": req["chunk_handle"]
					}
				),
				req["sender_ip"],req["sender_port"]
			)
		return 

	'''
	Master Update Format
	{
		"type": 2,
		"sender_ip":"localhost",
		"sender_port":8080,
		"sender_version": 2,
		"req_id": "666666666666666"
		"primary_ip": "100", 
		"lease_time": "5" (None),
		"version_number": 2,
		"ip_list" : [...],
	}
	'''
	def master_update_handler(self, req):
		self.primary_ip_port = req["primary_ip"]
		self.ip_list = req["ip_list"]
		self.version_number = req["version_number"]
		self.lease_time = req["lease_time"]
		return

	def heartbeat(self):
		while(True):
			recv_req = self.tcp_socket.receive()
			if recv_req == None:
				continue
			recv_req = json.loads(recv_req)
			if recv_req["type"] == 10:
				# TODOLIST: Create separate thread for this 
				self.swim_start(recv_req)
			elif recv_req["type"] == 11: 
				self.swim_handler(recv_req)
			elif recv_req["type"] == 12:
				self.swim_resp_handler(recv_req)
			else:
				print("FATAL ERROR WORLD WILL END SOON")


	'''
	Start swim request format:
		{
		"type": "10",
		"sender_ip":"localhost",
		"sender_port":8080,
		"sender_version": 1,
		"suspicious_ip": "localhost:2020"
		}
	'''
	def swim_start(self, req):
		# Start SWIM by sending a SWIM ping request to suspicious_ip
		sus_ip = req["suspicious_ip"].split(":")
		self.sock.send(json.dumps({"type": 11, "sender_ip": self.host, "sender_port": self.port}), 
			sus_ip[0], sus_ip[1])
		return
	
	'''
	Swim request format:
		{
		"type": "11",
		"sender_ip":"localhost",
		"sender_port":8080,
		}
	'''
	def swim_handler(self, req):
		ip = req["sender_ip"]
		self.sock.send(
			json.dumps(
				{
					"type": 12,
					"sender_ip": self.host,
					"sender_port": self.port
				}
			), ip[0], ip[1])
		return

	'''
	Swim response format:
		{
		"type": "12",
		"sender_ip":"localhost",
		"sender_port":8080,
		}
	'''
	def swim_resp_handler(self, req):
		# Send a response to master that we have got another path to the chunkserver
		# We have received from sus IP
		sus_ip = req["sender_ip"]
		sus_port = req["sender_port"]
		self.sock.send(
			json.dumps({
					"type": 13,
					"sender_ip": self.host,
					"sender_port": self.port,
					"sus_ip": sus_ip,
					"sus_port": sus_port
				}
			), self.master_ip[0], self.master_ip[1]
   		)
		return