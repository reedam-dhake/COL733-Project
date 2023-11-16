from socket_class import TCPSocketClass
from kafka_class import KafkaClient
from mrds import MyRedis
from constants import *
import json, threading, ping3, uuid,random,datetime

class Master(object):
	def __init__(self,host,port,heartbeat_port,redis_port,available_ips,available_extra_ips):
		self.host = host
		self.port = port
		self.heartbeat_port = heartbeat_port
		self.files : dict[tuple(str,str), tuple(str,str)] = {}
		self.chunkgrp_map : dict[str, tuple(list(tuple(str,int)), tuple(str,int), int, int)] = {}
		self.socket = TCPSocketClass(self.port,self.host)
		self.heartbeat_socket = TCPSocketClass(self.heartbeat_port,self.host)
		self.rds = MyRedis(host, redis_port)
		self.available_ips = available_ips
		self.available_extra_ips = available_extra_ips
		listen_thread = threading.Thread(target=self.listen)
		listen_thread.start()
		heartbeat_thread = threading.Thread(target=self.heartbeat)
		heartbeat_thread.start()
		self.rerouting_table = {}
		self.swim_mode = False
		
	def listen(self):
		while(True):
			recv_req = self.socket.receive()
			if recv_req == None:
				continue
			recv_req = json.loads(recv_req)
			if recv_req["type"] == 7:
				self.metadata_request_handler(recv_req)
			elif recv_req["type"] == 13:
				# SWIM 
				self.swim_handler(recv_req)
			else:
				print("FATAL ERROR IN MASTER WORLD WILL END SOON")

	'''
	Metadata request format: 
	{
		"type": 7,
		"sender_ip":"localhost",
		"sender_port":5000,
		"req_id": "666666666666666",
		"filename": "file.txt",
		"chunk_number": 53
	}
	'''
	def metadata_request_handler(self, req):
		sender_ip = req["sender_ip"]
		sender_port = req["sender_port"]
		filename = req["filename"]
		chunk_number = req["chunk_number"]
		req_id = req["req_id"]
		if (filename,str(chunk_number)) not in self.files:
			self.create_meta_record(filename,str(chunk_number))
		chunk_handle,chunk_group = self.files[(filename,str(chunk_number))]
		ip_list,primary_ip,version,lease_time = self.chunkgrp_map[chunk_group]
		response = {
			"type": 1,
			"sender_ip": sender_ip,
			"sender_port": sender_port,
			"req_id": req_id,
			"filename": filename,
			"chunk_number": chunk_number,
			"chunk_handle": chunk_handle,	
			"version_number": version,
			"primary_ip": primary_ip,
			"ip_list": ip_list,
			"lease_time": lease_time
		}
		self.socket.send(json.dumps(response),sender_port,sender_ip)
		return

	def create_meta_record(self,filename,chunk_number):
		chunk_handle = "{filename}:{chunk_number}".format(filename,chunk_number)
		chunk_group_id = str(uuid.uuid4())
		chunk_server_ip_list = self.allot_chunk_server()
		primary_ip,lease_time = self.choose_primary(chunk_server_ip_list)
		version = 1
		self.chunkgrp_map[chunk_group_id] = (chunk_server_ip_list,primary_ip,version,lease_time)
		self.files[(filename,chunk_number)] = (chunk_handle,chunk_group_id)
		return

	def allot_chunk_server(self):
		chunk_servers = random.sample(self.available_ips,3)
		return chunk_servers
	
	def choose_primary(self,chunk_server_list):
		primary_ip = chunk_server_list[random.randrange(len(chunk_server_list))]
		current_time = datetime.datetime.now()
		future_time = current_time + datetime.timedelta(seconds=600)
		lease_time = future_time.strftime('%s')
		return (primary_ip,lease_time)
	
	'''
	Iterate over chunk group and in each chunk group over the CS 
	Send ping to all
	If some server misses 3 pings:
	-> Make new chunkserver
	-> If primary: choose new primary, update version numbers, send update
	-> Else send update (list of ips in chunk group)
	->response on chunkserver failure :: 
	respose_format = {
		"type":3,
		"sender_ip_addr":"localhost",
		"sender_port":5050
		"new_server_ip_addr" : "localhost",
		"new_server_port":2020,
		"old_server_ip_addr": "localhost",
		"old_server_port":2022
	}
	'''
		
	def heartbeat(self):
		miss_history = [0 for x in range(len(self.available_ips))]
		while(True):
			for i in range(len(self.available_ips)):
				# Change ping timeout 
				res = ping3.ping(self.available_ips[i],timeout=5)
				if not res:
					if self.available_ips[i] in self.rerouting_table:
						continue
					miss_history[i]+=1
					continue
				miss_history[i] = 0					
				self.rerouting_table.pop(self.available_ips[i])
			for i in range(len(self.available_ips)):
				if(miss_history[i]<3):
					continue
				if (swim_mode):
					self.run_swim(self.available_ips[i])
					# Give it another chance 
					miss_history[i] = 0
				self.switch_server(i,self.available_ips[i])
				miss_history[i] = 0
			self.update_primary_and_lease()

	def switch_server(self,old_index,old_ip):
		index = random.randrange(len(self.available_extra_ips))
		# new ip from available_extra_ips
		new_ip = self.available_extra_ips[index]
		response = {
			"type":3,
			"sender_ip": self.host,
			"sender_port": self.heartbeat_port,
			"new_server_ip_addr" : new_ip[0],
			"new_server_port": new_ip[1],
			"old_server_ip_addr": old_ip[0],
			"old_server_port": old_ip[1]
		}
		self.heartbeat_socket.send(json.dumps(response),new_ip[1],new_ip[0])
		self.available_ips[old_index]=new_ip
		self.available_extra_ips[index]=old_ip
		updates = {}
		for x,y in self.chunkgrp_map:
			chunk_server_ips = y[0]
			if old_ip not in chunk_server_ips:
				continue
			for i in range(len(chunk_server_ips)):
				if chunk_server_ips[i] == old_ip:
					chunk_server_ips[i] = new_ip
					break
			primary_ip = y[1]
			if primary_ip == old_ip:
				primary_ip = new_ip
			version = y[2]
			lease = y[3]
			updates[x] = (chunk_server_ips,primary_ip,version,lease)
		for chunk_handle,metadata in updates:
			self.chunkgrp_map[chunk_handle] = metadata
		
	def update_primary_and_lease(self):
		lease_updates = {}
		for x,y in self.chunkgrp_map:
			chunk_server_ips = y[0]
			version = y[2]
			lease = y[3]
			now = datetime.datetime.now().strftime('%s')
			if lease > now:
				continue
			new_primary,new_lease = self.choose_primary(chunk_server_ips)
			lease_updates[x] = (chunk_server_ips,new_primary,version,new_lease)
		for chunk_handle,metadata in lease_updates:
			self.chunkgrp_map[chunk_handle] = metadata

	'''
	Send a message to all chunk servers which are a part 
	of the chunk group to run SWIM
	Start SWIM format
	{
		"type": "10",
		"sender_ip_port":"localhost:8080",
		"sender_version": 1,
		"suspicious_ip": "localhost:2020"
	}
	'''
	def run_swim(self, suspicious_ip):
		# Tell everyone else to start SWIM
		for k in self.chunkgrp_map:
			chunk_server_ips = self.chunkgrp_map[k][0]
			if suspicious_ip not in chunk_server_ips:
				continue
			for ip in chunk_server_ips:
				response = {
					"type": 10,
					"sender_ip_port": f"{self.host}:{self.port}",
					"sender_version": self.chunkgrp_map[k][2],
					"suspicious_ip": suspicious_ip
				}
				self.heartbeat_socket.send(json.dumps(response),ip[1],ip[0])
			break
		return

	'''
	SWIM HANDLER 
	Handle responses of chunkserver on handling chunks 
	req format = {
		"type": 13,
		"sender_ip_port": "localhost:8080",
		"sus_ip": "localhost:2020"
	}
	'''

	def swim_handler(self, req):
		sender_ip_port = req["sender_ip_port"]
		sus_ip = req["sus_ip"]
		# We have received confirmation from a chunk server that the sus IP is still up
		# We can mark it in the rerouting_table table
		self.rerouting_table[sus_ip] = sender_ip_port