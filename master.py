from socket_class import TCPSocketClass
from mrds import MyRedis
from constants import *
import json, threading, uuid,random, datetime
from collections import defaultdict

class Master(object):
	def __init__(self,host,port,redis_port):
		self.host = host
		self.port = port
		self.socket = TCPSocketClass(self.port,self.host)
		self.heartbeat_socket = TCPSocketClass(self.port+1000,self.host)
		self.rds = MyRedis(host, redis_port)
  
		self.extra_ips = EXTRA_CHUNKSERVER_IPS
		self.extra_ports = EXTRA_CHUNKSERVER_TCP_PORTS
		# chunkgrp_map format: chunk_group_id -> (chunk_server_ip_port,primary_ip_port,version,lease)
		self.chunkgrp_map : dict[str, tuple(list(tuple(str,int)), tuple(str,int), int, int)] = {}
		self.ip_to_grpid_map : dict[tuple(str,int), str] = {}
  
		self.files : dict[tuple(str,str), tuple(str,str)] = {}

		self.rerouting_table = {}
		self.swim_mode = False
  
		self.populate_maps()
		listen_thread = threading.Thread(target=self.listen)
		listen_thread.start()
		heartbeat_thread = threading.Thread(target=self.heartbeat)
		heartbeat_thread.start()

	def populate_maps(self):
		# get chunk server ips and hosts from constants.py
		chunk_server_ips = CHUNKSERVER_IPS
		chunk_server_ports = CHUNKSERVER_TCP_PORTS
  
		assert(len(chunk_server_ips) == len(chunk_server_ports))
		assert(len(chunk_server_ips) % 3 == 0)
  
		for i in range(0,len(chunk_server_ips),3):
			ips_list = []
			chunk_group_id = str(uuid.uuid4())
			for j in range(i,i+3):
				ips_list.append((chunk_server_ips[j],chunk_server_ports[j]))
				self.ip_to_grpid_map[(chunk_server_ips[j],chunk_server_ports[j])] = chunk_group_id
			primary = random.choice(ips_list)
			lease_time = (datetime.datetime.now() + datetime.timedelta(seconds=LEASE_TIME)).strftime('%s')
			self.chunkgrp_map[chunk_group_id] = (ips_list,primary,1,lease_time)
		
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
		chunk_group_id = random.choice(list(self.chunkgrp_map.keys()))
		self.files[(filename,chunk_number)] = (chunk_handle,chunk_group_id)
		return
	
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
		miss_history = defaultdict(int)
		while(True):
			for ip in self.ip_to_grpid_map:
				# Change ping timeout 
				res = self.heartbeat_socket.ping(ip[0],ip[1]+1000)
				if not res:
					# if ip in self.rerouting_table:
					# 	continue
					miss_history[ip]+=1
					continue
				miss_history[ip] = 0					
				# self.rerouting_table.pop(ip)
			for ip in self.ip_to_grpid_map:
				if(miss_history[ip] >=3):
					if (self.swim_mode):
						self.run_swim(ip)
						# Give it another chance 
						miss_history[ip] = 0
					self.switch_server(ip)
					miss_history[ip] = 0
			self.update_primary_and_lease()

	def switch_server(self,failed_ip):
		# Make new chunk server
		new_ip = self.extra_ips.pop()
		new_port = self.extra_ports.pop()
		chunkgrp_id = self.ip_to_grpid_map[failed_ip]
		del self.ip_to_grpid_map[failed_ip]
		self.ip_to_grpid_map[(new_ip,new_port)] = chunkgrp_id
		# Update chunk group map
		chunk_server_ips,_,version,_ = self.chunkgrp_map[chunkgrp_id]
		new_chunk_server_ips = []
		for ip in chunk_server_ips:
			if ip == failed_ip:
				continue
			new_chunk_server_ips.append(ip)
		new_chunk_server_ips.append((new_ip,new_port))
		new_version = version + 1
		new_primary = random.choice(new_chunk_server_ips)
		new_lease_time = (datetime.datetime.now() + datetime.timedelta(seconds=LEASE_TIME)).strftime('%s')
		self.updates_handler(chunkgrp_id,new_version,new_primary,new_lease_time,new_chunk_server_ips)
  
	def update_primary_and_lease(self):
		for ip in self.ip_to_grpid_map:
			chunkgrp_id = self.ip_to_grpid_map[ip]
			chunk_server_ips,primary_ip,version,lease_time = self.chunkgrp_map[chunkgrp_id]
			if lease_time == None:
				continue
			if lease_time < datetime.datetime.now().strftime('%s'):
				# Update primary and lease time
				new_primary = random.choice(chunk_server_ips)
				new_lease_time = (datetime.datetime.now() + datetime.timedelta(seconds=LEASE_TIME)).strftime('%s')
				self.updates_handler(chunkgrp_id,version+1,new_primary,new_lease_time,chunk_server_ips)


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
	def updates_handler(self,chunkgrp_id,new_version,new_primary,new_lease_time,new_chunk_server_ips):
		self.chunkgrp_map[chunkgrp_id] = (new_chunk_server_ips,new_primary,new_version,new_lease_time)
		# Send update to chunk servers
		for ip in new_chunk_server_ips:
			request = {
				"type": 2,
				"sender_ip_port": f"{self.host}:{self.port}",
				"sender_version": new_version,
				"req_id": str(uuid.uuid4()),
				"primary_ip": new_primary,
				"lease_time": new_lease_time,
				"version_number": new_version,
				"ip_list" : new_chunk_server_ips,
			}
			self.socket.send(json.dumps(request),ip[1],ip[0])
		
		
		
	
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


if __name__ == "__main__":
	master = Master(MASTER_ADDR,MASTER_TCP_PORT,6379)