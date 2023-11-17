import socket
import threading
import os
import math
import sys
import pickle
import time,ast
from constants import Chunk,File,MASTER_ADDR,MASTER_PORT
from socket_class import TCPSocketClass

class MasterServer(object):
	def __init__(self):
		self.files: dict[str,File] = {}
		self.addr: str = MASTER_ADDR
		self.port: int = MASTER_PORT
		self.chunkserver_count: int = 0
		self.chunkserver_workload: int = []
		self.chunkserver_lease: bool = [] 
		self.socket: TCPSocketClass = TCPSocketClass(self.port,self.addr)
		return
	
	# RECVS
	def listen(self):
		while True:
			msg = self.socket.receive()
			if msg == None:
				time.sleep(1)
				continue
			msg_split = msg.split(":")

			# handling role of the sender (client / chunkserver)
			role = msg_split[0]
			msg = msg_split[1]+':'+msg_split[2]
			if role == "client":
				self.listen_client(msg)
			elif role == "chunkserver":
				self.listen_heartbeat(msg)
		return

	def listen_client(self,msg: str):
		msg_split = msg.split(':')

		# handling cliend address part
		client_id = msg_split[0]
		temp = client_id.split('#')
		client_ip_addr = temp[0]
		client_port_no = int(temp[1])

		# handling mssg part of request
		msg = msg_split[1]
		temp = msg.split('#')
		operation = temp[0]
		filename = temp[1]
		chunk_index = int(temp[2])

		# responding back to client with chunk_handle
		if operation == "read":
			response_msg = self.handle_read(filename,chunk_index)
		else:
			response_msg = self.handle_write(filename,chunk_index)
		self.send_client(response_msg,client_port_no,client_ip_addr)
		return
	

	def listen_heartbeat(self,msg: str):
		msg_split = msg.split(':')

		# handling cliend address part
		chunkserver_id = msg_split[0]
		temp = chunkserver_id.split('#')
		cs_ip_addr = temp[0]
		cs_port_no = int(temp[1])
		replica_location = (cs_ip_addr,cs_port_no)
		# handling mssg part of heartbeat
		msg = msg_split[1]
		temp = msg.split('#')
		chunks_map = ast.literal_eval(temp[1])
		self.Update_FileToChunk_Mapping(chunks_map,replica_location)

		return
	# SENDS
	def send_client(self,message: str,port_no: int,ip_addr: str):
		self.socket.send(message,port_no,ip_addr)
		return "SENT RESPONSE TO CLIENT!!"
	
	def send_chunkservers(self,message: str,port_no: int, ip_addr: str):
		self.socket.send(message,port_no,ip_addr)
		return "SENT RESPONSE TO CHUNKSERVER!!"
	
	# MASTER FUNCTIONS

	def choose_replica(self):
		replica = []
		return replica
	
	def choose_primary(self):
		return ()
	
	def alloc_chunks(self):
		return
	
	def Update_FileToChunk_Mapping(self,chunks_map: list,replica_location: tuple):
		for x in chunks_map:
			filename = x[0]
			chunk_index = int(x[1])
			try:
				new_chunk = self.files[filename].get_chunk(chunk_index)
			except:
				new_chunk = Chunk()
			new_chunk.store_replica(replica_location)
			self.files[filename].add_chunk(chunk_index,new_chunk)
		return
	
	def handle_read(self,filename: str, chunk_index: int) -> str:
		if filename not in self.files:
			raise Exception("File doesn't exist")
		file  = self.files[filename]
		chunk = file.get_chunk(chunk_index)
		replica = chunk.get_replica()
		master_id = self.addr + '#' + str(self.port)
		response = 'replica' + '#' + str(replica)
		response_msg = 'master' + ':' + master_id + ':' + response
		return response_msg
	
	def handle_write(self,filename: str, chunk_index: int) -> str:
		if filename not in self.files:
			self.files[filename] = File(filename)
		replica = self.choose_replica()
		primary = self.choose_primary()
		master_id = self.addr + '#' + str(self.port)
		response = 'replica' + '#' + str(replica) + '#' + 'primary' + '#' + str(primary)
		response_msg = 'master' + ':' + master_id + ':' + response
		return response_msg