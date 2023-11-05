import socket
import threading
import os
import math
import sys
import pickle

class client(object):
	def __init__(self):
		# LRU cache for caching chunk handle(s), lease and replicas location temporarily
		return 
	
# master communication for reading

	# SENDS
	def request_chunk_handle(self):
		return
	
	# RECVS
	def recieve_chunk_handle(self):
		return
	
# chunkserver communication for reading

	# SENDS
	def request_chunk(self):
		return
	
	# RECVS
	def recieve_chunk(self):
		return
	
# master communication for writing

	# Checking Cache for a chunk's lease and replicas location
	def check_cache(self)->bool:
		return True
	
	# Retrieval of chunk's lease and replicas location from cache 
	def get_cache(self):
		return

	# SENDS
	def request_chunk_lease(self):
		return
	
	# RECV
	def revieve_chunk_lease(self):
		return
	
# chunkserver communication for writing
	
	# SEND
	def push_data(self):
		return

	# RECV
	def recive_ack(self):
		return


