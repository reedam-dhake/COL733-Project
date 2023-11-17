MASTER_ADDR = 'localhost'
MASTER_TCP_PORT = 5999

CHUNKSERVER_IPS = ['localhost','localhost','localhost','localhost','localhost','localhost','localhost','localhost','localhost']
CHUNKSERVER_TCP_PORTS = [6000,6001,6002,6003,6004,6005,6006,6007,6008]

EXTRA_CHUNKSERVER_IPS = ['localhost','localhost']
EXTRA_CHUNKSERVER_TCP_PORTS = [6009,6010]

REQUEST_DELIMITER = ":::::::"

LEASE_TIME = 600


class Chunk:
	# replica: list of tuple of IP and Port-Number
	# version: integer value (version of chunk), default (0) 
	# primary: tuple of IP and Port-Number

	def __init__(self) -> None:
		self.version = 0
		self.primary: tuple = None
		self.replica: set = set()

	def update_version(self) -> None:
		self.version += 1
	
	def update_primary(self,primary: tuple) -> None:
		self.primary = primary
	
	def store_replica(self,replica: tuple) ->None:
		self.replica.add(replica)
	
	def get_replica(self) -> list:
		if len(self.replica) < 3:
			raise Exception("Less than 3 replicas")
		return self.replica
	
	def get_version(self) -> int:
		return self.version
	
	def get_primary(self) -> tuple:
		if self.primary == None:
			raise Exception("Primary Not assigned!!")
		return self.primary
	
class File:
	def __init__(self,filename) -> None:
		self.filename: str = filename
		self.chunks: dict[int,Chunk] = {}

	def add_chunk(self,chunk_index: int,chunk: Chunk) -> None:
		self.chunks[chunk_index] = chunk 

	def get_chunk(self,chunk_index: int) -> Chunk:
		if chunk_index not in self.chunks:
			raise Exception("Chunk not found")
		return self.chunks[chunk_index]

## SWIM CONSTANTS 
SWIM_K = 2
SWIM_T = 5
SWIM_RTT = 1
SWIM_ROUNDS = 3