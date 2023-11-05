MASTER_ADDR = '127.0.0.1'
MASTER_PORT = 25252

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