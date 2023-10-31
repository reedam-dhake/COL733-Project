from typing import Optional

class Chunk:
    # replica: list of tuple of IP and Port-Number
    # version: integer value (version of chunk), default (0) 
    # primary: tuple of IP and Port-Number

    def __init__(self) -> None:
        self.version = 0
        self.primary: Optional[int] = None
        self.replica: Optional[list] = []

    def update_version(self) -> None:
        self.version += 1
    
    def update_primary(self,primary: tuple) -> None:
        self.primary = primary
    
    def store_replica(self,r1: tuple,r2: tuple,r3: tuple) ->None:
        self.replica.append(r1)
        self.replica.append(r2)
        self.replica.append(r3)
    
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
    def __init__(self) -> None:
        self.chunks: Optional[Chunk] = []

    def add_chunk(self,chunk: Chunk) -> None:
        self.chunks.append(chunk)

    def get_chunk(self,chunk_index: int) -> Chunk:
        if len(self.chunks)<chunk_index:
            raise Exception("Chunk index out of bound")
        return self.chunks[chunk_index]