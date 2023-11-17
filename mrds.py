from redis.client import Redis

class MyRedis:
	def __init__(self, host_ip,port_num):
		self.is_terminated = False
		self.host_ip = host_ip
		self.port_num = port_num
		self.rds = Redis(host=host_ip, port=port_num, password='',db=0)
		self.rds.flushall()
	
	def get(self, key):
		return self.rds.get(key)

	def record_append(self, key, value, offset):
		if offset:
			return self.rds.setrange(key, offset, value)
		else:
			return self.rds.append(key, value)

	def read_record(self,key,byte_range):
		startbyte, endbyte = byte_range.split(":")
		startbyte = int(startbyte)
		endbyte = int(endbyte)
		return self.rds.getrange(key, startbyte, endbyte)

	def migrate_data(self,send_ip, send_port, key):
		if key == None:
			all_keys = self.rds.keys()
			self.rds.migrate(send_ip, send_port, keys=all_keys, destination_db=0, copy=True, replace=True,timeout=1000000,auth='')
		else:
			self.rds.migrate(send_ip, send_port, keys=key, destination_db=0, copy=True, replace=True,timeout=1000000,auth='')
 