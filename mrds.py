from redis.client import Redis

class MyRedis:
	def __init__(self, host_ip,port_num):
		self.is_terminated = False
		self.host_ip = host_ip
		self.port_num = port_num
		self.rds = Redis(host=host_ip, port=port_num, password='',db=0, decode_responses=False)
		self.rds.flushall()

	def record_append(self, key, value, offset):
		if offset:
			return self.rds.setrange(key, offset, value)
		else:
			return self.rds.append(key, value)

	def read_record(self,key,byte_range):
		startbyte, endbyte = byte_range.split(":")
		startbyte = int(startbyte)
		endbyte = int(endbyte)
		return self.rds.getrange(key, startbyte, endbyte).decode()
