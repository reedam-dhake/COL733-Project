from redis.client import Redis

class MyRedis:
	def __init__(self, host_ip,port_num):
		self.is_terminated = False
		self.host_ip = host_ip
		self.port_num = port_num
		self.rds = Redis(host=host_ip, port=port_num, password='',db=0, decode_responses=False)
		self.rds.flushall()

	def wirte(self, key, value):
		self.rds.set(key, value)

	def read(self):
		chunk = ""
		for key in sorted(self.rds.scan_iter()):
			chunk += self.rds.get(key).decode()
		return chunk
