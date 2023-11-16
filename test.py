import redis

rds = redis.Redis(host = 'localhost', port=6380, password='',db=0, decode_responses=False)

x=rds.append(1,"abc")
print(x)