import kafka

class KafkaClient:
    def __init__(self, host, port, topic):
        self.host = host
        self.port = port
        self.topic = topic
        self.consumer = kafka.KafkaConsumer(self.topic, bootstrap_servers = '{host}:{port}'.format(host = self.host, port = self.port),auto_offset_reset='latest',group_id='my-group')
        self.producer = kafka.KafkaProducer(bootstrap_servers = '{host}:{port}'.format(host = self.host, port = self.port))

    def send(self, msg, topic):
        future = self.producer.send(topic, msg.encode('utf-8'))
        return future

    def receive(self):
        for msg in self.consumer:
            return msg.value.decode('utf-8')

    def close(self):
        self.producer.close()
        self.consumer.close()
        
