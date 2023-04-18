from pysyncobj import SyncObj, replicated_sync,replicated
from pysyncobj import batteries

class Topic(SyncObj):
	def __init__(self, self_address, peers, topic_name, partition_id, configSync):
		super(Topic, self).__init__(self_address, peers, conf = configSync)
		print(self_address, peers)
		self.topic_name = topic_name
		self.partition_id = partition_id
		self.queue1 = list()

	@replicated_sync
	def enqueue1(self, producer_id, log_message, timestamp):
		print("-"*50)
		print(log_message)
		print(len(self.queue1))
		self.queue1.append((log_message, producer_id, timestamp))
		
		return 1

	def dequeue(self, consumer_front):
		return self.queue1[consumer_front]

	def empty(self, consumer_front):
		return consumer_front == len(self.queue1)

	def size(self, consumer_front):
		return len(self.queue1) - consumer_front