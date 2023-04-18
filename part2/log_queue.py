# from multiprocessing import Manager
from Topic import Topic
from pysyncobj import SyncObjConf
import time

class InMemoryLogQueue:
	def __init__(self):
		self.queue = {}
		self.self_address = None
		self.peers = None

	def create_topic(self, topic_name, partition_id, self_address, peers):
		qname = topic_name+'#'+str(partition_id)
		self.self_address = self_address
		self.peers = peers
		print(qname)
		print(self.self_address, self.peers)
		configSync = SyncObjConf(journalFile = "./BrokerJournalFiles/"+qname+self_address+".journal")
		self.queue[qname] = Topic(self_address, peers, topic_name, partition_id, configSync)
		self.queue[qname].waitBinded()
		self.queue[qname].waitReady()
		print(self.queue[qname].getStatus())

	def enqueue(self, topic_name, partition_id, producer_id, log_message, timestamp):
		qname = topic_name+'#'+str(partition_id)
		print("Hello")
		print("-"*50)
		print(qname)
		# while self.queue[qname]._getLeader() is None:
		# 	print(self.queue[qname].getStatus())
		# 	print("Waiting for leader")
		# 	time.sleep(1)
		# 	continue
		print(self.queue[qname].getStatus())
		print("Calling enqueue")
		self.queue[qname].enqueue1(producer_id, log_message, timestamp)
		print(self.queue[qname].getStatus())
		return 1

	def dequeue(self, topic_name, partition_id, consumer_front):
		qname = topic_name+'#'+str(partition_id)
		return self.queue[qname].dequeue(consumer_front)

	def empty(self, topic_name, partition_id, consumer_front):
		qname = topic_name+'#'+str(partition_id)
		print(self.queue[qname].getStatus())
		return self.queue[qname].empty(consumer_front)

	def size(self, topic_name, partition_id, consumer_front):
		qname = topic_name+'#'+str(partition_id)
		return self.queue[qname].size(consumer_front)
