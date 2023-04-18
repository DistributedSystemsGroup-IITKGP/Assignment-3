from flask import request
from fastapi import FastAPI, Body, Request
from fastapi.encoders import jsonable_encoder
from log_queue import InMemoryLogQueue
from contextlib import asynccontextmanager
import datetime
import uvicorn
import sys
import os

log_queue = InMemoryLogQueue()

topics = {} # topic_name to topic_id
partitions = {} # topic_name to partition_id

@asynccontextmanager
async def lifespan(app: FastAPI):
    path_file = "./BrokerLogFiles/localhost{}.txt".format(port)

    if os.path.exists(path_file):
        with open(path_file, "r") as f:
            lines = f.readlines()

        for line in lines:
            request = eval(line)
            topic_name = request["topic_name"]
            partition_id = request["partition_id"]
            self_address = request["self_address"]
            other_addresses = request["other_address"]

            print(self_address, other_addresses)
            
            topics[topic_name] = len(topics)
            partitions[topic_name] = partition_id
            log_queue.create_topic(topic_name, partition_id, self_address, other_addresses)

    yield

server = FastAPI(lifespan = lifespan)
    

@server.get("/")
def index():
    return "<h1>Welcome to the In-memory Broker!</h1>"


@server.get("/status")
def status():
    return jsonable_encoder({"status": "success", "message": "broker running"})


@server.post("/topics")
def create_topic(request : dict = Body(...)):
    # request = request.json()
    topic_name = request["topic_name"]
    partition_id = request["partition_id"]
    self_address = request["self_address"]
    other_addresses = request["other_address"]

    with open("BrokerLogFiles/localhost{}.txt".format(port), "a") as f:
        dict_ = {"topic_name": topic_name, "partition_id": partition_id, "self_address": self_address, "other_address": other_addresses}
        f.write(str(dict_) + "\n")

    print(self_address, other_addresses)
    
    if topic_name in topics and partition_id == partitions[topic_name]:
        return jsonable_encoder({"status": "failure", "message": f"Topic '{topic_name}' partition '{partition_id}' already exists"})

    topics[topic_name] = len(topics)
    partitions[topic_name] = partition_id
    log_queue.create_topic(topic_name, partition_id, self_address, other_addresses)
    
    return jsonable_encoder({"status": "success", "message": f"Topic '{topic_name}' partition '{partition_id}' created successfully"})


@server.post("/producer/produce")
def enqueue(data : dict = Body(...)):
    # data = data.json()
    topic_name = data["topic_name"]
    partition_id = data["partition_id"]
    producer_id = data["producer_id"]
    log_message = data["log_message"]

    timestamp = datetime.datetime.utcnow()

    print("Got here 7")

    log_queue.enqueue(topic_name, partition_id, producer_id, log_message, timestamp)
    

    return jsonable_encoder({"status": "success"})


@server.get("/consumer/consume")
def dequeue(data : dict = Body(...)):
    topic_name = data["topic_name"]
    partition_id = data["partition_id"]
    consumer_front = data["consumer_front"]

    if log_queue.empty(topic_name, partition_id, consumer_front):
        return jsonable_encoder({"status": "failure", "message": "Queue is empty"})
    
    log_message, _, timestamp = log_queue.dequeue(topic_name, partition_id, consumer_front)

    return jsonable_encoder({"status": "success", "log_message": log_message, 'timestamp': timestamp})


@server.get("/size")
def size(data : dict = Body(...)):
    topic_name = data["topic_name"]
    partition_id = data["partition_id"]
    consumer_front = data["consumer_front"]
    return jsonable_encoder({"status": "success", "size": log_queue.size(topic_name, partition_id, consumer_front)})

port = 5000
if len(sys.argv)>1:
	port = int(sys.argv[1])

if __name__ == '__main__':
    uvicorn.run(server, host="0.0.0.0", port=port)