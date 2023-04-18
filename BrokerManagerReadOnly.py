import time
from flask import Blueprint, jsonify, request
import requests
import part2.health_check as health_check
from dbBrokerManager.config import async_session, engine, BaseBroker
from dbBrokerManager.AsyncDAL import DAL
import datetime
from pysyncobj import SyncObj, replicated
import asyncio
import random

server = Blueprint("broker_manager_Read_Only", __name__)

topics_lock = True

class Broker:
    def __init__(self, address, port, broker_id, isReadOnly = False):
        self.address = address
        self.port = port
        self.isAlive = True
        self.brokerID = broker_id
        self.isReadOnly = isReadOnly

    def declareDead(self):
        self.isAlive = False

    def declareAlive(self):
        self.isAlive = True
    
    async def checkHealth(self):
        try:
            print(self.address)
            r = requests.get(f"{self.address}/status")
            print(r)
            response = r.json()
            print(response)
            if response["status"] == "success" and response["message"] == "broker running":
                self.declareAlive()
            else:
                self.declareDead()
        except Exception as e:
            print(e)
            self.declareDead()

brokers = [Broker("http://localhost:{}".format(5000+i), 5000+i, i) for i in range(100, 110)]                  # List of brokers in the network

async def getServerAddress(broker_id):
    global topics_lock
    while topics_lock == False:
        time.sleep(1)
    topics_lock = False 

    global brokers

    address = None

    await brokers[broker_id].checkHealth()
    if brokers[broker_id].isAlive:
        address = brokers[broker_id].address
    else:
        brokers[broker_id].declareDead()
    
    topics_lock = True

    return address

@server.before_app_first_request
async def setUpBrokerManager():
    # This function sets up the broker manager by backing up things from server and setting up brokers in the network and read only copies of broker manager.
    global topics_lock
    topics_lock = True

@server.route("/")
def index():
    return "<h1>Welcome to the Broker Manager Copy!</h1>"

@server.route("/status")
def status():
    return jsonify({"status": "success", "message": "Broker Manager Copy running"})

@server.route("/topics", methods=["POST"])
async def create_topic():
    topic_name = request.json["topic_name"]
    topic_id = request.json["topic_id"]

    brokers_list = list()

    numPartitions = random.randint(1, len(brokers)-1)

    counter = 0

    for broker_id in range(len(brokers)):
        if counter == numPartitions:
            break

        address = await getServerAddress(broker_id)
        print(address)
        if address is None:
            continue

        raft_addresses = list()
        partition_addresses = list()

        await brokers[broker_id].checkHealth()
        if brokers[broker_id].isAlive:
            raft_address = "127.0.0.1:{}".format(brokers[broker_id].port + 1000 + topic_id * 100 + counter*10 + 1)

            raft_addresses.append(raft_address)
            partition_addresses.append(address)
            counterSec = 0
            for broker_id_sec in range(len(brokers)):
                if broker_id_sec == broker_id:
                    continue
                if counterSec == 2:
                    break

                await brokers[broker_id_sec].checkHealth()
                if brokers[broker_id_sec].isAlive:
                    raft_address = "127.0.0.1:{}".format(brokers[broker_id_sec].port + 1000 + topic_id * 100 + counter*10 + counterSec + 5)
                    raft_addresses.append(raft_address)
                    partition_addresses.append(brokers[broker_id_sec].address)
                    counterSec += 1
                else:
                    continue
            print(partition_addresses)
            print(raft_addresses)
            for i in range(len(partition_addresses)):
                self_addr = raft_addresses[i]
                other_addrs = [raft_addresses[(i+j)%3] for j in range(1,3)]
                params = {"topic_name": topic_name, "topic_id": topic_id, "partition_id": counter, "self_address": self_addr, "other_address": other_addrs}
                r = requests.post(f"{partition_addresses[i]}/topics", json=params)
                response = r.json()
                print(response)
                
            brokers_list.append(partition_addresses)
            counter += 1
        else:
            continue

    if len(brokers_list) == 0:
        return jsonify({"status": "failure", "message": f"Topic '{topic_name}' creation failed"})

    return jsonify({"status": "success", "message": f"Topic '{topic_name}' created successfully", "brokers_list": brokers_list})
   

@server.route("/producer/produce", methods=["POST"])
async def enqueue():
    topic_name = request.json["topic_name"]
    producer_id = request.json["producer_id"]
    log_message = request.json["log_message"]
    partition_address = request.json["partition_address"]
    partition_id = request.json["partition_id"]
    
    address = None

    print("Got here 4")

    for addressPart in partition_address:
        print(addressPart)
        r = requests.get(f"{addressPart}/status")
        response = r.json()
        if response["status"] == "success" and response["message"] == "broker running":
            address = addressPart
            break

    print("Got here 5")
    print(address)

    params = {"topic_name": topic_name, "producer_id": producer_id, "log_message": log_message, "partition_id": partition_id}
    r = requests.post(f"{address}/producer/produce", json=params)
    response = r.json()
    print(response)
    print(type(response))
    if response["status"] == "failure":
        return jsonify({"status": "failure", "message": f"Message production failed"})

    print("Got here 6")

    return jsonify({"status": "success"})


@server.route("/consumer/consume", methods=["GET"])
async def dequeue():
    topic_name = request.json["topic_name"]
    consumer_id = request.json["consumer_id"]
    partitions = request.json["partitions"]
    consumerFront = request.json["consumer_fronts"]

    log_message = None
    minTime = datetime.datetime.utcnow()
    minbrokerID = None

    actualPartitions = set()
    for x in partitions:
        actualPartitions.add(x[0])

    print('-'*50)
    print(actualPartitions)

    for broker_id in actualPartitions:
        address = None

        for partition in partitions:
            if partition[0] == broker_id:
                addressPart = partition[1]
                r = requests.get(f"{addressPart}/status")
                response = r.json()
                if response["status"] == "success" and response["message"] == "broker running":
                    address = addressPart
                else:
                    continue
                
                print(partition, address)

                if address is None:
                    return jsonify({"status": "failure", "message": f"Data Lost due to complete broker failure"})

                query = None
                params = {"topic_name": topic_name, "partition_id": broker_id, "consumer_front": consumerFront[str(broker_id)]}
                r = requests.get(f"{address}/consumer/consume", json=params)
                query = r.json()
                
                if query["status"] == "failure":
                    continue
                
                print(query)

                if minTime > datetime.datetime.strptime(query["timestamp"].replace('T', ' '), '%Y-%m-%d %H:%M:%S.%f'):
                    minTime = datetime.datetime.strptime(query["timestamp"].replace('T', ' '), '%Y-%m-%d %H:%M:%S.%f')
                    log_message = query["log_message"]
                    minbrokerID = broker_id
    
    if log_message is None:
        return jsonify({"status": "failure", "message": f"No message to consume"})

    return jsonify({"status": "success", "log_message": log_message, "broker_id": minbrokerID})


@server.route("/size", methods=["GET"])
async def size():
    topic_name = request.json["topic_name"]
    consumer_id = request.json["consumer_id"]
    
    partitions = request.json["partitions"]
    consumerFront = request.json["consumer_fronts"]

    consumer_size = 0

    actualPartitions = set()
    for x in partitions:
        actualPartitions.add(x[0])

    for broker_id in actualPartitions:
        address = None

        for partition in partitions:
            if partition[0] == broker_id:
                addressPart = partition[1]
                r = requests.get(f"{addressPart}/status")
                response = r.json()
                if response["status"] == "success" and response["message"] == "broker running":
                    address = addressPart
                    break

        if address is None:
            return jsonify({"status": "failure", "message": f"Data Lost due to complete broker failure"})

        query = None
        params = {"topic_name": topic_name, "partition_id": broker_id, "consumer_front": consumerFront[str(broker_id)]}
        r = requests.get(f"{address}/size", json=params)
        query = r.json()
        if query["status"] == "failure":
            return jsonify({"status": "failure", "message": f"Size query failed"})
        consumer_size += query["size"]
    
    return jsonify({"status": "success", "size": consumer_size})