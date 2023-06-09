rm -rf BrokerJournalFiles

rm -rf BrokerLogFiles

rm serverBrokerManager.db

mkdir BrokerJournalFiles

mkdir BrokerLogFiles

curl -X POST -H "Content-Type: application/json" -d '{"topic_name":"Kagenou"}' http://localhost:5000/topics

curl -X POST -H "Content-Type: application/json" -d '{"topic_name":"Minoru"}' http://localhost:5000/topics

# sleep 50

curl -X GET http://localhost:5000/topics

curl -X POST -H "Content-Type: application/json" -d '{"topic_name":"Kagenou"}' http://localhost:5000/consumer/register

curl -X POST -H "Content-Type: application/json" -d '{"topic_name":"Minoru"}' http://localhost:5000/producer/register

curl -X POST -H "Content-Type: application/json" -d '{"topic_name":"Kagenou"}' http://localhost:5000/producer/register

sleep 50

curl -X POST -H "Content-Type: application/json" -d '{"topic_name":"Minoru", "producer_id":1, "log_message":"example log message 1", "partition_id":0}' http://localhost:5000/producer/produce

curl -X POST -H "Content-Type: application/json" -d '{"topic_name":"Kagenou", "producer_id":2, "log_message":"example log message 2"}' http://localhost:5000/producer/produce

curl -X GET -H "Content-Type: application/json" -d '{"topic_name":"Kagenou", "consumer_id":1}' http://localhost:5000/consumer/consume

curl -X GET -H "Content-Type: application/json" -d '{"topic_name":"Kagenou", "consumer_id":1}' http://localhost:5000/size