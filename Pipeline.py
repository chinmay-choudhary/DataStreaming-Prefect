from prefect import flow, task,get_run_logger
from confluent_kafka import Consumer, KafkaException, KafkaError
import pymongo
import logging
from pymongo import MongoClient
from os import environ
from dotenv import load_dotenv
import json
load_dotenv()

class MongoDatabase:
    def __init__(self, connectionString:str,dbName:str):
        '''
        Initialize MongoDB connection.
        '''
        self.client = MongoClient(connectionString)
        self.db = self.client[dbName]

    def insertData(self, collectionName, data):
        '''
        Inserts a document or multiple documents into a collection.
        '''
        try:
            collection = self.db[collectionName]
            if isinstance(data, list):
                result = collection.insert_many(data)
                return result.inserted_ids
            else:
                result = collection.insert_one(data)
                return result.inserted_id
        except pymongo.errors.PyMongoError as e:
            logging.error(f"Error while inserting data: {e}")
            return str(e)

    def runQuery(self, collectionName, query):
        '''
        Executes a given query and returns the results.
        '''
        try:
            collection = self.db[collectionName]
            result = collection.find(query)
            return list(result)
        except pymongo.errors.PyMongoError as e:
            logging.error(f"Error while running query: {e}")
            return str(e)

    def fetchAllRecords(self,collectionName):
        '''
        Fetch All Data from collection
        '''
        try:
            collection = self.db[collectionName]
            results = []
            for record in collection.find():
                record.pop('_id')
                results.append(record)
            return results
        except pymongo.errors.PyMongoError as e:
            logging.error(f"Error while running query: {e}")
            return str(e)

    def close(self):
        '''
        Close the MongoDB connection.
        '''
        self.client.close()

@task(log_prints=True)
def streamDataFromKafkaToMongo():
    kafkaBroker = f'{environ.get("CLOUDKAFKA_HOSTNAME")}:{environ.get("CLOUDKAFKA_PORT")}' # Replace with actual broker port

    topicName = 'gcskzyuu-stream-userdata'

    conf = {
        'bootstrap.servers': kafkaBroker,
        'group.id': 'gcskzyuu-chinmay-group',
        'session.timeout.ms': 6000,
        'auto.offset.reset': 'earliest',
        'sasl.mechanisms': 'SCRAM-SHA-512',
        'security.protocol': 'SASL_SSL',
        'sasl.username': environ.get("CLOUDKAFKA_USERNAME"),
        'sasl.password': environ.get("CLOUDKAFKA_PASSWORD")
    }

    consumer = Consumer(conf)

    def print_assignment(consumer, partitions):
        print('Assignment:', partitions)

    # Subscribe to topics
    consumer.subscribe([topicName], on_assign=print_assignment)

    print('Connecting to db')
    db = MongoDatabase(f'mongodb+srv://{environ.get("MONGODB_USERNAME")}:{environ.get("MONGODB_PASSWORD")}@{environ.get("MONGODB_PORT")}','streamedData')

    print('Connected to db')


    print(f"Listening for messages on topic '{topicName}'...")
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(f"Kafka error: {msg.error()}")
                    break

            try:
                print('Inserting Data into db')
                data = json.loads(msg.value().decode('utf-8'))  # Assuming message value is JSON
                db.insertData('cleanedData', data)
                print('Inserted Data Into db')
                consumer.commit(asynchronous=False)  # Committing the message

            except Exception as e:
                logging.error(f"Error processing message: {e}")
    finally:
        consumer.close()

@flow(name="Data Stream From Kafka")
def dataFlow():
    streamDataFromKafkaToMongo()

if __name__=="__main__":
    dataFlow()