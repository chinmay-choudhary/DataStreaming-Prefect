from prefect import flow, task,get_run_logger
from confluent_kafka import Producer,Consumer, KafkaException, KafkaError
from cryptography.fernet import Fernet
import base64
from datetime import datetime
import httpx
from os import environ
from dotenv import load_dotenv
import json
import pydash
import uuid
import time
load_dotenv()


@task(log_prints=True)
def getData():
    logger = get_run_logger()
    logger.info("Starting Data Pull")
    url = "https://randomuser.me/api/"
    logger.info("Data Pulled")
    data = httpx.get(url).json()
    data = data['results'][0]
    logger.info(f'Fetched Data is {data}')
    return data


@task(log_prints=True)
def prepData(data,id):
    logger = get_run_logger()
    def getValueFromJson(json, key, default='null'): 
        return pydash.get(json, key, default)
    
    def formatDate(date):
        date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
        date = date.strftime("%Y-%m-%d")
        return date

    def encryptData(message):
        """
        Encrypts a message.
        """
        try:
            f = Fernet(environ.get('FERNET_KEY'))
            message = f.encrypt(message.encode())

            return base64.urlsafe_b64encode(message).decode()
        except Exception as error:
            logger.error(error)
            return 'null'

    def extractPersonDetails(data,id):
        """
            "registration_date"	"date"
            "registration_age"	"integer"
            "uuid"	"uuid"
            "dob"	"date"
            "age"	"integer"
            "phone"	"character varying"
            "cell"	"character varying"
            "title"	"character varying"
            "nationality"	"character varying"
            "first_name"	"character varying"
            "last_name"	"character varying"
            "gender"	"character"
            "email_id"	"character varying"
        """

        registrationDate = formatDate(getValueFromJson(data,"registered.date"))
        registrationAge = getValueFromJson(data,"registered.age",0)
        dob = formatDate(getValueFromJson(data,"dob.date"))
        age = getValueFromJson(data,"dob.age",0)
        phone = getValueFromJson(data,"phone")
        cell = getValueFromJson(data,"cell")
        title = getValueFromJson(data,"name.title")
        firstName = getValueFromJson(data,"name.first")
        lastName = getValueFromJson(data,"name.last")
        gender = getValueFromJson(data,"gender")[0].capitalize()
        nationality = getValueFromJson(data,"nat")
        emailId = getValueFromJson(data,"email")

        return {
            "registration_date":registrationDate,
            "registration_age":registrationAge,
            "uuid":id,
            "dob":dob,
            "age":age,
            "phone":phone,
            "cell":cell,
            "title":title,
            "nationality":nationality,
            "first_name":firstName,
            "last_name":lastName,
            "gender":gender,
            "email_id":emailId
        }
    def extractLocationDetails(data,id):
        """
        "uuid"	"uuid"
        "street_number"	"integer"
        "street_name"	"character varying"
        "city"	"character varying"
        "state"	"character varying"
        "country"	"character varying"
        "postcode"	"character varying"
        "latitude"	"character varying"
        "longitude"	"character varying"
        "timezone_offset"	"character varying"
        "timezone_description"	"character varying"
        """
        streetNumber = int(getValueFromJson(data,"location.street.number",0))
        streetName   = getValueFromJson(data,"location.street.name")
        city = getValueFromJson(data,"location.city")
        state = getValueFromJson(data,"location.state")
        country = getValueFromJson(data,"location.country")
        postcode = getValueFromJson(data,"location.postcode")
        latitude = getValueFromJson(data,"location.coordinates.latitude")
        longitude = getValueFromJson(data,"location.coordinates.longitude")
        timezoneOffset = getValueFromJson(data,"location.timezone.offset")
        timezoneDescription = getValueFromJson(data,"location.timezone.description")

        return {
            "uuid":id,
            "street_number":streetNumber,
            "street_name":streetName,
            "city":city,
            "state":state,
            "country":country,
            "postcode":postcode,
            "latitude":latitude,
            "longitude":longitude,
            "timezone_offset":timezoneOffset,
            "timezone_description":timezoneDescription,
        } 
    
    def extractIdDetails(data,id):
        """
        "uuid"	"uuid"
        "id_name"	"character varying"
        "id_value"	"character varying"
        """
        idName = getValueFromJson(data,"id.name")
        idVal = encryptData(getValueFromJson(data,"id.value"))

        return {
            "uuid" : id,
            "id_name":idName,
            "id_value" : idVal
        }
    
    def extractLoginDetails(data,id):
        """
        "uuid"	"uuid"
        "username"	"character varying"
        "password_hash"	"character varying"
        "email_id"	"character varying"
        """
        userName = getValueFromJson(data,"login.username")
        passwordHash = encryptData(getValueFromJson(data,"login.password"))
        emailId = getValueFromJson(data,"email")

        return {
            "uuid":id,
            "username":userName,
            "password_hash":passwordHash,
            "email_id":emailId
        }
    
    return {
        "person": extractPersonDetails(data,id),
        "location" : extractLocationDetails(data,id),
        "id" : extractIdDetails(data,id),
        "login": extractLoginDetails(data,id)
    }

@task(log_prints=True)
def streamDataToKafka(data):
    logger = get_run_logger()
    kafkaBroker = f'{environ.get("CLOUDKAFKA_HOSTNAME")}:{environ.get("CLOUDKAFKA_PORT")}'
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

    producer = Producer(conf)

    def deliveryReport(err, msg):
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

    # Replace this with your data sending logic
    try:
        # Assuming 'data' is a dictionary that you want to send as JSON
        producer.produce(topicName, json.dumps(data).encode('utf-8'), callback=deliveryReport)
        producer.flush()
    except Exception as e:
        logger.error(f"Error sending message: {e}")


@flow(name="Data Stream To Kafka")
def dataFlow():
    while True:
        start_time = time.time()
        data = getData()
        data = prepData(data, str(uuid.uuid4()))
        streamDataToKafka(data)
        time.sleep(max(20 - (time.time() - start_time), 0))  # Ensures that loop runs approximately every 20 seconds

if __name__ == "__main__":
    dataFlow()