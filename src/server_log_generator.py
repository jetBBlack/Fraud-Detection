import random
import uuid
import time
from kafka import KafkaProducer, producer
import kafka
from model.server_log import ServerLog

locationCountry =  [ "USA", "IN", "UK", "CA", "AU", "DE", "ES", "FR", "NL", "SG", "RU", "JP", "BR", "CN", "O"]

eventType = ["click", "purchase", "login", "log-out", "delete-account", "create-account", "update-settings", "other"]

def getSeverLog():
    eventId = str(uuid.uuid4())
    accountId = random.randint(0,10000)
    currentEventType = random.choice(eventType)
    currentCountry = random.choice(locationCountry)
    timestamp = time.gmtime()
    return ServerLog(eventId, accountId, currentEventType, currentCountry, timestamp)

def sendEvent():
    topic = "server-logs"
    config = {
        'bootstrap.servers': 'kafka:9092',
        'key.serializer' : 'org.apache.kafka.common.serialization.StringSerializer',
        'value.serializer' : 'org.apache.kafka.common.serialization.StringSerializer'
    }

    producer = KafkaProducer(configs=config)
    
    i = 0
    while(i<10000):
        log = getSeverLog()
        producer.send(topic=topic, key=log.eventId, value=log.toString)
        i +=1

    producer.close()

if __name__ == "__main__":
    sendEvent()


    
