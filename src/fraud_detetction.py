from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaProducer, FlinkKafkaConsumer, JdbcSink
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import SinkFunction
from fraud_detection_job import FraudDetection

query = "INSERT INTO server_log " + "(eventId, userId, eventType, locationCountry, eventTimeStamp) " + "VALUES (?, ?, ?, ?, ?)"
COUNTRY_MAP = {
    "USA" : "United States of America",
    "IN" : "India", "UK" : "United Kingdom", "CA" : "Canada",
    "AU" : "Australia", "DE" : "Germany", "ES" : "Spain",
    "FR" : "France", "NL" : "New Zealand", "SG" : "Singapore",
    "RU" : "Russia", "JP" : "Japan", "BR" : "Brazil", "CN" : "China",
    "O" : "Other"}
env = StreamExecutionEnvironment.get_execution_environment()

properties = {
    "bootstrap.servers": "kafka:9092"
}
myConsumer = FlinkKafkaConsumer(topics="server-logs",
    deserialization_schema=SimpleStringSchema(), properties=properties)
myConsumer.set_start_from_earliest()

events = env.add_source(myConsumer).name('incoming-events')

alerts = events.key_by(lambda event: event.split(",")[1]).process(FraudDetection()).name("fraud-detector")

myProducer = FlinkKafkaProducer("alerts", serialization_schema=SimpleStringSchema(), 
                                producer_config=properties)

alerts.add_sink(myProducer).name("send-alerts")

events.add_sink(
    JdbcSink.sink(sql=query,)
).name("event-log")

env.execute("Fraud Detection")