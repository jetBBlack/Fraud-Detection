from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaProducer, FlinkKafkaConsumer, JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink
from pyflink.common.serialization import SimpleStringSchema
from fraud_detection_job import FraudDetection
from model.server_log import fromString, ServerLog


#JDBC connect and write
query = "INSERT INTO server_log " + "(eventId, userId, eventType, locationCountry, eventTimeStamp) " + "VALUES (?, ?, ?, ?, ?)"
country_map = {
    "USA" : "United States of America",
    "IN" : "India", "UK" : "United Kingdom", "CA" : "Canada",
    "AU" : "Australia", "DE" : "Germany", "ES" : "Spain",
    "FR" : "France", "NL" : "New Zealand", "SG" : "Singapore",
    "RU" : "Russia", "JP" : "Japan", "BR" : "Brazil", "CN" : "China",
    "O" : "Other"}

def setStatement(stmt, event:ServerLog):
    stmt.setString(1, event.eventId)
    stmt.setInt(2, event.accountId)
    stmt.setString(3, event.eventType)
    stmt.setString(4, country_map.get(event.locationCountry, "Other"))
    stmt.setString(5, datetime.utcfromtimestamp(event.eventTimeSteamp).strftime('%Y-%m-%d %H:%M:%S'))

#fraud detection
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

events.from_collecttion(lambda event: fromString(event)).add_sink(
    JdbcSink.sink(query, lambda stmt, event: setStatement(stmt, event),
        jdbc_connection_options=JdbcExecutionOptions.builder().with_batch_size(10000).with_batch_interval_ms(200).with_max_retries(3).build(),
        jdbc_execution_options=JdbcConnectionOptions.JdbcConnectionOptionsBuilder().with_url("jdbc:postgresql://postgres:5432/events").with_driver_name('org.postgresql.Driver').with_user_name('shadowburn').with_password('password').build())
).name("events-log")

env.execute("Fraud Detection")


    