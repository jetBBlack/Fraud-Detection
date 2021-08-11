from pyflink.datastream.functions import SinkFunction
from model.server_log import fromString
from pyflink.datastream.connectors import JdbcSink

class ServerLogSink(JdbcSink):
    INSERT_CASE = "INSERT INTO server_log"+"(eventId, userId, eventType, locationCountry, eventTimeStamp) " +"VALUES (?, ?, ?, ?, ?)"
    COUNTRY = {"USA": "United States of America",
                "IN": "India", "UK" : "United Kingdom", "CA" : "Canada",
                "AU": "Australia", "DE" : "Germany", "ES" : "Spain",
                "FR": "France", "NL" : "New Zealand", "SG" : "Singapore",
                "RU": "Russia", "JP" : "Japan", "BR" : "Brazil", "CN" : "China",
                "O" :"Other"}  

   
   
        



#https://hakibenita.com/fast-load-data-python-postgresql#execute-batch