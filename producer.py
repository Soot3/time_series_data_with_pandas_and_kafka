# importing packages
import pandas as pd
import json
import datetime as dt

from kafka import KafkaProducer


df = pd.read_csv("timeseries.csv")

df["date"] = pd.to_datetime(df["date"])

# initializing Kafka Producer Client
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         json.dumps(x,default=str).encode('utf-8'))


print('Initialized Kafka producer at {}'.format(dt.datetime.utcnow()))

for i in df.index:
        
    # add the schema for Kafka
    data = {'schema': {
        'type': 'struct',
        'fields': [{'type': 'datetime64[ns]', 'optional': False, 'field': 'date'
                }, {'type': 'float64', 'optional': False, 'field': 'meantemp'
                },{'type': 'float64', 'optional': False, 'field': 'humidity'
                },{'type': 'float64', 'optional': False, 'field': 'wind_speed'
                },{'type': 'float64', 'optional': False, 'field': 'meanpressure'
                }]
        }, 'payload': {'date': df['date'][i],
                    'meantemp': df['meantemp'][i]},'humidity': df['humidity'][i],
                    'wind_speed': df['wind_speed'][i],'meanpressure': df['meanpressure'][i]}

    producer.send(topic="timeseries",value=data)
        
print('Sent record to topic at time {}'.format(dt.datetime.utcnow()))
