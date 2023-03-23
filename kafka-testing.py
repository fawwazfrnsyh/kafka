from kafka import KafkaProducer, KafkaConsumer
import requests

# Kafka Producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Retrieve data from url
url1 = 'https://dummyjson.com/products'
response1 = requests.get(url1)
data1 = response1.content

url2 = 'https://dummyjson.com/users'
response2 = requests.get(url2)
data2 = response2.content

# Send retrieved data as message to Kafka topic
producer.send('topic1',data1,partition=0)
producer.send('topic1',data1,partition=1)

# Kafka Consumer
consumer = KafkaConsumer('topic1', bootstrap_servers=['localhost:9092'])