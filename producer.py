from kafka import KafkaProducer
from const import *
import sys
import time

try:
    topic = sys.argv[1]
    username = "admin"
    password = "admin-secret"
except:
    print ('Usage: python3 producer <topic_name> <username> <password>')
    exit(1)
    
producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
                         security_protocol='SASL_PLAINTEXT',
                         sasl_mechanism='PLAIN',
                         sasl_plain_username=username,
                         sasl_plain_password=password)

try: 
    i = 0
    while True:
        i = i + 1
        msg = 'My ' + str(i) + 'st message for topic ' + topic
        print ('Sending message: ' + msg)
        producer.send(topic, value=msg.encode())
        time.sleep(0.5)
except KeyboardInterrupt:
    producer.flush()
