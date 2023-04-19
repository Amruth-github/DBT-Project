from json import loads  
from kafka import KafkaConsumer  
from pymongo import MongoClient 

# generating the Kafka Consumer  
my_consumer = KafkaConsumer(  
    'mytopic',  
     bootstrap_servers = ['localhost : 9092'],  
     auto_offset_reset = 'earliest',  
     enable_auto_commit = True,  
     group_id = 'my-group',  
     value_deserializer = lambda x : loads(x.decode('utf-8'))  
     )  

"""
uri = "mongodb+srv://JavaFX:AkArshj21@javafx.qe7bf3c.mongodb.net/?retryWrites=true&w=majority"
# Create a new client and connect to the server
my_client = MongoClient(uri)
# Send a ping to confirm a successful connection
try:
    my_client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

my_collection = my_client.linuxhint.linuxhint"""

for message in my_consumer:  
    message = message.value  
    #collection.insert_one(message)  
    print(str(message) + " added to ")
