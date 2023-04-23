import socket
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import pickle
from kafka import KafkaConsumer
import json
from time import sleep
from threading import *

# Data to plot
labels = ["Traffic Hazard", "Crash Urgent", "Crash Service", "COLLISION"]
sizes = [1, 1, 1, 1]

# Set up the pie chart
fig, ax = plt.subplots()
ax.pie(sizes, labels=labels, autopct='%1.1f%%')
plt.ion()


consumer = KafkaConsumer('aggregate_count', group_id='plot', bootstrap_servers=[
                         "localhost:9092"], value_deserializer=lambda x: json.loads(x))
                       
for message in consumer:
	aggregate_count = message.value
	sizes[0] += aggregate_count.get("Traffic Hazard", 0)
	sizes[1] += aggregate_count.get("Crash Urgent", 0)
	sizes[2] += aggregate_count.get("Crash Service", 0)
	sizes[3] += aggregate_count.get("COLLISION", 0)
	ax.clear()
	ax.pie(sizes, labels = labels, autopct='%1.1f%%')
	plt.ion()
	plt.pause(0.000001)

	

