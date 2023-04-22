import socket
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import pickle
from kafka import KafkaConsumer
import json
from time import sleep
from threading import *

# Data to plot
labels = ['Positive', 'Negative']
sizes = [1, 1]

# Set up the pie chart
fig, ax = plt.subplots()
ax.pie(sizes, labels=labels)
plt.ion()

consumer = KafkaConsumer('sentiment', group_id='plot', bootstrap_servers=[
                         "localhost:9092"], value_deserializer=lambda x: json.loads(x))

for message in consumer:
	if message.value.get("sentiment") == 1:
		sizes[0] += 1
	else:
		sizes[1] += 1
	ax.clear()
	ax.pie(sizes, labels = labels)
	plt.pause(0.001)
		
		
    
# Create the animation
ani = FuncAnimation(fig, iterate, frames=range(50), repeat=True)

# Show the live pie chart
t1 = td.Thread(target = plt.show)
t1.start()

