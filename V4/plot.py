import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from kafka import KafkaConsumer
import json
from time import sleep
from threading import *
import tkinter as tk
from matplotlib.figure import Figure
import numpy as np


consumer_stream = KafkaConsumer('aggregate_count', group_id='plot', bootstrap_servers=["localhost:9092"], value_deserializer=lambda x: json.loads(x))
consumer_batch = KafkaConsumer('aggregate_count_batch', bootstrap_servers=["localhost:9092"], value_deserializer=lambda x: json.loads(x))
# Data to plot
labels = ["Traffic Hazard", "Crash Urgent", "Crash Service", "COLLISION"]
sizes_stream = [0.1, 0.1, 0.1, 0.1]
sizes_batch = [0.1, 0.1, 0.1, 0.1]
colors = ["#0080ff", "#00cc00"]  # contrasting colors

root = tk.Tk()
root.title("Processing")
root.configure(bg="black")

textprops = {"color" : "white", "bbox" : {"facecolor" : "black", "edgecolor" : "brown", "pad" : 5}}
# create a frame to hold the graphs
frame = tk.Frame(root, bg="black")
frame.pack(side=tk.TOP, pady=20)

# create the pie graph
fig1 = Figure(figsize=(8, 8), dpi=100, facecolor='black')
ax1 = fig1.add_subplot(111, facecolor='black')
ax1.pie(sizes_stream, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90, textprops = textprops)
ax1.axis("equal")
ax1.set_title("Stream Processing", color="white")
ax1.tick_params(axis='both', colors='white')  # white ticks
pie_canvas_stream = FigureCanvasTkAgg(fig1, master=frame)
pie_canvas_stream.get_tk_widget().pack(side=tk.LEFT, padx=10)

fig2 = Figure(figsize=(8, 8), dpi=100, facecolor='black')
ax2 = fig2.add_subplot(111, facecolor='black')
ax2.pie(sizes_batch, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90, textprops = textprops)
ax2.axis("equal")
ax2.set_title("Batch Processing", color="white")
ax2.tick_params(axis='both', colors='white')  # white ticks
pie_canvas_batch = FigureCanvasTkAgg(fig2, master=frame)
pie_canvas_batch.get_tk_widget().pack(side=tk.RIGHT, padx=10)



def pie_graph_thread_stream():   
	while True: 
		for message in consumer_stream:
			aggregate_count:dict = message.value
			print(aggregate_count)
			sizes_stream[0] += aggregate_count.get("Traffic Hazard", 0)
			sizes_stream[1] += aggregate_count.get("Crash Urgent", 0)
			sizes_stream[2] += aggregate_count.get("Crash Service", 0)
			sizes_stream[3] += aggregate_count.get("COLLISION", 0)
			ax1.clear()
			ax1.pie(sizes_stream, labels=labels, colors=colors, autopct='%1.2f%%', startangle=90, textprops = textprops)
			ax1.axis("equal")
			ax1.set_title("Stream Processing", color="white")
			ax1.tick_params(axis='both', colors='white')  # white ticks
			pie_canvas_stream.draw()
			
def pie_graph_thread_batch():
	while True:
		for message in consumer_batch:
			aggregate_count = message.value
			sizes_batch[0] += aggregate_count.get("Traffic Hazard", 0)
			sizes_batch[1] += aggregate_count.get("Crash Urgent", 0)
			sizes_batch[2] += aggregate_count.get("Crash Service", 0)
			sizes_batch[3] += aggregate_count.get("COLLISION", 0)
			ax2.clear()
			ax2.pie(sizes_batch, labels=labels, colors=colors, autopct='%1.2f%%', startangle=90, textprops = textprops)
			ax2.axis("equal")
			ax2.set_title("Batch Processing", color="white")
			ax2.tick_params(axis='both', colors='white')  # white ticks
			pie_canvas_batch.draw()


t1 = Thread(target = pie_graph_thread_stream)
t1.start()

t2 = Thread(target = pie_graph_thread_batch)
t2.start()

root.mainloop()

	

