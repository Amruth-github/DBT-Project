import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from kafka import KafkaConsumer
import json
from time import sleep
from threading import *
import tkinter as tk
from matplotlib.figure import Figure
import numpy as np


consumer = KafkaConsumer('aggregate_count', group_id='plot', bootstrap_servers=["localhost:9092"], value_deserializer=lambda x: json.loads(x))
# Data to plot
labels = ["Traffic Hazard", "Crash Urgent", "Crash Service", "COLLISION"]
sizes = [0.1, 0.1, 0.1, 0.1]

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
colors = ["#0080ff", "#00cc00"]  # contrasting colors
ax1.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90, textprops = textprops)
ax1.axis("equal")
ax1.set_title("Percentage of Processing Types", color="white")
ax1.tick_params(axis='both', colors='white')  # white ticks
pie_canvas = FigureCanvasTkAgg(fig1, master=frame)
pie_canvas.get_tk_widget().pack(side=tk.LEFT, padx=10)

# create the bar graph
fig2 = Figure(figsize=(7,7), dpi=100, facecolor='black')
ax2 = fig2.add_subplot(111, facecolor='black')
x = np.arange(len(labels))
y = [20, 80, 30, 40]
ax2.bar(x, y, color=colors)
ax2.set_xticks(x)
ax2.set_xticklabels(labels)
ax2.set_title("Number of Processing Types", color="white")
ax2.tick_params(axis='both', colors='white')  # white ticks
bar_canvas = FigureCanvasTkAgg(fig2, master=frame)
bar_canvas.get_tk_widget().pack(side=tk.RIGHT)


def pie_graph_thread():   
	while True: 
		for message in consumer:
			aggregate_count:dict = message.value
			print(aggregate_count)
			sizes[0] += aggregate_count.get("Traffic Hazard", 0)
			sizes[1] += aggregate_count.get("Crash Urgent", 0)
			sizes[2] += aggregate_count.get("Crash Service", 0)
			sizes[3] += aggregate_count.get("COLLISION", 0)
			ax1.clear()
			ax1.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90, textprops = textprops)
			ax1.axis("equal")
			ax1.set_title("Percentage of Processing Types", color="white")
			ax1.tick_params(axis='both', colors='white')  # white ticks
			pie_canvas.draw()


t1 = Thread(target = pie_graph_thread)
t1.start()

root.mainloop()

	

