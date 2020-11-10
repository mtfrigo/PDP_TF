import sys, getopt
import paho.mqtt.client as mqtt
from datetime import datetime
import json
import time
from threading import Thread
import psutil
import csv

counting = False

message_counter = 0
stop = False
start = False

PUBLISHERS = 0
ITERATION_DURATION = 0
WARMINGUP_DURATION = 0
THROUGHPUT = 0

class Client(object):
    hostname = '192.168.0.39'
    port = 1883
    clientId = ''
    durable_subscriber = False

    def __init__(self, hostname, port, clientId, durable_subscriber):
        self.hostname = hostname
        self.port = port
        self.clientId = clientId
        self.durable_subscriber = durable_subscriber
        self.topic = ''

        self.client = mqtt.Client(client_id=self.clientId, clean_session=self.durable_subscriber, userdata=None, protocol=mqtt.MQTTv31)

        # self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    # def on_connect(self, client, userdata, flags, rc):
        # print("[" + datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + "]: " + "ClientID: " + self.clientId + "; Connected to "+str(self.hostname)+" with result code " + str(rc))

    def on_message(self, client, userdata, message):

        if self.topic == 'master':
            global start
            global stop


            parsed_json = json.loads(message.payload.decode("utf-8", "ignore"))
            print(parsed_json)

            if parsed_json["status"] == "0":
                stop = True
            else:
                global PUBLISHERS, WARMINGUP_DURATION, ITERATION_DURATION, THROUGHPUT
                PUBLISHERS = int(parsed_json['pubs'])
                WARMINGUP_DURATION = int(parsed_json['warmup_duration'])
                THROUGHPUT = int(parsed_json['throughput'])
                ITERATION_DURATION = int(parsed_json['iteration_duration'])
                start = True
        else:
            global message_counter
            global counting
            
            if counting == True:
                message_counter = message_counter + 1
        

    # publishes message to MQTT broker
    def sendMessage(self, topic, msg):
        self.client.publish(topic=topic, payload=msg, qos=0, retain=False)

    def subscribe(self, topic):
        self.topic = topic
        self.client.subscribe(topic)

    def connect(self):
        self.client.connect(self.hostname, self.port, 60)

    # connects to MQTT Broker
    def start(self):
        #runs a thread in the background to call loop() automatically.
        #This frees up the main thread for other work that may be blocking.
        #This call also handles reconnecting to the broker.
        #Call loop_stop() to stop the background thread.
        self.client.loop_start()


def createSub(broker_ip, broker_port, topic, client_id):
    subscriber = Client(broker_ip, broker_port, client_id, False)
    subscriber.connect()
    subscriber.subscribe(topic)
    subscriber.start()

    while True:
        time.sleep(0.1)

def main(argv):
    global counting
    global message_counter
    # --- Broker 
    broker_ip = 'localhost'
    # broker_ip = 'localhost'
    broker_port = 1883
    topic = "System"
    master_topic = "master"

    print("Setting up..")

    client_id = 'sub_master'
    master_thread = Thread(target=createSub, args=(broker_ip, broker_port, master_topic, client_id))
    master_thread.name = 'MasterThread'
    master_thread.daemon = True
    master_thread.start()

    # --- Benchmark
    client_id = 'sub0'
    sub_thread = Thread(target=createSub, args=(broker_ip, broker_port, topic, client_id))
    sub_thread.name = client_id + 'Thread'
    sub_thread.daemon = True
    sub_thread.start()

    # --- Time to set up all clients
    time.sleep(5)

    print("Waiting for command to start..")

    while start == False:
        time.sleep(0.1)

    print("Starting server...")

    print("Warming up... ("+str(WARMINGUP_DURATION)+"s)")
    time.sleep(WARMINGUP_DURATION)

    print("Starting...")
    counting = True

    with open("sv_p"+str(PUBLISHERS)+"t"+str(THROUGHPUT)+".csv", 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        start_time = time.time()

        i = 1
        while stop == False:
            print(i, message_counter)

            time_now = str(time.time() -  start_time)
            cpu = str(psutil.cpu_percent())
            memory = str(psutil.virtual_memory().percent)

            spamwriter.writerow([i, time_now, cpu, memory])
            i = i + 1
            time.sleep(ITERATION_DURATION)

    print("Stopping server...")
    print("Total of messages received: " + str(message_counter))
    
        
if __name__ == "__main__":
   main(sys.argv[1:])