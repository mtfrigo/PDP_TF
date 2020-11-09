import sys, getopt
import paho.mqtt.client as mqtt
from datetime import datetime
import json
import time
from threading import Thread
import psutil
import csv

latency_sum = 0
message_counter = 0
stop = False

MESSAGE_INTERVAL = 1
PUBLISHERS = 500
MESSAGE_PER_PUB = 100
BENCHMARK_DURATION = 330
ITERATIONS = 10

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

        self.client = mqtt.Client(client_id=self.clientId, clean_session=self.durable_subscriber, userdata=None, protocol=mqtt.MQTTv31)

        # self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    # def on_connect(self, client, userdata, flags, rc):
        # print("[" + datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + "]: " + "ClientID: " + self.clientId + "; Connected to "+str(self.hostname)+" with result code " + str(rc))

    def on_message(self, client, userdata, message):
        global latency_sum
        global message_counter


        # --- Parse message to JSON
        parsed_json = json.loads(message.payload.decode("utf-8", "ignore"))

        latency = time.time() -  float(parsed_json["sent"])

        # print(parsed_json["id"])
        # print(parsed_json["count"])
        latency_sum += (latency)
        

    # publishes message to MQTT broker
    def sendMessage(self, topic, msg):
        self.client.publish(topic=topic, payload=msg, qos=0, retain=False)

    def subscribe(self, topic):
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

def createPub(broker_ip, broker_port, topic, client_id):
    global message_counter
    publisher = Client(broker_ip, broker_port, client_id, False)
    publisher.connect()
    publisher.start()

    count = 1

    while True:
        # message = '{"id": "'+client_id+'", "count": '+str(count)+', sent": '+str(datetime.now())+'}'
        message = '{"id": "'+client_id+'", "count": '+str(count)+', "sent": "'+str(time.time())+'"}'

        publisher.sendMessage(topic, message)
        message_counter = message_counter + 1
        count = count + 1

        time.sleep(MESSAGE_INTERVAL)

def createSub(broker_ip, broker_port, topic, client_id):
    subscriber = Client(broker_ip, broker_port, client_id, False)
    subscriber.connect()
    subscriber.subscribe(topic)
    subscriber.start()

    while True:
        time.sleep(1)

def createTimer():
    global stop
    time.sleep(BENCHMARK_DURATION)
    stop = True

def main(argv):
    # --- Broker 
    broker_ip = '192.168.0.39'
    # broker_ip = 'localhost'
    broker_port = 1883
    topic = "System"

    # --- Benchmarnk
    # total_messages = int(PUBLISHERS * (MESSAGE_PER_PUB / MESSAGE_INTERVAL))
    total_messages = int(PUBLISHERS * MESSAGE_PER_PUB)
    global stop


    client_id = 'sub0'
    sub_thread = Thread(target=createSub, args=(broker_ip, broker_port, topic, client_id))
    sub_thread.name = client_id + 'Thread'
    sub_thread.daemon = True
    sub_thread.start()
    
    for x in range(PUBLISHERS):
        client_id = 'pub' + str(x)
        pub_thread = Thread(target=createPub, args=(broker_ip, broker_port, topic, client_id))
        pub_thread.name = client_id + 'Thread'
        pub_thread.daemon = True
        pub_thread.start()

    

    # --- Time to set up all clients
    time.sleep(5)

    with open("p"+str(PUBLISHERS)+".csv", 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)


        # --- Initiate global timer
        timer_thread = Thread(target=createTimer, args=())
        timer_thread.name = 'TimerThread'
        timer_thread.daemon = True
        timer_thread.start()

        start_time = time.time()

        i = 0
        while i < ITERATIONS:
            if latency_sum > 0 and message_counter > 0:
                spamwriter.writerow([str(time.time() -  start_time), str(psutil.cpu_percent()), psutil.virtual_memory().percent, str(latency_sum/message_counter*1.0)])
            
            i = i + 1
            print(i, ITERATIONS)
            time.sleep(30)

        
if __name__ == "__main__":
   main(sys.argv[1:])