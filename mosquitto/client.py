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
start = False
counting = False

# MESSAGE_INTERVAL = 0.1
# MESSAGE_INTERVAL = 0.5
MESSAGE_INTERVAL = 1
# PUBLISHERS = 329

PUBLISHERS = 500
MESSAGE_PER_PUB = 100

WARMUP_DURATION = 30

BENCHMARK_DURATION = 180
ITERATION_DURATION = 10

ITERATIONS = int(BENCHMARK_DURATION / ITERATION_DURATION ) + 1
THROUGHPUT = int((1 / MESSAGE_INTERVAL ) * PUBLISHERS)

# ITERATIONS = 6

class Client(object):
    hostname = 'localhost'
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
        global counting

        # --- Parse message to JSON
        # parsed_json = json.loads(message.payload.decode("utf-8", "ignore"))
        decoded_message = message.payload.decode("utf-8", "ignore")

        if counting == True:
            # latency = time.time() -  float(parsed_json["sent"])
            latency = time.time() -  float(decoded_message)
            latency_sum =  latency_sum + latency
        

    # publishes message to MQTT broker
    def sendMessage(self, topic, msg):
        # print(topic, msg)
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

def createPubMaster(broker_ip, broker_port, topic, client_id):
    global stop
    publisher = Client(broker_ip, broker_port, client_id, False)
    publisher.connect()
    publisher.start()

    print("Initiliazing server...")

    message = '{"pubs": "'+str(PUBLISHERS)+'", "iteration_duration": "'+str(ITERATION_DURATION)+'", "warmup_duration": "'+str(WARMUP_DURATION)+'", "status": "'+str(1)+'", "throughput": "'+str(THROUGHPUT)+'"}'

    publisher.sendMessage(topic, message)

    while stop == False:
        time.sleep(1)

    print("Stopping server...")
    message = '{"pubs": "'+str(PUBLISHERS)+'", "iteration_duration": "'+str(ITERATION_DURATION)+'", "warmup_duration": "'+str(WARMUP_DURATION)+'", "status": "'+str(0)+'", "throughput": "'+str(THROUGHPUT)+'"}'
    publisher.sendMessage(topic, message)

    

def createPub(broker_ip, broker_port, topic, client_id):
    global message_counter
    global counting

    publisher = Client(broker_ip, broker_port, client_id, False)
    publisher.connect()
    publisher.start()

    count = 1

    while start == False:
        time.sleep(0.1)

    while True:
        # message = '{"id": "'+client_id+'", "count": '+str(count)+', sent": '+str(datetime.now())+'}'
        message = str(time.time())

        publisher.sendMessage(topic, message)

        if counting == True:
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
    broker_ip = 'ec2-54-208-177-151.compute-1.amazonaws.com'
    # broker_ip = 'localhost'
    broker_port = 1883
    topic = "System"
    topic_master = "master"

    # --- Benchmarnk
    # total_messages = int(PUBLISHERS * MESSAGE_PER_PUB)
    global stop
    global start
    global counting

    client_id = 'pub_master'
    pub_thread = Thread(target=createPubMaster, args=(broker_ip, broker_port, topic_master, client_id))
    pub_thread.name = 'MasterThread'
    pub_thread.daemon = True
    pub_thread.start()

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

    start = True
    
    # --- Time to set up all clients
    print("Warming up... ("+str(WARMUP_DURATION)+"s)")
    time.sleep(WARMUP_DURATION)

    print("Starting...")
    counting = True

    with open("cl_p"+str(PUBLISHERS)+"t"+str(THROUGHPUT)+".csv", 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)

        start_time = time.time()

        i = 0
        while i <= ITERATIONS:
            print(i+1, latency_sum, message_counter)
            time_now = str(time.time() -  start_time)
            cpu = str(psutil.cpu_percent())
            memory = str(psutil.virtual_memory().percent)

            avg_latency = 0
            if message_counter > 0 :
                avg_latency = str(latency_sum/message_counter*1.0)

            spamwriter.writerow([i+1, time_now, cpu, memory, avg_latency])
            
            i = i + 1
            time.sleep(ITERATION_DURATION)

    stop = True
    time.sleep(5)
        
if __name__ == "__main__":
   main(sys.argv[1:])