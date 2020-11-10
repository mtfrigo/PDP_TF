import sys, getopt
import paho.mqtt.client as mqtt
from datetime import datetime
import json
import time
from threading import Thread
import psutil
import csv
import pika

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

def callback(ch, method, properties, body):
    global message_counter
    global counting

    if counting == True:
        message_counter = message_counter + 1


def master_cb(ch, method, properties, body):
    global start
    global stop

    parsed_json = json.loads(body.decode("utf-8", "ignore"))
    print(parsed_json)

    if parsed_json["status"] == "0":
        stop = True
    else:
        global PUBLISHERS, WARMUP_DURATION, ITERATION_DURATION, THROUGHPUT
        PUBLISHERS = int(parsed_json['pubs'])
        WARMUP_DURATION = int(parsed_json['warmup_duration'])
        THROUGHPUT = int(parsed_json['throughput'])
        ITERATION_DURATION = int(parsed_json['iteration_duration'])
        start = True

def createSubMaster(broker_ip, queue):
    connection = pika.BlockingConnection(pika.ConnectionParameters(broker_ip))
    channel = connection.channel()

    channel.queue_declare(queue=queue)

    channel.basic_consume(queue=queue,
                      auto_ack=True,
                      on_message_callback=master_cb)

    channel.start_consuming()

def createSub(broker_ip, queue):
    connection = pika.BlockingConnection(pika.ConnectionParameters(broker_ip))
    channel = connection.channel()

    channel.queue_declare(queue=queue)

    channel.basic_consume(queue=queue,
                      auto_ack=True,
                      on_message_callback=callback)

    channel.start_consuming()

def main(argv):
    global stop
    global start
    global counting

    # --- Broker 
    # broker_ip = 'ec2-54-208-177-151.compute-1.amazonaws.com'
    broker_ip = 'localhost'
    broker_queue = 'test'

    sub_thread = Thread(target=createSubMaster, args=(broker_ip, 'master'))
    sub_thread.name = 'SubMasterThread'
    sub_thread.daemon = True
    sub_thread.start()

    sub_thread = Thread(target=createSub, args=(broker_ip, broker_queue))
    sub_thread.name = 'SubThread'
    sub_thread.daemon = True
    sub_thread.start()

    # --- Time to set up all clients
    time.sleep(5)

    print("Waiting for command to start..")

    while start == False:
        time.sleep(0.1)

    # --- Time to set up all clients
    print("Warming up... ("+str(WARMUP_DURATION)+"s)")
    time.sleep(WARMUP_DURATION)

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

            spamwriter.writerow([i+1, time_now, cpu, memory])
            i = i + 1
            time.sleep(ITERATION_DURATION)

    print("Stopping server...")
    print("Total of messages received: " + str(message_counter))

if __name__ == "__main__":
   main(sys.argv[1:])