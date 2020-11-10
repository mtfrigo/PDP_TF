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

PUBLISHERS = 500
MESSAGE_PER_PUB = 100

WARMUP_DURATION = 30

BENCHMARK_DURATION = 180
ITERATION_DURATION = 10

ITERATIONS = int(BENCHMARK_DURATION / ITERATION_DURATION ) + 1
THROUGHPUT = int((1 / MESSAGE_INTERVAL ) * PUBLISHERS)

def callback(ch, method, properties, body):
    global latency_sum
    global counting

    decoded_message = body.decode("utf-8", "ignore")

    if counting == True:
        latency = time.time() -  float(decoded_message)
        latency_sum =  latency_sum + latency

    # print(" [x] Received %r" % decoded_message)

def createSub(broker_ip, queue):
    connection = pika.BlockingConnection(pika.ConnectionParameters(broker_ip))
    channel = connection.channel()

    channel.queue_declare(queue=queue)

    channel.basic_consume(queue=queue,
                      auto_ack=True,
                      on_message_callback=callback)

    channel.start_consuming()

def createPubMaster(broker_ip, queue):
    global stop

    connection = pika.BlockingConnection(pika.ConnectionParameters(broker_ip))
    channel = connection.channel()

    channel.queue_declare(queue=queue)

    print("Initiliazing server...")

    message = '{"pubs": "'+str(PUBLISHERS)+'", "iteration_duration": "'+str(ITERATION_DURATION)+'", "warmup_duration": "'+str(WARMUP_DURATION)+'", "status": "'+str(1)+'", "throughput": "'+str(THROUGHPUT)+'"}'

    channel.basic_publish(exchange='',
                      routing_key=queue,
                      body=message)
    print('Mensagem enviada')
    while stop == False:
        time.sleep(0.1)

    print("Stopping server...")
    connection = pika.BlockingConnection(pika.ConnectionParameters(broker_ip))
    channel = connection.channel()

    message = '{"pubs": "'+str(PUBLISHERS)+'", "iteration_duration": "'+str(ITERATION_DURATION)+'", "warmup_duration": "'+str(WARMUP_DURATION)+'", "status": "'+str(0)+'", "throughput": "'+str(THROUGHPUT)+'"}'

    channel.basic_publish(exchange='',
                      routing_key=queue,
                      body=message)

    connection.close()


def createPub(broker_ip, queue):
    global message_counter
    global counting

    connection = pika.BlockingConnection(pika.ConnectionParameters(broker_ip))
    channel = connection.channel()

    channel.queue_declare(queue=queue)

    count = 1

    while start == False:
        time.sleep(0.1)

    while stop == False:
        message = str(time.time())

        channel.basic_publish(exchange='',
                      routing_key=queue,
                      body=message)

        if counting == True:
            message_counter = message_counter + 1
        count = count + 1

        time.sleep(MESSAGE_INTERVAL)


def main(argv):
    global stop
    global start
    global counting

    # --- Broker 
    # broker_ip = 'ec2-54-208-177-151.compute-1.amazonaws.com'
    broker_ip = '192.168.0.39'
    # broker_ip = 'localhost'
    broker_queue = 'test'

    client_id = 'pub_master'
    # client_id = 'pub1'
    pub_thread = Thread(target=createPubMaster, args=(broker_ip, 'master'))
    pub_thread.name = client_id + 'Thread'
    pub_thread.daemon = True
    pub_thread.start()

    client_id = 'sub1'
    sub_thread = Thread(target=createSub, args=(broker_ip, broker_queue))
    sub_thread.name = client_id + 'Thread'
    sub_thread.daemon = True
    sub_thread.start()

    for x in range(PUBLISHERS):
        client_id = 'pub' + str(x)
        # client_id = 'pub1'
        pub_thread = Thread(target=createPub, args=(broker_ip, broker_queue))
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
    time.sleep(10)

if __name__ == "__main__":
   main(sys.argv[1:])