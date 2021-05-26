import boto3
import pulsar
import json
import time

client = pulsar.Client('pulsar://localhost:6650')
consumer = client.subscribe('Popo.list_of_book', 'my-subscription')

start_time = time.time()
books = {}

def list_of_books(mission, data):
    print("-----Starting search books:")
    print("Data: ", data)
    global books
    if mission == 1:
        books["books"].append = data
    else:
        books = {}
    print("List of books:", books)

def convert_msg(msg):
    print("-----Starting covert msg:")
    json_data = json.loads(msg.data().decode('utf8'))
    print("Convert message to json")
    dict_data = json.loads(json.dumps(json_data))
    print("Convert json to dictionary")
    print("Done!")
    return dict_data

def send_message(encode_new_data):
    print("-----2. Starting send message:")
    client_ = pulsar.Client('pulsar://localhost:6650')
    producer = client_.create_producer('Popo.category_book')
    producer.send(encode_new_data)
    client_.close()
    print("Sent")

def receive_message():    
    print("---------------Run consumer!")
    while True:
        #if time.time() - start_time % 10 == 0:
        print("Consumer is runing")
        try: 
            msg = consumer.receive()
            print("-----3. Received list of books")
            try:
                consumer.acknowledge(msg)
                try:
                    dict_data = convert_msg(msg)
                    list_of_books(1, dict_data)
                except:
                    print("Can't convert")
            except:
                consumer.negative_acknowledge(msg)
        except Exception as e:
            print(e)
    client.close()

receive_message()
