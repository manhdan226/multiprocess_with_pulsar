from flask import Flask, request, jsonify
from flask_restful import Resource, Api
import boto3
import pulsar
import json
from multiprocessing import Process, Manager
import time

app = Flask(__name__)
api = Api(app)

# dynamodb = boto3.resource('dynamodb', endpoint_url = "http://localhost:4566")
# table = dynamodb.Table('Popo.user')
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
        if (time.time() - start_time) % 2 == 0:
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
            print("Can't receive")
            print(e)
    client.close()

class User(Resource):
    global books
    def post(self):
        #Receive and convert data
        try:
            request_data = request.get_json()
            print("-----1. Received request!")
            try:
                new_data = {"category" : request_data["category"]}
                encode_new_data = json.dumps(new_data, indent=2).encode('utf-8')
            #Send request to Book server
                send_message(encode_new_data)
            except:
                print("Can't send")
                return "Can't send"

            #Check if User server receive list of book
            try:
                rs = books
                print("Result: ", rs)
                while True:
                    if len(rs["books"]) > 0:
                        break
                print("Received list")
                list_of_books(0, {})
                return "Sucessful"
            except:
                print("Can't receive")
                return "Can't receive"
            #Clear list_of_book and return result
            
        except:
            return "Can't process"

def run_web():
    app.run(debug = True, port = 2901)

api.add_resource(User, '/user')

if __name__ == '__main__':
    #with Manager() as manager:
    p_web = Process(target=run_web, args=())
    p_pulsar = Process(target=receive_message, args=())
    
    p_web.start()
    p_pulsar.start()
    
    p_pulsar.join()
    p_web.join()

    #while True:
    



