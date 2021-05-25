from flask import Flask, request, jsonify
from flask_restful import Resource, Api
import boto3
import pulsar
import json
from multiprocessing import Process, Manager

app = Flask(__name__)
api = Api(app)

dynamodb = boto3.resource('dynamodb', endpoint_url = "http://localhost:4566")
table = dynamodb.Table('Popo.user')

client = pulsar.Client('pulsar://localhost:6650')
client.close()
client = pulsar.Client('pulsar://localhost:6650')
consumer = client.subscribe('Popo.list_of_book', 'my-subscription')
producer = client.create_producer('Popo.category_book')

books = {}

def list_of_books(mission, data):
    print("Data: ", data)
    global books
    if mission == 1:
        books = data
    else:
        books = {}
    print(books)

def receive_message():
    print("Run consumer!")
    while True:
        msg = consumer.receive()
        print("Received list of books")
        try:
            consumer.acknowledge(msg)
            try:
                json_data = json.loads(msg.data().decode('utf8'))
                print("Convert message to json")
                dict_data = json.loads(json.dumps(json_data))
                print("Convert json to dictionary")
                print("Done!")
                list_of_books(1, dict_data)
            except:
                print("Can't convert")
        except:
            consumer.negative_acknowledge(msg)
    

class User(Resource):
    global books
    def post(self):
        #Receive and convert data
        request_data = request.get_json()
        new_data = {"category" : request_data["category"]}
        encode_new_data = json.dumps(new_data, indent=2).encode('utf-8')
        print("Received request!")

        #Send request to Book server
        #client = pulsar.Client('pulsar://localhost:6650')
        
        producer.send(encode_new_data)
        #client.close()
        print("Sent message!")

        #Check if User server receive list of book
        rs = books["books"]
        print("Result: ", rs)
        while True:
            if len(rs) > 0:
                break
        print("Received list")

        #Clear list_of_book and return result
        list_of_books(0, [])
        return jsonify(rs)

def run_web():
    app.run(port = 2901)

api.add_resource(User, '/user')

if __name__ == '__main__':
    with Manager() as manager:
        p_web = Process(target=run_web, args=())
        p_pulsar = Process(target=receive_message, args=())

        p_web.start()
        p_pulsar.start()

        p_web.join()
        p_pulsar.join()



