import boto3
import pulsar
import json

dynamodb = boto3.resource('dynamodb', endpoint_url = "http://localhost:4566")
table = dynamodb.Table('Popo.books')

client = pulsar.Client('pulsar://localhost:6650')
consumer = client.subscribe('Popo.category_book', 'my-subscription')

def list_of_books(category):
    print("---Starting search books:")
    books = []
    scanResponse = table.scan(TableName='Popo.books')
    items = scanResponse["Items"]
    for item in items:
        print(item)
        if item["category"] == category:
            books.append(item["name"])
    print(books)
    return books

while True:
    msg = consumer.receive()
    print("Received category books")
    try:        
        consumer.acknowledge(msg)
        try:
            json_data = json.loads(msg.data().decode('utf8'))
            print("Convert message to json")
            dict_data = json.loads(json.dumps(json_data))
            print("Convert json to dictionary")
            print("Done!")
        except:
            print("Can't convert!")
        
        try:
            category = dict_data["category"]
            print(category)
            books = list_of_books(category)
            new_data = {"books": books}
            print(new_data)
            encode_new_data = json.dumps(new_data, indent=2).encode('utf-8')
            
            client_ = pulsar.Client('pulsar://localhost:6650')
            producer = client_.create_producer('Popo.list_of_book')
            producer.send(encode_new_data)
            client_.close()
            print("Sent")
        except:
            print("Can't send!")
    except:
        consumer.negative_acknowledge(msg)
client.close()