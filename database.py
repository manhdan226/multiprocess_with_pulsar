import boto3 

dynamodb = boto3.resource('dynamodb', endpoint_url = "http://localhost:4566")

def show_all_table(table):
    scanResponse = table.scan(TableName='tracking_id')
    items = scanResponse['Items']
    print('Scan all items in datatable: ')
    for item in items:
        print(item)

def create_table(name, key):
    table = dynamodb.create_table(
    TableName=name,
    KeySchema=[
        {
            'AttributeName': key,
            'KeyType': 'HASH'
        }
         
    ],
    AttributeDefinitions=[
             {
            'AttributeName': 'id',
            'AttributeType': 'S'
        } 
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
    )

    table.meta.client.get_waiter('table_exists').wait(TableName=name)
    print("Done!")
    

def delete_table(table):
    table.delete()

if __name__ == "__main__":
    #table = dynamodb.Table('tracking_data')
    create_table("Popo.user", "id")
    user = {"id": "1", "username": "Dan", "password": "DanD"}
    table1 = dynamodb.Table('Popo_user')
    table1.put_item(Item=user)

    create_table("Popo.books", "id")
    books = [["1","Cha giàu, cha nghèo","Kinh tế"],
            ["2","Từ tốt đến vĩ đại","Kinh tế"],
            ["3","Chí Phèo","Văn học"],
            ["4","Bình Ngô đại cáo","Lịch sử"]]

    table2 = dynamodb.Table('Popo_books')
    for book in books:
        data = {"id": book[0], "name": book[1], "category": book[2]}
        table2.put_item(Item=data)
        

    #show_all_table(table)
    #delete_table(table)
        