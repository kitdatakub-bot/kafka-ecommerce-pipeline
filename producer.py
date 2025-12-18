import json, time, random

from kafka import KafkaProducer

 

producer = KafkaProducer(

    bootstrap_servers='localhost:_____',

    value_serializer=lambda v: json.dumps(v).encode('utf-8')

)

 

products = ['laptop', 'phone', 'headphones', 'keyboard']

 

while True:

    order = {

        "order_id": random.randint(1000, 9999),

        "product": random.choice(products),

        "quantity": random.randint(1,5),

        "price": random.randint(100, 1000),

        "timestamp": int(time.time())

    }

    producer.send('orders', order)

    print("Sent:", order)

    time.sleep(random.randint(1, 5))

 
