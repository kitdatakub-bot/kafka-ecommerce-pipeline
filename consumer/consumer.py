import json

from kafka import KafkaConsumer

import psycopg2

import time

 

# Kafka Consumer

consumer = KafkaConsumer(

    'orders',

    bootstrap_servers='localhost:port',  # ใช้ internal Docker network

    value_deserializer=lambda m: json.loads(m),

    auto_offset_reset='earliest',     # เริ่มอ่านจาก message แรก

    enable_auto_commit=True

)

 


 

# Postgres connection with retry

while True:

    try:

        conn = psycopg2.connect(

            host='localhost',             # container name ของ Postgres

            port ='port',

            database='ecommerce',

            user='',

            password=''

        )

        break

    except psycopg2.OperationalError:

        print("Postgres not ready, retrying in 3s...")

        time.sleep(3)

 

cur = conn.cursor()

 

# สร้าง table ถ้ายังไม่มี

cur.execute("""

CREATE TABLE IF NOT EXISTS orders (

    order_id INT PRIMARY KEY,

    product TEXT,

    quantity INT,

    price INT,

    timestamp BIGINT

);

""")

conn.commit()

 

for message in consumer:

    order = message.value

    try:

        cur.execute(

            """

            INSERT INTO orders (order_id, product, quantity, price, timestamp)

            VALUES (%s, %s, %s, %s, %s)

            ON CONFLICT(order_id) DO NOTHING

            """,

            (order['order_id'], order['product'], order['quantity'], order['price'], order['timestamp'])

        )

        conn.commit()

        print("Inserted:", order)

    except Exception as e:

        print("Error inserting order:", e)

        conn.rollback()
