from kafka import KafkaProducer
from Producer.producer_DBsetup import *
from sqlite3 import Cursor
import random
import json
import datetime
import time
import os

PRODUCED_ORDERS = 'C:\\Users\\vijis\\python_ovningar\\Projekt_ehandel\\Tabular_data\\produced_orders.txt'
orders_created_now = 'C:\\Users\\vijis\\python_ovningar\\Projekt_ehandel\\Tabular_data\\order_created_now.txt'
last_order_id = 'C:\\Users\\vijis\\python_ovningar\\Projekt_ehandel\\Tabular_data\\last_order_id.txt'
CUSTOMER_ID = [id for id in range(1000,10000)]
# Parameters for a normal distribution
MU = 1 # dictates the avrage amount of orders sent for each second
SIGMA = 0.5 # dictates the variaton around MU, small number implies more likely MU orders, bigger number the oposite


def random_products(
        nr_of_prod_to_random:int, 
        nr_of_prod_in_db:int, 
        cursor:Cursor
        ) -> list[dict]:
    
    list_of_random_products = []

    for _ in range(nr_of_prod_to_random):
        rand_prod_id = random.randint(1,nr_of_prod_in_db)
        prod = cursor.execute(f"SELECT * FROM products WHERE productid={rand_prod_id}").fetchone()

        quantity = random.randint(1,10)

        # Change here if you need to control if the inventory runs out
        if quantity > prod[5]: quantity = 0

        random_product = {"product_id": prod[0],
                          "product_name": prod[1],
                          "product_type": prod[2],
                          "price_type": prod[3],
                          "price":prod[4],
                          "quantity":quantity}
        
        list_of_random_products.append(random_product)

    return list_of_random_products

def get_last_order_id():
    if os.path.exists(last_order_id):
        with open(last_order_id, "r") as file:
            return int(file.read().strip())
    return 100000

def save_last_order_id(order_id):
    with open(last_order_id, "w") as file:
        file.write(str(order_id))


def random_order(order_id:int, num_of_prod:int, cursor:Cursor) -> dict:
    customer_id = random.choice(CUSTOMER_ID)
    products = random_products(random.randint(1,5), num_of_prod, cursor)
    order_time = datetime.datetime.now().strftime("%m/%d/%Y-%H:%M:%S")

    new_order = dict(order_id=order_id,
                     customer_id=customer_id,
                     order_details=products,
                     order_time=order_time) 
    return new_order


if __name__ == "__main__":    
    order_id = get_last_order_id()
    cursor = producer_db_setup()

    products = cursor.execute("SELECT * FROM products").fetchall()
    number_of_products_in_database = len(products)

    producer = KafkaProducer(
                bootstrap_servers='localhost:9092', 
                value_serializer=lambda v: json.dumps(v).encode(encoding='utf-8')
                )
    try:
        while True:
            time.sleep(5)
            random_whole_numb_gaussian = int(random.gauss(mu=MU, sigma=SIGMA))
            for _ in range(random_whole_numb_gaussian):
                order_id += 1
                new_order = random_order(order_id, number_of_products_in_database, cursor)
                producer.send("ehandel_consumer3", new_order)
                print(f'{new_order} \n')

                #only current order
                with open(PRODUCED_ORDERS, 'w') as file:    
                    file.write(json.dumps(new_order, indent=4) + '\n')      

                #all orders stored here
                with open(orders_created_now, 'a') as file:    
                    file.write(json.dumps(new_order, indent=4) + '\n')                  
            
            save_last_order_id(order_id)            

            producer.flush()

    except KeyboardInterrupt:
        print('Shutting down!')
    
    finally:
        producer.flush()
        producer.close()
        cursor.close()
