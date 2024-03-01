# 1.2.4 4:e konsumenten 
# För varje försäljning så ska saldo för produkten uppdateras! 
# Vi vill alltså ha ett lagersaldo tillgängligt att kunna ta ifrån 
# när en försäljning av en produkt sker. 
# (Tänk på detta vid simulering av försäljningen ovan!) 

import os
import json
import sqlite3
from kafka import KafkaConsumer
from collections import defaultdict

# Initialize Kafka consumer
consumer = KafkaConsumer(
    "ehandel_consumer3",
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_commit_interval_ms=3000
)

db_path = "C:\\Users\\vijis\\python_ovningar\\Projekt_ehandel\\sqlite_db\\ehandelDB.db"  
file_path = "C:\\Users\\vijis\\python_ovningar\\Projekt_ehandel\\Tabular_data\\stock_update_Consumer4.txt"

product_quantities = defaultdict(int)  
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

def consumer4(consumer, conn, cursor):
    processed_products = set()
    try:
        for message in consumer:        
            for product in message.value["order_details"]:
                product_id = product["product_id"]
                product_name = product["product_name"]
                quantity_sold = product["quantity"]
                print(product_id, product_name, quantity_sold)
                try:                        
                    if product_id not in processed_products:
                        cursor.execute("SELECT saldo FROM products WHERE productid = ?", (product_id,))
                        saldo = cursor.fetchone()[0] 
                        print('\n')
                        print(f"Product ID: {product_id}, Product Name: {product_name}, Saldo: {saldo}")

                        if saldo is not None:                            
                            cursor.execute("UPDATE products SET saldo = saldo - ? WHERE productid = ?", (quantity_sold, product_id))
                            conn.commit()
                            print(f"Order created for Product ID {product_id}: {quantity_sold} products sold")

                            # Fetch the updated saldo from the database                            
                            cursor.execute("SELECT saldo FROM products WHERE productid = ?", (product_id,))
                            saldo_after_update = cursor.fetchone()[0] 
                            print(f"Saldo after update for Product ID {product_id}: {saldo_after_update}")

                        with open(file_path, 'a', encoding='utf-8') as f:
                            f.write(f"Product ID: {product_id}, Product Name: {product_name}, Saldo: {saldo}\n")
                            f.write(f"Order created for Product ID {product_id}: {quantity_sold} products sold\n")
                            f.write(f"Saldo after update for Product ID {product_id}: {saldo_after_update}\n")

                            last_processed_product_id = product_id  
                            
                except Exception as e:
                    print(f"Error updating stock for Product ID {product_id}: {str(e)}")
                    conn.rollback()  # Rollback in case of error

        if last_processed_product_id is not None:
            processed_products.add(last_processed_product_id)  # Add the last processed product ID

    except KeyboardInterrupt:
        print('Closing consumer') 

    finally:
        cursor.close()
        conn.close()
        consumer.close()


consumer4(consumer, conn, cursor)
