# 1.2.3 3:e konsumenten 
# Varje 24:e timme, aka vid 00:00 ska en daglig rapport skapas! 
# Antal ordrar, summa på försäljning, antal för vardera produkt såld under den dagen. 
# (sparas till en fil med dagens datum)! 
# Total_order_count, Total_sales, total_number_each_product_sold - Per day

import os
import json
from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime, timedelta

# Initialize Kafka consumer
consumer = KafkaConsumer(
    "ehandel_consumer3",
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_commit_interval_ms=3000)

current_date = datetime.today().date()
yesterday = current_date - timedelta(days=1)

now = datetime.now()
midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)

file_path = f"C:\\Users\\vijis\\python_ovningar\\Projekt_ehandel\\Sales_Update_Consumer3\\sales_{yesterday}.txt"            

def is_past_midnight():    
    return now.hour > 0 or (now.hour == 0 and now.minute == 0 and now.second == 0)

def consumer3_func():
    try:        
        order_count = 0
        total_sales = 0
        product_quantities = defaultdict(int)
        for message in consumer:
            order_time = message.value["order_time"] 
            order_time = datetime.strptime(order_time, '%m/%d/%Y-%H:%M:%S')    
            order_date = order_time.date()               
            
            if order_date == yesterday:
                order_count += 1
                os.system('cls')
                print('Consumer-3')
                print(f"Total_order_count {yesterday}: {order_count}") 
                #total sales for one day                    
                for product in message.value["order_details"]:
                    total_sales += product["price"] * product["quantity"]  
                
                    product_name = product["product_name"]
                    product_quantities[product_name] += product["quantity"]    
                print(f'Total sales {yesterday}: {total_sales} kr')            
            
            # sales_{yesterday}.txt report will create for before day in Sales_Update_Consumer3 
            os.system('cls')
            if is_past_midnight() and order_count > 0 and order_date == yesterday:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(f"Total_order_count{yesterday}: {order_count}\n")
                    f.write(f"Total sales{yesterday}: {total_sales}\n")
                    f.write(f"Products sold {yesterday}:\n")
                    for product_name, quantity_sold in product_quantities.items():
                        f.write(f"Product: {product_name}, Total Quantity Sold: {quantity_sold}\n")    
                        
                        print(f"Product: {product_name}, Total Quantity Sold: {quantity_sold}")
                    print(f"Data stored in todays sales report")
            else:
                print('Consumer-3')
                print('No sales recorded yesterday')

    except KeyboardInterrupt:
        print('Closing consumer')
                       
    finally:
        consumer.close()

consumer3_func()     
