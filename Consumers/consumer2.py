# 1.2.2 2:a konsumenten 
# Vi vill veta lite om försäljning!! 
# Vi vill ha dagens totala försäljning hittils och 
# den senaste timmens försäljning! 

import os
import json
from kafka import KafkaConsumer
from datetime import datetime, timedelta

# Initialize Kafka consumer
consumer = KafkaConsumer(
    "ehandel_consumer3",
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_commit_interval_ms=3000)

def todays_sales_report():
    total_sales = 0
    total_last_hour_sales = 0
    try:    
        for message in consumer:        
            current_date = datetime.now().strftime("%m/%d/%Y")        
            order_time = message.value["order_time"]
            order_time = datetime.strptime(order_time, '%m/%d/%Y-%H:%M:%S')       
            order_date = order_time.strftime("%m/%d/%Y")                    
                    
            if current_date == order_date:        
                #total sales for one day                    
                for product in message.value["order_details"]:
                    total_sales += product["price"] * product["quantity"]  

                #last one hour sales report
                current_time = datetime.now()
                one_hour_ago = current_time - timedelta(hours = 1)
                if one_hour_ago <= order_time <= current_time:
                    for product in message.value["order_details"]:
                        total_last_hour_sales += product["price"] * product["quantity"]  

            os.system('cls')
            print('Consumer-2')
            print(f'Total sales for {current_date}:', total_sales, 'kr')
            print(f'Total last one hour sales for{current_date}:', total_last_hour_sales, 'kr')
                    
    except KeyboardInterrupt:        
        print('Closing consumer')

    finally:
        consumer.close()


todays_sales_report()