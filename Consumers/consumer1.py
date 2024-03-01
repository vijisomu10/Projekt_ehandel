# 1.2.1 1:a konsumenten 
# Vi har en konsument som vill veta hur många ordrar 
# vi har fått in från kl 00.00 till nuvarande tidpunkt. 
# (Brytpunkt varje dag kl 00.00 alltså) 

import os
import json
from datetime import datetime
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'ehandel_consumer3',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_commit_interval_ms=3000 )

def consumer1_func():
    try:        
        order_count = 0
        for message in consumer:
            order_time = message.value["order_time"] 
            order_time = datetime.strptime(order_time, '%m/%d/%Y-%H:%M:%S')    
            order_date = order_time.date()    

            current_date = datetime.today().date()
            
            if order_date == current_date:
                order_count += 1
            os.system('cls')
            print('Consumer-1')
            print(f'Total orders today {current_date} from 00:00 till now')    
            print(f"Orders added {order_count}")                     
            
    except KeyboardInterrupt:
        print('Closing consumer')

    finally:
        consumer.close()

consumer1_func()
