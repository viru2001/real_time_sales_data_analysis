from google.cloud import bigquery
import random
from datetime import datetime
import uuid
from google.cloud import pubsub_v1
import json
import time

sleep_time=0.1
project_id = 'sales-analysis-413205'
dataset_id = 'sales_data'

client = bigquery.Client(project=project_id)

# Function to fetch data from BigQuery and create dictionaries
def fetch_and_create_dicts(table_name, columns):
    query = f"SELECT {', '.join(columns)} FROM `{project_id}.{dataset_id}.{table_name}`"
    query_job = client.query(query)
    results = query_job.result()

    data_dicts = []
    for row in results:
        data_dict = dict(zip(columns, row))
        data_dicts.append(data_dict)

    return data_dicts

table_1_name = 'products'
table_2_name = 'stores'

products_table_columns = ['product_id', 'name', 'category', 'price', 'supplier_id']
stores_table_columns = ['store_id','location','size','manager']
product_data = fetch_and_create_dicts(table_1_name,products_table_columns)
stores_data = fetch_and_create_dicts(table_2_name,stores_table_columns)


def generate_uuid():
    random_uuid = uuid.uuid4()
    uuid_str = f"T-{random_uuid}"
    return uuid_str

def random_datetime():
    year = 2023
    month = random.randint(11, 11)
    day = random.randint(1, 30)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    dt_object = datetime(year, month, day, hour, minute, second)
    return dt_object

def generate_mock_data(product_data,stores_data):
    product = product_data[random.randint(0,9)]
    store = random.sample(stores_data,1)[0]
    quantity = random.randint(1, 5)
    sales_transaction= {
        "transaction_id": generate_uuid(),
        "product_id": product['product_id'],
        "timestamp": random_datetime().strftime("%Y-%m-%d %H:%M:%S"),
        "quantity": quantity,
        "unit_price": product['price'],
        "store_id": store['store_id']
    }
    inventory_updates = { 
        "product_id": product['product_id'], 
        "timestamp": random_datetime().strftime("%Y-%m-%d %H:%M:%S"), 
        "quantity_change": -quantity,
        "store_id": store['store_id'] 
    }
    return sales_transaction,inventory_updates

# Callback function to handle the publishing results.
def callback(future):
    try:
        message_id = future.result()
        # print(f"Published message with ID: {message_id}")
    except Exception as e:
        print(f"Error publishing message: {e}")

# Initialize the Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()

# Project and Topic details
project_id = "sales-analysis-413205"
topic_1_name = "sales_transactions"
topic_2_name = "inventory_updates"
topic_1_path = publisher.topic_path(project_id, topic_1_name)
topic_2_path = publisher.topic_path(project_id, topic_2_name)


print("---------------- Actual Data starts --------------------------------------")
while True:
    sales_transaction_mock_data,inventory_updates_mock_data = generate_mock_data(product_data,stores_data)
    print(f"Sales Transaction : {sales_transaction_mock_data}")
    print(f"Invenotory Updates  : {inventory_updates_mock_data}")
    json_sales_transaction_mock_data = json.dumps(sales_transaction_mock_data).encode('utf-8')
    json_inventory_updates_mock_data = json.dumps(inventory_updates_mock_data).encode('utf-8')

    try:
        future_transaction = publisher.publish(topic_1_path, data=json_sales_transaction_mock_data)
        future_transaction.add_done_callback(callback)
        future_transaction.result()

        future_inventory = publisher.publish(topic_2_path, data=json_inventory_updates_mock_data)
        future_inventory.add_done_callback(callback)
        future_inventory.result()

        if(random.randint(1, 8) == 3):
            for store_data in stores_data:
                product = random.sample(product_data,1)[0]
                inventory_intial_stock_data = { 
                    "product_id": product['product_id'], 
                    "timestamp": random_datetime().strftime("%Y-%m-%d %H:%M:%S"), 
                    "quantity_change": random.randint(5, 10),
                    "store_id": store_data['store_id'] 
                }

                json_inventory_intial_stock_data = json.dumps(inventory_intial_stock_data).encode('utf-8')
                future_inventory = publisher.publish(topic_2_path, data=json_inventory_intial_stock_data)
                future_inventory.add_done_callback(callback)
                future_inventory.result()
                print(f"Inventory Data intial stock : {inventory_intial_stock_data}")
                print("---------------- Row End --------------------------------------")
    except Exception as e:
        print(f"Exception encountered: {e}")
    
    print("---------------- Row End --------------------------------------")
    time.sleep(sleep_time) 