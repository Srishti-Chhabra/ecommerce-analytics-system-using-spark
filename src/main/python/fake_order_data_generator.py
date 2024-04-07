import random
import time
from datetime import datetime
from faker import Faker
import json

fake = Faker()

# Set to keep track of generated IDs
generated_order_ids = set()

def generate_fake_order_data():
    # Load customer data from JSON file
    with open("customer_data.json", "r") as file:
        customer_data = json.load(file)

    # Randomly select a customer
    selected_customer = random.choice(customer_data)

    # Load product data from JSON file
    with open("product_data.json", "r") as file:
        product_data = json.load(file)

    # Randomly select a sequence of products
    order_items = []
    for _ in range(random.randint(1, 6)):
        while True:
                selected_product = random.choice(product_data)
                if 1 <= selected_product["product"]["size_id"] <= 6 and selected_product["quantity"] >= 1:
                    break
        order_items.append({
            "product_specs": {
                "product": {
                    "product_id": selected_product["product"]["product_id"],
                    "size_id": selected_product["product"]["size_id"],
                    "color": selected_product["product"]["color"]
                },
                "product_name": selected_product["product_name"],
                "brand": selected_product["brand"],
                "material": selected_product["material"],
                "price": selected_product["price"],
                "quantity": selected_product["quantity"],
                "availability_status": selected_product["availability_status"]
            },
            "quantity": random.randint(1, selected_product["quantity"]+1),
            "order_status_id": random.randint(1, 8)
        })

    while True:
        order_id = random.randint(1, 100)
        if order_id not in generated_order_ids:
            generated_order_ids.add(order_id)
            break
    orderData = {
            "order_id": order_id,
            "customer": {
                "customer_id": selected_customer["customer_id"],
                "name": selected_customer["name"],
                "email": selected_customer["email"],
                "contact_no": selected_customer["contact_no"],
                "address": selected_customer["address"],
                "age": selected_customer["age"],
                "gender_id": selected_customer["gender_id"]
            },
            "payment_id": order_id,
            "order_items": order_items
        }
    return orderData

def generate_fake_order_status_data():
    orderStatusData = [
                  {"order_status_id": 1, "order_status": "Confirmed"},
                  {"order_status_id": 2, "order_status": "Processed"},
                  {"order_status_id": 3, "order_status": "Shipped"},
                  {"order_status_id": 4, "order_status": "Delivered"},
                  {"order_status_id": 5, "order_status": "Cancelled"},
                  {"order_status_id": 6, "order_status": "Returned"},
                  {"order_status_id": 7, "order_status": "Refunded"}
              ]
    return orderStatusData

if __name__ == "__main__":

    curr_time = datetime.now()

    while (datetime.now() - curr_time).seconds < 100:
        generated_data = generate_fake_order_data()
        json_data = json.dumps(generated_data)

        time.sleep(1)

        with open("order_data.json", "a") as json_file:
            json_file.write(json_data + ',\n')

    print("Order data has been generated and saved to order_data.json.")

    order_status_data = generate_fake_order_status_data()

    with open("order_status_data.json", "w") as json_file:
        for order_status_dict in order_status_data:
            json.dump(order_status_dict, json_file)
            json_file.write(',\n')

    print("Order status data has been generated and saved to order_status_data.json.")