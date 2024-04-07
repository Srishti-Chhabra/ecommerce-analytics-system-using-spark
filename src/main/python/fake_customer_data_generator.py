import random
import time
from datetime import datetime
from faker import Faker
import json

fake = Faker()

# Set to keep track of generated IDs
generated_customer_ids = set()

def generate_fake_customer_data():
    while True:
        customer_id = random.randint(1, 50)
        if customer_id not in generated_customer_ids:
            generated_customer_ids.add(customer_id)
            break
    customerData = {
            "customer_id": customer_id,
            "name": {
                "first_name": fake.first_name(),
                "last_name": fake.last_name()
            },
            "email": fake.email(),
            "contact_no": fake.random_int(6000000000, 9999999999),
            "address": fake.random_element(elements=("Ferozpur, Punjab","Mohali, Punjab","Solan, HP", "Lucknow, UP", "Kolkata, West Bengal")),
            "age": random.randint(18, 80),
            "gender_id": fake.random_element(elements=(1,2,3))
        }
    return customerData

def generate_fake_gender_data():
    genderData = [
                  {"gender_id": 1, "gender": "Male"},
                  {"gender_id": 2, "gender": "Female"},
                  {"gender_id": 3, "gender": "Others"},
              ]
    return genderData

if __name__ == "__main__":

    curr_time = datetime.now()

    while (datetime.now() - curr_time).seconds < 50:
        generated_data = generate_fake_customer_data()
        json_data = json.dumps(generated_data)

        time.sleep(1)

        with open("customer_data.json", "a") as json_file:
            json_file.write(json_data + ',\n')

    print("Customer data has been generated and saved to customer_data.json.")

    gender_data = generate_fake_gender_data()

    with open("gender_data.json", "w") as json_file:
        for gender_dict in gender_data:
            json.dump(gender_dict, json_file)
            json_file.write(',\n')

    print("Gender data has been generated and saved to gender_data.json.")