import random
import time
from datetime import datetime
from faker import Faker
import json

fake = Faker()

# Set to keep track of generated product IDs
generated_ids = set()

def generate_fake_product_data():
    while True:
        product_id = random.randint(1, 100)
        if product_id not in generated_ids:
            generated_ids.add(product_id)
            break
    productData = {
            "product": {
                "product_id": product_id,
                "size_id": fake.random_element(elements=(-1,1,2,3,4,5,6,7,8)),
                "color": fake.random_element(elements=("Black", "White", "Red", "Blue", "Green", "Yellow", "Grey", "Olive")),
            },
            "product_name": fake.random_element(elements=("Shirt", "Jeans", "Skirt", "Hoodie", "Sweater","Sweatshirt","Jumpsuit","Jacket","Overcoat","Trousers")),
            "brand": fake.random_element(elements=("Roadster", "H&M", "Zara", "HRX", "Puma","Addidas","Red Tape","Pepe","Octave","Numero Uno")),
            "material": fake.random_element(elements=("cotton", "linen", "polyester", "denim", "organza", "satin")),
            "price": random.randint(899, 3999),
            "quantity": random.randint(0, 50)
        }
    # Set availability status based on quantity
    if productData["quantity"] == 0:
        productData["availability_status"] = 0
    else:
        productData["availability_status"] = 1
    return productData

def generate_fake_size_data():
    sizeData = [
                  {"size_id": 1, "size": "XS"},
                  {"size_id": 2, "size": "S"},
                  {"size_id": 3, "size": "M"},
                  {"size_id": 4, "size": "L"},
                  {"size_id": 5, "size": "XL"},
                  {"size_id": 6, "size": "XXL"}
              ]
    return sizeData

if __name__ == "__main__":

    curr_time = datetime.now()

    while (datetime.now() - curr_time).seconds < 100:
        generated_data = generate_fake_product_data()
        json_data = json.dumps(generated_data)

        time.sleep(1)

        with open("product_data.json", "a") as json_file:
            json_file.write(json_data + ',\n')

    print("Product data has been generated and saved to product_data.json.")

    size_data = generate_fake_size_data()

    with open("size_data.json", "w") as json_file:
        for size_dict in size_data:
            json.dump(size_dict, json_file)
            json_file.write(',\n')

    print("Size data has been generated and saved to size_data.json.")