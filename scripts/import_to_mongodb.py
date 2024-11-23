import pymongo
import csv
from pathlib import Path
from tqdm.auto import tqdm
from pymongo import InsertOne, DeleteOne, ReplaceOne, UpdateOne

def this_dir():
    import inspect
    import os
    filename = inspect.getframeinfo(inspect.currentframe()).filename
    return os.path.dirname(os.path.abspath(filename))

DATA_DIR = Path(this_dir()).resolve().parent / 'data'

def main():
    client = pymongo.MongoClient("mongodb://root:example@localhost:27017/")
    db = client.get_database("amazon")

    db_categories = db.get_collection("categories")
    db_categories.create_index("cat_id", unique=True)
    with open(DATA_DIR / 'categories.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        db_categories.insert_many({"cat_id": int(row["catId"]), "category": row["category"]} for row in reader)

    
    db_products = db.get_collection("products")
    db_products.create_index("product_id", unique=True)
    products = {}
    with open(DATA_DIR / 'products.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cat_ids = set(int(i) for i in row["catIds"].split(','))
            products[int(row["productId"])] = {"name": row["name"], "ratings": [], "amazon_link": None, "categories": list(cat_ids), "product_id": int(row["productId"])}
    
    with open(DATA_DIR / 'ratings.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["userId"] == "":
                continue
            products[int(row["productId"])]["ratings"].append({"user_id": int(row["userId"]), "rating": int(row["rating"]), "timestamp": int(row["timestamp"]), "title": row["title"], "comment": row["comment"]})
    db_products.insert_many(products.values())


    # Import ratings separately. It can be very slow so we don't use this part.
    # with open(DATA_DIR / 'ratings.csv', 'r', encoding='utf-8') as f:
    #     reader = csv.DictReader(f)
    #     ratings = []
    #     for row in reader:
    #         if row["userId"] == "":
    #             continue
    #         ratings.append(UpdateOne({"product_id": row["productId"]}, {"$push": {"ratings": {"user_id": int(row["userId"]), "rating": int(row["rating"]), "timestamp": int(row["timestamp"]), "title": row["title"], "comment": row["comment"]}}}))
    #     db_products.bulk_write(ratings)
    
    with open(DATA_DIR / 'links.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        links = []
        for row in reader:
            links.append(UpdateOne({"product_id": row["productId"]}, {"$set": {"amazon_link": row["amazonId"]}}))
        db_products.bulk_write(links)

if __name__ == '__main__':
    main()


