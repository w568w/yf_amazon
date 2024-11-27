import csv
from pathlib import Path

import psycopg
import pymongo
from tqdm.auto import tqdm


def update_mongodb():
    client = pymongo.MongoClient("mongodb://root:example@localhost:27017/")
    db = client.get_database("amazon")
    db_products = db.get_collection("products")

    # Add product_id_no_index field to all documents
    db_products.update_many({}, [{"$set": {"product_id_no_index": "$product_id"}}])


def update_postgresql():
    with psycopg.connect(
        "postgresql://postgres:example@localhost:5432/postgres"
    ) as conn:
        with conn.cursor() as cur:
            # Add product_id_no_index column
            cur.execute("""
                ALTER TABLE products 
                ADD COLUMN IF NOT EXISTS product_id_no_index INTEGER;
            """)

            # Copy product_id to product_id_no_index
            cur.execute("""
                UPDATE products 
                SET product_id_no_index = product_id;
            """)


def main():
    print("Updating MongoDB...")
    update_mongodb()
    print("Updating PostgreSQL...")
    update_postgresql()


if __name__ == "__main__":
    main()
