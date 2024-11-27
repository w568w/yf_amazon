import csv
from pathlib import Path

import psycopg
from tqdm.auto import tqdm


def this_dir():
    import inspect
    import os

    filename = inspect.getframeinfo(inspect.currentframe()).filename
    return os.path.dirname(os.path.abspath(filename))


DATA_DIR = Path(this_dir()).resolve().parent / "data"

SQL_CREATE_PRODUCTS = """
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    amazon_id TEXT
);
"""

SQL_CREATE_CATEGORIES = """
CREATE TABLE IF NOT EXISTS categories (
    category_id INTEGER PRIMARY KEY,
    category TEXT NOT NULL
);
"""

SQL_CREATE_PRODUCT_CATEGORIES = """
CREATE TABLE IF NOT EXISTS product_categories (
    product_id INTEGER REFERENCES products(product_id) ON DELETE CASCADE,
    category_id INTEGER REFERENCES categories(category_id) ON DELETE CASCADE,
    PRIMARY KEY (product_id, category_id)
);
"""

SQL_CREATE_RATINGS = """
CREATE TABLE IF NOT EXISTS ratings (
    rating_id BIGSERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(product_id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL,
    rating SMALLINT NOT NULL,
    timestamp BIGINT NOT NULL,
    title TEXT,
    comment TEXT
);
"""


def main():
    with psycopg.connect(
        "postgresql://postgres:example@localhost:5432/postgres"
    ) as conn:
        with conn.cursor() as cur:
            conn.execute(SQL_CREATE_PRODUCTS)
            conn.execute(SQL_CREATE_CATEGORIES)
            conn.execute(SQL_CREATE_PRODUCT_CATEGORIES)
            conn.execute(SQL_CREATE_RATINGS)

            with open(DATA_DIR / "categories.csv", "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                categories = [(row["catId"], row["category"]) for row in reader]
                with cur.copy(
                    "COPY categories (category_id, category) FROM STDIN"
                ) as copy:
                    for category in tqdm(categories, desc="Categories"):
                        copy.write_row(category)

            with open(DATA_DIR / "products.csv", "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                products = []
                product_categories = []
                for row in reader:
                    products.append((row["productId"], row["name"]))
                    # see product id 452639 for an example of a product with duplicate categories
                    cat_ids = set(int(i) for i in row["catIds"].split(","))
                    for cat_id in cat_ids:
                        product_categories.append((row["productId"], cat_id))

                with cur.copy("COPY products (product_id, name) FROM STDIN") as copy:
                    for product in tqdm(products, desc="Products"):
                        copy.write_row(product)
                with cur.copy(
                    "COPY product_categories (product_id, category_id) FROM STDIN"
                ) as copy:
                    for product_category in tqdm(
                        product_categories, desc="Product categories"
                    ):
                        copy.write_row(product_category)

            with open(DATA_DIR / "ratings.csv", "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                # see line 94639 for an example of a rating with no user id
                # see line 6186318 for an example of a rating with same user, timestamp, and product
                ratings = [
                    (
                        row["userId"],
                        row["productId"],
                        row["rating"],
                        row["timestamp"],
                        row["title"],
                        row["comment"],
                    )
                    for row in reader
                    if row["userId"] != ""
                ]
                with cur.copy(
                    "COPY ratings (user_id, product_id, rating, timestamp, title, comment) FROM STDIN"
                ) as copy:
                    for rating in tqdm(ratings, desc="Ratings"):
                        copy.write_row(rating)

            with open(DATA_DIR / "links.csv", "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                links = [(row["amazonId"], row["productId"]) for row in reader]
                for link in tqdm(links, desc="Links"):
                    cur.execute(
                        "UPDATE products SET amazon_id = %s WHERE product_id = %s",
                        (link[0], link[1]),
                    )


if __name__ == "__main__":
    main()
