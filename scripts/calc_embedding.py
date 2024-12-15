import psycopg
from tqdm.auto import tqdm, trange
import pickle
import numpy as np
import pymongo
from bson.binary import Binary, BinaryVectorDtype


def test(name):
    def decorator(func):
        def wrapper(*args, **kwargs):
            mean, std = func(*args, **kwargs)
            formatted_name = name.format(*args, **kwargs)
            print(f"{formatted_name}: mean={mean:.6f}s, std={std:.6f}s")
            return mean, std

        return wrapper

    return decorator


def benchmark(func):
    import timeit

    # run the function 10 times and return the mean and std of the time
    times = timeit.repeat(func, number=1, repeat=3)
    return np.mean(times), np.std(times)


conn = psycopg.connect("postgresql://postgres:example@localhost:5432/postgres")

SQL_CREATE_EXTENSIONS = """
CREATE EXTENSION IF NOT EXISTS vector;
ALTER TABLE ratings ADD COLUMN IF NOT EXISTS doc_embedding vector(1536);
ALTER TABLE products ADD COLUMN IF NOT EXISTS title_embedding vector(1536);
"""

SQL_CREATE_INDEX_IVFFLAT = """
SET maintenance_work_mem TO '8GB';
CREATE INDEX ON ratings USING ivfflat (doc_embedding vector_cosine_ops) WITH (lists = 100);
"""

SQL_CREATE_INDEX_HNSW = """
SET max_parallel_maintenance_workers = 8;
CREATE INDEX ON ratings USING hnsw (doc_embedding vector_cosine_ops);
"""

SQL_CREATE_VIEW_USERS = """
CREATE MATERIALIZED VIEW users (user_id, user_embedding) AS
SELECT user_id, AVG(doc_embedding)::vector(1536) FROM ratings WHERE doc_embedding IS NOT NULL GROUP BY user_id;
"""

SQL_CREATE_INDEX_HNSW_PRODUCTS = """
SET max_parallel_maintenance_workers = 8;
CREATE INDEX IF NOT EXISTS products_title_embedding_hnsw_idx ON products USING hnsw (title_embedding vector_cosine_ops);
"""

SQL_CREATE_INDEX_HNSW_USERS = """
SET max_parallel_maintenance_workers = 8;
CREATE INDEX IF NOT EXISTS users_user_embedding_hnsw_idx ON users USING hnsw (user_embedding vector_cosine_ops);
"""

client = pymongo.MongoClient(
)
db = client["amazon"]
collection = db["products"]


def export_ratings(limit=1000000):
    with conn.cursor() as cur:
        # get rating_id, title, comment from ratings
        cur.execute("SELECT rating_id, title, comment FROM ratings LIMIT %s", (limit,))
        rows = cur.fetchall()
        return rows

def export_products(limit=1000000):
    with conn.cursor() as cur:
        # get product_id, name from products
        cur.execute("SELECT product_id, name FROM products LIMIT %s", (limit,))
        rows = cur.fetchall()
        return rows

def vector2str(vector):
    return f"[{','.join(map(str, vector))}]"


def import_ratings(rating_tuples, vectors):
    with conn.pipeline():
        with conn.cursor() as cur:
            for rating_tuple, vector in tqdm(
                zip(rating_tuples, vectors), total=len(rating_tuples)
            ):
                cur.execute(
                    "UPDATE ratings SET doc_embedding = %s WHERE rating_id = %s",
                    (vector2str(vector), rating_tuple[0]),
                )
        conn.commit()

def import_products(product_tuples, vectors):
    with conn.pipeline():
        with conn.cursor() as cur:
            for product_tuple, vector in tqdm(zip(product_tuples, vectors), total=len(product_tuples)):
                cur.execute("UPDATE products SET title_embedding = %s WHERE product_id = %s", (vector2str(vector), product_tuple[0]))
        conn.commit()


def import_data_mongodb(rating_tuples, vectors):
    # use insert_many to insert all the vectors at once
    collection.drop()
    batch_size = 1000
    for i in trange(0, len(rating_tuples) // 10, batch_size):
        batch = [
            {
                "rating_id": rating_tuple[0],
                "doc_embedding": Binary.from_vector(
                    vector,
                    dtype=BinaryVectorDtype.FLOAT32,
                ),
            }
            for rating_tuple, vector in zip(
                rating_tuples[i : i + batch_size], vectors[i : i + batch_size]
            )
        ]
        result = collection.insert_many(batch)


@test("Searching for one vector in given range")
def search_vector_in_range():
    def query():
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM ratings WHERE doc_embedding <=> %s > 0.5 LIMIT 1",
                (vector2str(np.random.rand(1536)),),
            )
            rows = cur.fetchall()
            return rows

    return benchmark(query)


@test("Searching for the nearest vector")
def search_nearest_vector():
    def query():
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM ratings ORDER BY doc_embedding <=> %s LIMIT 1",
                (vector2str(np.random.rand(1536)),),
            )
            rows = cur.fetchall()
            return rows

    return benchmark(query)

@test("Searching for the nearest vector without index1")
def search_nearest_vector_no_index():
    def query():
        with conn.cursor() as cur:
            cur.execute(
                "begin;drop index ratings_doc_embedding_idx1;SELECT * FROM ratings ORDER BY doc_embedding <=> '%s' LIMIT 1;rollback;" % vector2str(np.random.rand(1536))
            )

    return benchmark(query)

def main():
    with conn.cursor() as cur:
        cur.execute(SQL_CREATE_VIEW_USERS)
        conn.commit()

    # rows = export_products(6000000)
    # with open("products.pkl", "wb") as f:
    #     pickle.dump(rows, f)
    # with open("ratings.pkl", "wb") as f:
    #     pickle.dump(rows, f)
    # with open("products.pkl", "rb") as f:
    #     rows = pickle.load(f)
    # with open("product_embeddings.npy", "rb") as f:
    #     vectors = np.load(f)
    # import_products(rows, vectors)
    # import_data_mongodb(rows, vectors)
    # search_vector_in_range()
    # search_nearest_vector()
    # search_nearest_vector_no_index()


if __name__ == "__main__":
    main()
