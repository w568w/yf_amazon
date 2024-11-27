import timeit

import matplotlib.pyplot as plt
import numpy as np
import psycopg
import pymongo

mongo_client = pymongo.MongoClient("mongodb://root:example@localhost:27017/")
psycopg_client = psycopg.connect(
    "postgresql://postgres:example@localhost:5432/postgres"
)


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
    # run the function 10 times and return the mean and std of the time
    times = timeit.repeat(func, number=1, repeat=10)
    return np.mean(times), np.std(times)


def plot_performance_comparison(sizes, results, title, filename):
    """
    Create a performance comparison plot between MongoDB and PostgreSQL.

    Args:
        sizes: List of input sizes tested
        mongodb_results: List of (mean, std) tuples for MongoDB
        postgresql_results: List of (mean, std) tuples for PostgreSQL
    """
    plt.figure(figsize=(10, 6))

    # # Convert results to numpy arrays for easier manipulation
    # mongo_means, mongo_stds = zip(*mongodb_results)
    # pg_means, pg_stds = zip(*postgresql_results)

    # # Plot with error bars
    # plt.errorbar(sizes, mongo_means, yerr=mongo_stds, label="MongoDB", marker="o")
    # plt.errorbar(sizes, pg_means, yerr=pg_stds, label="PostgreSQL", marker="s")

    for label, result in results.items():
        means, stds = zip(*result)
        plt.errorbar(sizes, means, yerr=stds, label=label, marker="s")

    plt.xscale("log")
    plt.yscale("log")

    plt.xlabel("Number of Items")
    plt.ylabel("Time (seconds)")
    plt.title(title)
    plt.legend()
    plt.grid(True)

    plt.savefig(filename)
    plt.close()


@test("MongoDB sequential scan {num} items")
def test_select_mongodb(num):
    db = mongo_client.get_database("amazon")
    db_products = db.get_collection("products")

    def query():
        return list(
            db_products.find(
                {}, limit=num, projection={"product_id": 1, "_id": 0, "name": 1}
            )
        )

    result = query()
    assert len(result) == num, f"Expected {num} items, got {len(result)}"
    assert isinstance(result[0], dict), "Expected a list of dictionaries"

    return benchmark(query)


@test("PostgreSQL sequential scan {num} items")
def test_select_postgresql(num):
    def query():
        return list(
            psycopg_client.execute(f"SELECT product_id, name FROM products LIMIT {num}")
        )

    result = query()
    assert len(result) == num, f"Expected {num} items, got {len(result)}"
    assert isinstance(result[0], tuple), "Expected a list of tuples"

    return benchmark(query)


@test("MongoDB random access {num} items")
def test_select_mongodb_random_multiple(num):
    db = mongo_client.get_database("amazon")
    db_categories = db.get_collection("products")
    product_ids = np.random.randint(0, 100000, size=num)

    def query():
        return list(
            db_categories.find(
                {"product_id": {"$in": product_ids.tolist()}},
                projection={"product_id": 1, "_id": 0, "name": 1},
            )
        )

    result = query()
    assert len(result) == len(
        product_ids
    ), f"Expected {len(product_ids)} items, got {len(result)}"
    assert isinstance(result[0], dict), "Expected a list of dictionaries"

    return benchmark(query)


@test("PostgreSQL random access {num} items")
def test_select_postgresql_random_multiple(num):
    product_ids = np.random.randint(0, 100000, size=num)

    def query():
        return list(
            psycopg_client.execute(
                f"SELECT product_id, name FROM products WHERE product_id IN ({', '.join(map(str, product_ids))})"
            )
        )

    result = query()
    assert len(result) == len(
        product_ids
    ), f"Expected {len(product_ids)} items, got {len(result)}"
    assert isinstance(result[0], tuple), "Expected a list of tuples"

    return benchmark(query)


@test("MongoDB random access {num} items no index")
def test_select_mongodb_random_no_index(num):
    db = mongo_client.get_database("amazon")
    db_categories = db.get_collection("products")
    product_ids = np.random.randint(0, 100000, size=num)

    def query():
        return list(
            db_categories.find({"product_id_no_index": {"$in": product_ids.tolist()}})
        )

    result = query()
    assert len(result) == len(
        product_ids
    ), f"Expected {len(product_ids)} items, got {len(result)}"
    assert isinstance(result[0], dict), "Expected a list of dictionaries"

    return benchmark(query)


@test("PostgreSQL random access {num} items no index")
def test_select_postgresql_random_no_index(num):
    product_ids = np.random.randint(0, 100000, size=num)

    def query():
        return list(
            psycopg_client.execute(
                f"SELECT * FROM products WHERE product_id_no_index IN ({', '.join(map(str, product_ids))})"
            )
        )

    result = query()
    assert len(result) == len(
        product_ids
    ), f"Expected {len(product_ids)} items, got {len(result)}"
    assert isinstance(result[0], tuple), "Expected a list of tuples"

    return benchmark(query)


@test("MongoDB one-to-many retrieval")
def test_select_mongodb_one_to_many_retrieval():
    db = mongo_client.get_database("amazon")
    db_products = db.get_collection("products")

    def query():
        return list(db_products.find_one({"product_id": 1})["ratings"])

    return benchmark(query)


@test("PostgreSQL one-to-many retrieval")
def test_select_postgresql_one_to_many_retrieval():
    def query():
        return list(
            psycopg_client.execute("SELECT * FROM ratings WHERE product_id = 1")
        )

    return benchmark(query)


@test("MongoDB many-to-many join")
def test_select_mongodb_many_to_many_join():
    db = mongo_client.get_database("amazon")
    db_products = db.get_collection("products")

    def query():
        return list(
            db_products.aggregate(
                [
                    {"$match": {"product_id": 1}},
                    {
                        "$lookup": {
                            "from": "categories",
                            "localField": "categories",
                            "foreignField": "cat_id",
                            "as": "categories",
                        }
                    },
                ]
            )
        )

    return benchmark(query)


@test("PostgreSQL many-to-many join")
def test_select_postgresql_many_to_many_join():
    def query():
        return list(
            psycopg_client.execute(
                """
                SELECT p.product_id, p.name, c.category_id, c.category
                FROM products p
                JOIN product_categories pc ON p.product_id = pc.product_id
                JOIN categories c ON pc.category_id = c.category_id
                WHERE p.product_id = 1
                """
            )
        )

    result = query()
    print(result)

    return benchmark(query)


@test("MongoDB create {num} records")
def test_mongodb_create_record(num):
    db = mongo_client.get_database("amazon")
    db_products = db.get_collection("products")
    max_product_id = db_products.find_one(sort=[("product_id", pymongo.DESCENDING)])[
        "product_id"
    ]

    current_id = max_product_id + 1

    def query():
        nonlocal current_id
        current_ids = [current_id + i for i in range(num)]
        current_id += num
        return db_products.insert_many(
            [{"product_id": id, "name": "test"} for id in current_ids]
        )

    return benchmark(query)


@test("PostgreSQL create {num} records")
def test_postgresql_create_record(num):
    max_product_id = psycopg_client.execute(
        "SELECT MAX(product_id) FROM products"
    ).fetchone()[0]
    current_id = max_product_id + 1

    def query():
        nonlocal current_id
        current_ids = [current_id + i for i in range(num)]
        current_id += num
        values = ", ".join(f"({id}, 'test')" for id in current_ids)
        return psycopg_client.execute(
            f"INSERT INTO products (product_id, name) VALUES {values}"
        )

    return benchmark(query)


@test("MongoDB update {num} records")
def test_mongodb_update_record(num):
    db = mongo_client.get_database("amazon")
    db_products = db.get_collection("products")

    start_index = 341242
    end_index = start_index + num

    def query():
        return db_products.update_many(
            {"product_id": {"$gte": start_index, "$lte": end_index}},
            {"$set": {"name": "test"}},
        )

    return benchmark(query)


@test("PostgreSQL update {num} records")
def test_postgresql_update_record(num):
    start_index = 341242
    end_index = start_index + num

    def query():
        return psycopg_client.execute(
            f"UPDATE products SET name = 'test' WHERE product_id BETWEEN {start_index} AND {end_index}"
        )

    return benchmark(query)


@test("MongoDB update {num} records no index")
def test_mongodb_update_record_no_index(num):
    db = mongo_client.get_database("amazon")
    db_products = db.get_collection("products")

    start_index = 341242
    end_index = start_index + num

    def query():
        return db_products.update_many(
            {"product_id_no_index": {"$gte": start_index, "$lte": end_index}},
            {"$set": {"name": "test"}},
        )

    return benchmark(query)


@test("PostgreSQL update {num} records no index")
def test_postgresql_update_record_no_index(num):
    start_index = 341242
    end_index = start_index + num

    def query():
        return psycopg_client.execute(
            f"UPDATE products SET name = 'test' WHERE product_id_no_index BETWEEN {start_index} AND {end_index}"
        )

    return benchmark(query)


delete_start_index_mongodb = 342643


@test("MongoDB delete {num} records")
def test_mongodb_delete_record(num):
    db = mongo_client.get_database("amazon")
    db_products = db.get_collection("products")

    def query():
        global delete_start_index_mongodb
        start = delete_start_index_mongodb
        end = start + num
        delete_start_index_mongodb = end
        return db_products.delete_many({"product_id": {"$gte": start, "$lte": end}})

    return benchmark(query)


delete_start_index_postgresql = 342643


@test("PostgreSQL delete {num} records")
def test_postgresql_delete_record(num):
    def query():
        global delete_start_index_postgresql
        start = delete_start_index_postgresql
        end = start + num
        delete_start_index_postgresql = end
        return psycopg_client.execute(
            f"DELETE FROM products WHERE product_id BETWEEN {start} AND {end}"
        )

    return benchmark(query)


def test_sequential_scan():
    mongodb_results = []
    postgresql_results = []
    sizes = [1, 3, 10, 30, 100, 300, 1000, 3000, 10000]

    for num in sizes:
        mongodb_results.append(test_select_mongodb(num=num))
        postgresql_results.append(test_select_postgresql(num=num))

    # Create the performance comparison plot
    plot_performance_comparison(
        sizes,
        {"MongoDB": mongodb_results, "PostgreSQL": postgresql_results},
        "Sequential Scan",
        "results/sequential_scan.png",
    )


def test_random_access_multiple():
    # Run random access test separately
    mongodb_results = []
    postgresql_results = []
    mongodb_results_no_index = []
    postgresql_results_no_index = []
    sizes = [3, 10, 30, 100]

    for num in sizes:
        mongodb_results.append(test_select_mongodb_random_multiple(num=num))
        postgresql_results.append(test_select_postgresql_random_multiple(num=num))
        mongodb_results_no_index.append(test_select_mongodb_random_no_index(num=num))
        postgresql_results_no_index.append(
            test_select_postgresql_random_no_index(num=num)
        )

    plot_performance_comparison(
        sizes,
        {
            "MongoDB": mongodb_results,
            "PostgreSQL": postgresql_results,
            "MongoDB no index": mongodb_results_no_index,
            "PostgreSQL no index": postgresql_results_no_index,
        },
        "Random Access Multiple",
        "results/random_access_multiple.png",
    )


def test_one_to_many_retrieval():
    test_select_mongodb_one_to_many_retrieval()
    test_select_postgresql_one_to_many_retrieval()


def test_many_to_many_join():
    test_select_mongodb_many_to_many_join()
    test_select_postgresql_many_to_many_join()


def test_create_record():
    mongodb_results = []
    postgresql_results = []
    sizes = [1, 3, 10, 30, 100, 300, 1000]

    for num in sizes:
        mongodb_results.append(test_mongodb_create_record(num=num))
        postgresql_results.append(test_postgresql_create_record(num=num))

    plot_performance_comparison(
        sizes,
        {"MongoDB": mongodb_results, "PostgreSQL": postgresql_results},
        "Create Record",
        "results/create_record.png",
    )


def test_update_record():
    mongodb_results = []
    postgresql_results = []
    mongodb_results_no_index = []
    postgresql_results_no_index = []
    sizes = [1, 3, 10, 30, 100, 300, 1000]

    for num in sizes:
        mongodb_results.append(test_mongodb_update_record(num=num))
        postgresql_results.append(test_postgresql_update_record(num=num))
        mongodb_results_no_index.append(test_mongodb_update_record_no_index(num=num))
        postgresql_results_no_index.append(
            test_postgresql_update_record_no_index(num=num)
        )

    plot_performance_comparison(
        sizes,
        {
            "MongoDB": mongodb_results,
            "PostgreSQL": postgresql_results,
            "MongoDB no index": mongodb_results_no_index,
            "PostgreSQL no index": postgresql_results_no_index,
        },
        "Update Record",
        "results/update_record.png",
    )


def test_delete_record():
    mongodb_results = []
    postgresql_results = []
    sizes = [1, 3, 10, 30, 100]

    for num in sizes:
        mongodb_results.append(test_mongodb_delete_record(num=num))
        postgresql_results.append(test_postgresql_delete_record(num=num))

    plot_performance_comparison(
        sizes,
        {"MongoDB": mongodb_results, "PostgreSQL": postgresql_results},
        "Delete Record",
        "results/delete_record.png",
    )


if __name__ == "__main__":
    # test_sequential_scan()
    # test_random_access_multiple()
    # test_one_to_many_retrieval()
    # test_many_to_many_join()
    # test_create_record()
    # test_update_record()
    test_delete_record()
