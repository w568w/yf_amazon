# Load model directly
from sentence_transformers import SentenceTransformer
import torch
import psycopg
from fastapi import FastAPI, Depends
from typing import Optional
from psycopg.rows import dict_row
from contextlib import asynccontextmanager
import uvicorn
import requests

# 全局变量用于存储数据库连接
db_connection = None
model = None
es_host = "http://localhost:9200"
app = FastAPI()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_connection
    global model
    db_connection = psycopg.connect(
        "",  # TODO: 填入数据库连接字符串
        row_factory=dict_row,  # 返回结果格式为字典
    )
    model = SentenceTransformer("./model", trust_remote_code=True)

    if torch.cuda.is_available():
        model = model.to("cuda")
    print("Database connection established.")
    yield
    db_connection.close()
    print("Database connection closed.")


def get_db():
    if not db_connection:
        raise RuntimeError("Database connection is not established.")
    return db_connection


def get_es_host():
    return es_host


app = FastAPI(lifespan=lifespan)


def vector2str(vector):
    return f"[{','.join(map(str, vector))}]"


def get_embedding(query_text: str):
    return model.encode([query_text], prompt_name="query")[0]


def get_product_psql(keyword, exact=False, top_k=100, db_connection=None):
    with db_connection.cursor() as cur:
        if exact:
            cur.execute(
                "SELECT name, product_id, amazon_id FROM products WHERE name LIKE %s LIMIT %s",
                ("%" + keyword + "%", top_k),
            )
        else:
            query_embedding = get_embedding(keyword)
            cur.execute(
                "SELECT name, product_id, amazon_id FROM products ORDER BY title_embedding <=> %s LIMIT %s",
                (vector2str(query_embedding), top_k),
            )
        return cur.fetchall()


def get_comments_psql(keyword, product_id, exact=False, top_k=100, db_connection=None):
    with db_connection.cursor() as cur:
        if exact:
            cur.execute(
                "SELECT product_id, user_id, rating, timestamp, title, comment FROM ratings WHERE product_id = %s AND (title LIKE %s OR comment LIKE %s) LIMIT %s",
                (product_id, "%" + keyword + "%", "%" + keyword + "%", top_k),
            )
        else:
            query_embedding = get_embedding(keyword)
            cur.execute(
                "SELECT product_id, user_id, rating, timestamp, title, comment FROM ratings WHERE product_id = %s AND doc_embedding is not null ORDER BY doc_embedding <=> %s LIMIT %s",
                (product_id, vector2str(query_embedding), top_k),
            )
        return cur.fetchall()


def get_product_elastic(
    keyword, top_k=100, es_host="http://localhost:9200", index="products"
):
    # 精确匹配 - 使用 `match` 或 `term` 查询
    query = {"query": {"match": {"name": keyword}}, "size": top_k}

    # 发送请求到 ElasticSearch
    response = requests.post(f"{es_host}/{index}/_search", json=query)

    # 解析返回结果
    if response.status_code == 200:
        hits = response.json().get("hits", {}).get("hits", [])
        return [
            {
                "name": hit["_source"]["name"],
                "product_id": hit["_source"]["product_id"],
                "amazon_id": hit["_source"].get("amazon_id"),
            }
            for hit in hits
        ]
    else:
        raise Exception(
            f"Error from ElasticSearch: {response.status_code}, {response.text}"
        )


def get_comments_elastic(
    keyword, product_id, top_k=100, es_host="http://localhost:9200", index="ratings"
):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"product_id": product_id}},
                    {"multi_match": {"query": keyword, "fields": ["title", "comment"]}},
                ]
            }
        },
        "size": top_k,
    }
    response = requests.post(f"{es_host}/{index}/_search", json=query)

    if response.status_code == 200:
        hits = response.json().get("hits", {}).get("hits", [])
        return [
            {
                "product_id": hit["_source"]["product_id"],
                "user_id": hit["_source"]["user_id"],
                "rating": hit["_source"]["rating"],
                "timestamp": hit["_source"]["timestamp"],
                "title": hit["_source"]["title"],
                "comment": hit["_source"]["comment"],
            }
            for hit in hits
        ]
    else:
        raise Exception(
            f"Error from ElasticSearch: {response.status_code}, {response.text}"
        )


@app.get("/ratings")
def get_filtered_comments(
    product_id: Optional[int] = None,
    user_id: Optional[int] = None,
    rating_min: Optional[float] = None,
    rating_max: Optional[float] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    top_k: int = 100,
    db_connection=Depends(get_db),
):
    with db_connection.cursor() as cur:
        # 初始化基础查询
        query = (
            "SELECT product_id, user_id, rating, timestamp, title, comment FROM ratings"
        )
        conditions = []  # 存储筛选条件
        params = []  # 存储参数值

        # 动态添加条件
        if product_id is not None:
            conditions.append("product_id = %s")
            params.append(product_id)
        if user_id is not None:
            conditions.append("user_id = %s")
            params.append(user_id)
        if rating_min is not None:
            conditions.append("rating >= %s")
            params.append(rating_min)
        if rating_max is not None:
            conditions.append("rating <= %s")
            params.append(rating_max)
        if start_time is not None:
            conditions.append("timestamp >= %s")
            params.append(start_time)
        if end_time is not None:
            conditions.append("timestamp <= %s")
            params.append(end_time)

        # 拼接 WHERE 子句
        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        # 添加排序和限制条件
        query += " ORDER BY timestamp DESC LIMIT %s"
        params.append(top_k)

        # 执行查询
        cur.execute(query, tuple(params))
        return cur.fetchall()


@app.post("/search")
def search(
    keyword: str,
    exact: bool = False,
    product_id: int = -1,
    backend: str = "psql",
    top_k: int = 100,
    db_connection=Depends(get_db),
    es_host=Depends(get_es_host),
):
    assert backend in ["psql", "elastic"]
    if backend == "psql":
        return (
            get_product_psql(keyword, exact, top_k, db_connection)
            if product_id == -1
            else get_comments_psql(keyword, product_id, exact, top_k, db_connection)
        )
    elif backend == "elastic":
        return (
            get_product_elastic(keyword, top_k, es_host)
            if product_id == -1
            else get_comments_elastic(keyword, product_id, top_k, es_host)
        )


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
