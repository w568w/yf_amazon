# Load model directly
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import psycopg
import requests
import torch
import uvicorn
from fastapi import Depends, FastAPI, HTTPException
from psycopg.rows import dict_row
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
from fastapi.middleware.cors import CORSMiddleware
from cache import cache
from recommend.recommend import recommend

# 全局变量用于存储数据库连接
db_connection = None
model = None
es_host = os.getenv("ES_HOST", "http://localhost:9200")
db_url = os.getenv("DB_URL", "postgresql://postgres:example@localhost:5432/postgres")
app = FastAPI()

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_connection
    global model
    db_connection = psycopg.connect(
        db_url,
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


@cache(cache_keys=["keyword", "exact", "top_k"])
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


@cache(cache_keys=["keyword", "product_id", "exact", "top_k"])
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


@cache(cache_keys=["keyword", "exact", "top_k"])
def get_product_elastic(
    keyword,
    exact=False,
    top_k=100,
    es_host="http://localhost:9200",
    index="products_with_amazon",
):
    # 精确匹配 - 使用 `match` 或 `term` 查询
    query = {"query": {"match": {"name": keyword}}, "size": top_k}
    if not exact:
        query = {
            "query": {
                "match": {
                    "name": {
                        "query": keyword,  # 你要搜索的关键字
                        "fuzziness": "AUTO",  # fuzziness 可以设为 AUTO 或者数字
                    }
                }
            },
            "size": top_k,
        }

    # 发送请求到 ElasticSearch
    response = requests.post(f"{es_host}/{index}/_search", json=query)

    # 解析返回结果
    if response.status_code == 200:
        hits = response.json().get("hits", {}).get("hits", [])

        return [
            {
                "name": hit["_source"]["name"],
                "product_id": hit["_source"]["productId"],
                "amazon_id": hit["_source"].get("amazonId"),
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
    assert not (backend == "elastic" and product_id != -1)
    if backend == "psql":
        return (
            get_product_psql(keyword, exact, top_k, db_connection)
            if product_id == -1
            else get_comments_psql(keyword, product_id, exact, top_k, db_connection)
        )
    elif backend == "elastic":
        return get_product_elastic(keyword, exact, top_k, es_host)


# 请求体的定义
class RecommendRequest(BaseModel):
    user_id: int
    method: str
    top_k: Optional[int] = 5  # 默认 5 个推荐商品


# 响应体的定义
class RecommendResponse(BaseModel):
    user_id: int
    method: str
    recommendations: List[Any]


@app.post("/recommend", response_model=RecommendResponse)
async def recommend_api(request: RecommendRequest, db_connection=Depends(get_db)):
    """
    POST 接口：根据用户 ID 和推荐方法返回推荐结果。
    """
    try:
        user_id = request.user_id
        method = request.method
        top_k = request.top_k

        # 根据 method 调用不同的推荐函数
        recommendations = recommend(db_connection, user_id, method, top_k)

        # 返回推荐结果
        with db_connection.cursor() as cur:
            cur.execute(
                """
                SELECT name, product_id, amazon_id FROM products WHERE product_id = ANY(%s)
                """,
                (recommendations,),
            )
            recommendations = cur.fetchall()
        return RecommendResponse(
            user_id=user_id, method=method, recommendations=recommendations
        )

    except Exception as e:
        print("Error in recommend_api:", e)
        raise HTTPException(status_code=500, detail=str(e))

origins = [
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
