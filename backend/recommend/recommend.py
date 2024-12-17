import numpy as np
import psycopg

from cache import cache


def load_product_embeddings(
    db_connection: psycopg.Connection, product_ids: list[int]
) -> dict[int, np.ndarray]:
    """
    加载指定商品的嵌入向量。

    :param db_connection: 数据库连接。
    :param product_ids: 商品 ID 列表。
    :return: 商品 ID 到嵌入向量的映射字典。
    """
    with db_connection.cursor() as cursor:
        cursor.execute(
            "SELECT product_id, title_embedding FROM products WHERE product_id = ANY(%s)",
            (product_ids,),
        )
        embeddings = {row["product_id"]: np.array(row["title_embedding"]) for row in cursor.fetchall()}
    return embeddings


def find_similar_users(
    db_connection: psycopg.Connection, target_user_id: int, top_k: int = 5
) -> list[int]:
    """
    查找与目标用户最相似的用户。

    :param db_connection: 数据库连接。
    :param target_user_id: 目标用户 ID。
    :param top_k: 返回的相似用户数量。
    :return: 相似用户的 ID 列表。
    """
    with db_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT user_id, user_embedding <=> (SELECT user_embedding FROM users WHERE user_id = %s) AS similarity
            FROM users
            WHERE user_id != %s
            ORDER BY similarity
            LIMIT %s
            """,
            (target_user_id, target_user_id, top_k),
        )
        results = cursor.fetchall()
    return [row["user_id"] for row in results] 


def get_user_embedding(
    db_connection: psycopg.Connection, user_id: int
) -> np.ndarray | None:
    """
    根据用户的历史购买记录计算用户的嵌入向量。

    :param db_connection: 数据库连接。
    :param user_id: 用户 ID。
    :return: 用户嵌入向量，若无记录返回 None。
    """
    with db_connection.cursor() as cursor:
        cursor.execute(
            "SELECT user_embedding FROM users WHERE user_id = %s", (user_id,)
        )
        result = cursor.fetchone()
    return np.array(result["user_embedding"]) if result else None


def recommend_related_embedding(
    db_connection: psycopg.Connection, user_id: int, top_k: int = 5
) -> list[int]:
    """
    根据协同过滤为用户推荐商品。

    :param db_connection: 数据库连接。
    :param user_id: 用户 ID。
    :param top_k: 推荐商品数量。
    :return: 推荐的商品 ID 列表。
    """
    similar_users = find_similar_users(db_connection, user_id, top_k=5)
    with db_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT product_id
            FROM ratings
            WHERE user_id = %s
            """,
            (user_id,),
        )
        user_purchased = set(row["product_id"] for row in cursor.fetchall())

        recommended_products = set()
        for similar_user_id in similar_users:
            cursor.execute(
                """
                SELECT product_id
                FROM ratings
                WHERE user_id = %s
                """,
                (similar_user_id,),
            )
            similar_user_products = set(row["product_id"] for row in cursor.fetchall())

            recommended_products.update(similar_user_products - user_purchased)

            if len(recommended_products) >= top_k:
                break

    return list(recommended_products)[:top_k]


def recommend_related(
    db_connection: psycopg.Connection, user_id: int, top_k: int = 5
) -> list[int]:
    """
    推荐与用户购买同一商品的其他用户购买的商品。

    :param db_connection: 数据库连接。
    :param user_id: 用户 ID。
    :param top_k: 推荐的商品数量。
    :return: 推荐的商品 ID 列表。
    """
    with db_connection.cursor() as cursor:
        # 获取目标用户购买的商品列表
        cursor.execute(
            """
            SELECT product_id
            FROM ratings
            WHERE user_id = %s
            """,
            (user_id,),
        )
        user_products = set(row["product_id"] for row in cursor.fetchall())
        
        if not user_products:
            return []  # 如果用户没有购买记录，直接返回空列表

        # 找到购买过同样商品的其他用户
        cursor.execute(
            """
            SELECT DISTINCT user_id
            FROM ratings
            WHERE product_id = ANY(%s) AND user_id != %s
            """,
            (list(user_products), user_id),
        )
        related_users = set(row["user_id"] for row in cursor.fetchall())

        if not related_users:
            return []  # 如果没有其他用户买过相同的商品，返回空列表

        # 找到这些用户购买的商品，排除目标用户已经购买过的商品
        recommended_products = set()
        cursor.execute(
            """
            SELECT product_id
            FROM ratings
            WHERE user_id = ANY(%s)
            """,
            (list(related_users),),
        )
        for row in cursor.fetchall():
            product_id = row["product_id"]
            if product_id not in user_products:  # 排除用户已经购买的商品
                recommended_products.add(product_id)
            if len(recommended_products) >= top_k:
                break

    return list(recommended_products)[:top_k]


def recommend_embedding(
    db_connection: psycopg.Connection, user_id: int, top_k: int = 5
) -> list[dict]:
    """
    根据用户嵌入优化搜索结果。

    :param db_connection: 数据库连接。
    :param user_id: 用户 ID。
    :param top_k: 推荐商品数量。
    :return: 按优化排序后的搜索结果。
    """
    with db_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT product_id, title_embedding <=> (SELECT user_embedding FROM users WHERE user_id = %s) AS similarity
            FROM products
            ORDER BY similarity
            LIMIT %s
            """,
            (user_id, top_k),
        )
        results = cursor.fetchall()
        products = set(row["product_id"] for row in results)
    return list(products)


@cache(cache_keys=["user_id", "method", "top_k"])
def recommend(
    db_connection: psycopg.Connection, user_id: int, method: str, top_k: int = 5
) -> list[int]:
    if method == "related":
        return recommend_related(db_connection, user_id, top_k)
    elif method == "related_embedding":
        return recommend_related_embedding(db_connection, user_id, top_k)
    else:
        return recommend_embedding(db_connection, user_id, top_k)
