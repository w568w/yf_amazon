import psycopg2
import numpy as np

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "example",
    "host": "192.168.84.3",
    "port": 11354
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def load_product_embeddings(product_ids: list[int]) -> dict[int, np.ndarray]:
    """
    加载指定商品的嵌入向量。

    :param product_ids: 商品 ID 列表。
    :return: 商品 ID 到嵌入向量的映射字典。
    """
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT product_id, title_embedding FROM products WHERE product_id = ANY(%s)", (product_ids,)
    )
    embeddings = {row[0]: np.array(row[1]) for row in cursor.fetchall()}
    cursor.close()
    conn.close()
    return embeddings


def get_user_history(user_id: int) -> list[int]:
    """
    获取指定用户的历史购买记录（商品 ID 列表）。

    :param user_id: 用户 ID。
    :return: 用户购买的商品 ID 列表。
    """
    
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT product_id FROM ratings WHERE user_id = %s", (user_id,))
    user_history = cursor.fetchone() or []

    cursor.close()
    conn.close()

    return user_history

# def compute_user_embedding(user_id: int) -> np.ndarray | None:
#     """
#     根据用户的历史购买记录计算用户的嵌入向量。

#     :param user_id: 用户 ID。
#     :return: 用户嵌入向量，若无记录返回 None。
#     """
#     user_history = get_user_history(user_id)
#     print(user_history)
#     # load embedding
#     product_embeddings = load_product_embeddings(list(user_history))
#     if not product_embeddings:
#         return None  # no product embeddings return None too
    
#     #product_embeddings = ast.literal_eval(product_embeddings.values())
#     print(product_embeddings.values())
#     # compute user embeddings
#     embeddings = np.array(list(product_embeddings.values()))
#     print(embeddings)
#     user_embedding = embeddings.mean(axis=0)

#     return user_embedding

# def update_user_embedding(user_id: int, embedding: np.ndarray) -> None:
#     """
#     更新或插入用户嵌入向量到数据库。

#     :param user_id: 用户 ID。
#     :param embedding: 用户嵌入向量。
#     """
#     conn = get_db_connection()
#     cursor = conn.cursor()

#     cursor.execute(
#         """
#         UPDATE users
#         SET user_embedding = %s
#         WHERE user_id = %s
#         """,
#         (embedding.tolist(), user_id)
#     )

#     conn.commit()
#     cursor.close()
#     conn.close()


def find_similar_users(target_user_id: int, top_k: int = 5) -> list[int]:
    """
    查找与目标用户最相似的用户。

    :param target_user_id: 目标用户 ID。
    :param top_k: 返回的相似用户数量。
    :return: 相似用户的 ID 列表。
    """
    
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT user_id, user_embedding <=> (SELECT user_embedding FROM users WHERE user_id = %s) AS similarity
        FROM users
        WHERE user_id != %s
        ORDER BY similarity
        LIMIT %s
        """,
        (target_user_id, target_user_id, top_k)
    )

    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return [row[0] for row in results] 


def get_user_embedding(user_id: int) -> np.ndarray | None:
    """
    根据用户的历史购买记录计算用户的嵌入向量。

    :param user_id: 用户 ID。
    :return: 用户嵌入向量，若无记录返回 None。
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # query user's embedding
    cursor.execute("SELECT user_embedding FROM users WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()

    cursor.close()
    conn.close()

    return np.array(result[0]) if result else None


def recommend_related_embedding(user_id: int, top_k: int = 5)-> list[int]:
    """
    根据协同过滤为用户推荐商品。

    :param user_id: 用户 ID。
    :param top_k: 推荐商品数量。
    :return: 推荐的商品 ID 列表。
    """
    similar_users = find_similar_users(user_id, top_k=5)
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT product_id
        FROM ratings
        WHERE user_id = %s
        """,
        (user_id,)
    )
    user_purchased = set(row[0] for row in cursor.fetchall())

    recommended_products = set()
    for similar_user_id in similar_users:
        cursor.execute(
            """
            SELECT product_id
            FROM ratings
            WHERE user_id = %s
            """,
            (similar_user_id,)
        )
        similar_user_products = set(row[0] for row in cursor.fetchall())

        recommended_products.update(similar_user_products - user_purchased)

        if len(recommended_products) >= top_k:
            break

    cursor.close()
    conn.close()

    return list(recommended_products)[:top_k]  


def recommend_related(user_id: int, top_k: int = 5) -> list[int]:
    """
    推荐与用户购买同一商品的其他用户购买的商品。
    
    :param user_id: 用户 ID。
    :param top_k: 推荐的商品数量。
    :return: 推荐的商品 ID 列表。
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # 获取目标用户购买的商品列表
    cursor.execute(
        """
        SELECT product_id
        FROM ratings
        WHERE user_id = %s
        """,
        (user_id,)
    )
    user_products = set(row[0] for row in cursor.fetchall())
    
    if not user_products:
        cursor.close()
        conn.close()
        return []  # 如果用户没有购买记录，直接返回空列表

    # 找到购买过同样商品的其他用户
    cursor.execute(
        """
        SELECT DISTINCT user_id
        FROM ratings
        WHERE product_id = ANY(%s) AND user_id != %s
        """,
        (list(user_products), user_id)
    )
    related_users = set(row[0] for row in cursor.fetchall())

    if not related_users:
        cursor.close()
        conn.close()
        return []  # 如果没有其他用户买过相同的商品，返回空列表

    # 找到这些用户购买的商品，排除目标用户已经购买过的商品
    recommended_products = set()
    cursor.execute(
        """
        SELECT product_id
        FROM ratings
        WHERE user_id = ANY(%s)
        """,
        (list(related_users),)
    )
    for row in cursor.fetchall():
        product_id = row[0]
        if product_id not in user_products:  # 排除用户已经购买的商品
            recommended_products.add(product_id)
        if len(recommended_products) >= top_k:
            break

    cursor.close()
    conn.close()

    return list(recommended_products)[:top_k]
# def cosine_similarity(vector_a, vector_b):
#     print(vector_a, )
#     print(vector_b)
#     #compute cosine similarity
#     # if np.linalg.norm(vector_a) == 0 or np.linalg.norm(vector_b) == 0:
#     #     return 0.0  

#     similarity = np.dot(vector_a, vector_b) / (np.linalg.norm(vector_a) * np.linalg.norm(vector_b))
#     return similarity

# def calculate_similarity_from_user_embedding(user_embedding: np.ndarray, search_results: list[dict])-> dict[int, float]:
#     """
#     计算用户嵌入向量与每个搜索结果中商品嵌入的相似度。

#     :param user_embedding: 用户嵌入向量。
#     :param search_results: 初始搜索结果，包含商品信息的列表。
#                            每个元素应包含 'product_id' 键。
#     :return: 一个字典, 键是商品 ID, 值是与用户嵌入的余弦相似度。
#     """
#     #product_ids = [result['product_id'] for result in search_results]
#     product_embeddings = load_product_embeddings(search_results)

#     similarities = {}
#     for result in search_results:
#         #product_id = result['product_id']
#         product_embedding = product_embeddings[result]

#         # 计算用户 embedding 与商品 embedding 的余弦相似度
#         similarity = cosine_similarity(user_embedding, product_embedding)
#         similarities[result] = similarity

#     return similarities
 

# def optimize_search_results_v2(initial_search_results: list[dict], user_id: int)-> list[dict]:
#     """
#     根据用户嵌入优化搜索结果。

#     :param initial_search_results: 初始搜索结果，包含商品信息的列表。
#     :param user_id: 用户 ID。
#     :return: 按优化排序后的搜索结果。
#     """
#     user_embedding = get_user_embedding(user_id)
#     if user_embedding is None:
#         return initial_search_results

#     similarities = calculate_similarity_from_user_embedding(user_embedding, initial_search_results)

#     sorted_results = sorted(initial_search_results, key=lambda x: similarities.get(x['product_id'], 0), reverse=True)
#     return sorted_results


def recommend_embedding(user_id: int, top_k: int=5)-> list[dict]:
    """
    根据用户嵌入优化搜索结果。

    :param initial_search_results: 初始搜索结果，包含商品信息的列表。
    :param user_id: 用户 ID。
    :return: 按优化排序后的搜索结果。
    """
    #user_embedding = get_user_embedding(user_id)
    
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT product_id, title_embedding <=> (SELECT user_embedding FROM users WHERE user_id = %s) AS similarity
        FROM products
        ORDER BY similarity
        LIMIT %s
        """,
        (user_id, top_k)
    )

    results = cursor.fetchall()
    products = set(row[0] for row in results)
    cursor.close()
    conn.close()
    return list(products)

def recommend(user_id, method, top_k=5):
    if method == "related":
        return recommend_related(user_id, top_k)
    elif method == 'related_embedding':
        return recommend_related_embedding(user_id, top_k)
    else:
        return recommend_embedding(user_id, top_k)

