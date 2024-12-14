from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import numpy as np
from recommend import recommend

app = FastAPI()

# 请求体的定义
class RecommendRequest(BaseModel):
    user_id: int
    method: str
    top_k: Optional[int] = 5  # 默认 5 个推荐商品

# 响应体的定义
class RecommendResponse(BaseModel):
    user_id: int
    method: str
    recommendations: List[int]

@app.post("/recommend", response_model=RecommendResponse)
async def recommend_api(request: RecommendRequest):
    """
    POST 接口：根据用户 ID 和推荐方法返回推荐结果。
    """
    try:
        user_id = request.user_id
        method = request.method
        top_k = request.top_k

        # 根据 method 调用不同的推荐函数
        recommendations = recommend(user_id, method, top_k)

        # 返回推荐结果
        return RecommendResponse(
            user_id=user_id,
            method=method,
            recommendations=recommendations
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
