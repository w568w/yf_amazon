// app/services/api.ts
const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8000'

interface SearchParams {
  keyword: string
  exact: boolean
  product_id: number
  backend: string
  topk: number
}

export async function searchProducts(params: SearchParams) {
  const queryParams = new URLSearchParams({
    keyword: params.keyword,
    exact: params.exact.toString(),
    product_id: params.product_id.toString(),
    backend: params.backend,
    topk: params.topk.toString()
  })

  try {
    const response = await fetch(`${API_BASE_URL}/search?${queryParams}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      }
    })

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`)
    }

    return await response.json()
  } catch (error) {
    console.error('API error:', error)
    throw error
  }
}

export async function getRatings(productId: number, topk: number, backend: string, userId?: number) {
  const queryParams = new URLSearchParams({
    product_id: productId.toString(),
    topk: topk.toString(),
    backend: backend,
  })
  if (userId) {
    queryParams.append('user_id', userId.toString())
  }

  try {
    const response = await fetch(`${API_BASE_URL}/ratings?${queryParams}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      }
    })

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`)
    }

    return await response.json()
  } catch (error) {
    console.error('API error:', error)
    throw error
  }
}

export async function getRecommendations(userId: number, method: string, topk: number, backend: string) {
  const body = {
    user_id: userId,
    method: method,
    topk: topk,
    backend: backend
  }

  try {
    const response = await fetch(`${API_BASE_URL}/recommend`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body)
    })

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`)
    }

    return await response.json()
  } catch (error) {
    console.error('API error:', error)
    throw error
  }
}