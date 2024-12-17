// types/index.ts
interface Product {
  name: string
  product_id: number
  amazon_id: string
}

interface Review {
  user_id: string
  product_id: number
  comment: string
  rating: number
  title: string
  timestamp: number
}

