// components/ProductCard.tsx
import { useState, useEffect } from 'react'
import { getRatings } from '../services/api'

interface ProductCardProps {
    product: Product
}

export default function ProductCard({ product, handleUserIdClick }: ProductCardProps & { handleUserIdClick: (userId: number) => void }) {
    const [ratings, setRatings] = useState<Review[]>([])
    const [isLoading, setIsLoading] = useState(false)
    const [error, setError] = useState<string | null>(null)
    const [isCommentsVisible, setIsCommentsVisible] = useState(false)

    const amazonUrl = `https://www.amazon.cn/dp/${product.amazon_id}`

    const formatTimestamp = (timestamp: number) => {
        const date = new Date(timestamp * 1000)
        return date.toLocaleDateString() + ' ' + date.toLocaleTimeString()
    }


    const fetchRatings = async () => {
        setIsLoading(true)
        setError(null)
        try {
            const ratingsData = await getRatings(product.product_id, 5, 'psql')
            setRatings(ratingsData)
        } catch (error) {
            setError('评论加载失败')
            console.error('Error loading ratings:', error)
        } finally {
            setIsLoading(false)
        }
    }

    const toggleComments = () => {
        if (!isCommentsVisible) {
            fetchRatings()
        }
        setIsCommentsVisible(!isCommentsVisible)
    }

    return (
        <div className="bg-white rounded-xl shadow-sm hover:shadow-md transition-shadow duration-200 overflow-hidden border border-gray-100">
            <div className="aspect-[4/3] relative bg-gray-100">
                <img
                    src={`https://images-na.ssl-images-amazon.com/images/P/${product.amazon_id}.01.L.jpg`}
                    alt={product.name}
                    className="object-contain w-full h-full p-4"
                    onError={(e) => {
                        (e.target as HTMLImageElement).src = '/placeholder-image.png'
                    }}
                />
            </div>

            <div className="p-4">
                <h3 className="text-lg font-medium text-gray-900 mb-2 line-clamp-2">
                    {product.name}
                </h3>

                <div className="flex justify-between items-center mt-4">
                    <span className="text-sm text-gray-500">
                        ID: {product.product_id}
                    </span>
                    <a
                        href={amazonUrl}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="inline-flex items-center justify-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                    >
                        在亚马逊查看
                    </a>
                </div>

                <div className="mt-4">
                    <h4 className="font-medium text-gray-900 mb-2">评论</h4>

                    {isLoading ? (
                        <div className="text-center text-gray-500">加载评论...</div>
                    ) : error ? (
                        <div className="text-center text-red-500">{error}</div>
                    ) : (
                        <>
                            <button
                                onClick={toggleComments}
                                className="text-blue-600 hover:text-blue-800 mb-4"
                            >
                                {isCommentsVisible ? '收起评论' : '展开评论'}
                            </button>

                            {isCommentsVisible && ratings.length > 0 && (
                                <ul className="space-y-4 max-h-96 overflow-y-auto">
                                    {ratings.map((rating) => (
                                        <li key={`${product.product_id}-${rating.user_id}-${rating.timestamp}`} className="text-sm text-gray-700">
                                            <div className="font-semibold">{rating.title || '无标题'}</div>
                                            <div className="text-sm text-yellow-500">
                                                {'★'.repeat(rating.rating)}{'☆'.repeat(5 - rating.rating)}
                                            </div>
                                            {rating.comment && <p className="mt-1 text-gray-600">{rating.comment}</p>}
                                            <div className="text-xs text-gray-500 mt-1 flex justify-between">
                                                <span>评论时间: {formatTimestamp(rating.timestamp)}</span>
                                                <span onClick={() => handleUserIdClick(parseInt(rating.user_id))}>用户ID: {rating.user_id}</span>
                                            </div>
                                        </li>
                                    ))}
                                </ul>
                            )}

                            {isCommentsVisible && ratings.length === 0 && (
                                <div className="text-center text-gray-500">暂无评论</div>
                            )}
                        </>
                    )}
                </div>
            </div>
        </div>
    )
}
