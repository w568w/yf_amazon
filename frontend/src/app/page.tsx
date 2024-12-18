'use client'

import { useState, useEffect } from 'react'
import ProductCard from './components/ProductCard'
import RatingCard from './components/RatingCard'
import { searchProducts, getRatings, getRecommendations } from './services/api'

interface SearchConfig {
    backend: string
    exact: boolean
    topk: number
    method: string
}

export default function Home() {
    const [products, setProducts] = useState<Product[]>([])
    const [reviews, setReviews] = useState<Review[]>([])
    const [searchTerm, setSearchTerm] = useState('')
    const [productId, setProductId] = useState<number>(-1)
    const [isLoading, setIsLoading] = useState(false)
    const [searchConfig, setSearchConfig] = useState<SearchConfig>({
        backend: 'psql',
        exact: false,
        topk: 10,
        method: 'related',
    })
    const [execTime, setExecTime] = useState<number | null>(null)

    const [view, setView] = useState<'searchProduct' | 'searchReview' | 'recommendation'>('searchProduct')
    const [userId, setUserId] = useState<number>(0)
    const [recommendations, setRecommendations] = useState<any[]>([])

    const handleSearch = async () => {
        setIsLoading(true)
        const startTime = performance.now()

        try {
            if (view === 'searchProduct') {
                const results = await searchProducts({
                    keyword: searchTerm,
                    exact: searchConfig.exact,
                    product_id: -1,
                    backend: searchConfig.backend,
                    topk: searchConfig.topk
                })
                setProducts(results)
            } else if (view === 'searchReview') {
                const results = await getRatings(productId, searchConfig.topk, searchConfig.backend, userId)
                setReviews(results)
            } else if (view === 'recommendation') {
                if (userId > 0) {
                    const results = await getRecommendations(userId, searchConfig.method, searchConfig.topk)
                    setRecommendations(results.recommendations)
                }
            }

            const endTime = performance.now()
            setExecTime(endTime - startTime)
        } catch (error) {
            console.error('Search error:', error)
        } finally {
            setIsLoading(false)
        }
    }

    useEffect(() => {
        setSearchTerm('')
        setProducts([])
        setReviews([])
        setProductId(-1)
        setRecommendations([])
    }, [view])

    const handleUserIdClick = async (id: number) => {
        setView('searchReview')
        setUserId(id)
        setProductId(-1)
        setSearchTerm('')
        setIsLoading(true)
        const startTime = performance.now()
        try {
            const results = await getRatings(productId, searchConfig.topk, searchConfig.backend, userId)
            setReviews(results)
        } catch (error) {
            console.error('Search error:', error)
        } finally {
            setIsLoading(false)
        }
        const endTime = performance.now()
        setExecTime(endTime - startTime)
    }

    return (
        <main className="min-h-screen bg-gradient-to-b from-gray-50 to-white">
            <div className="max-w-7xl mx-auto px-8 py-12">
                <div className="text-center mb-16">
                    <h1 className="text-5xl font-bold text-gray-900 mb-6">
                        商品评价分析系统
                    </h1>

                    <div className="flex justify-center space-x-6 mb-6">
                        <button
                            onClick={() => setView('searchProduct')}
                            className={`px-4 py-2 rounded-lg ${view === 'searchProduct' ? 'bg-blue-500 text-white' : 'bg-gray-100 text-gray-900'}`}
                        >
                            搜索商品
                        </button>
                        <button
                            onClick={() => setView('searchReview')}
                            className={`px-4 py-2 rounded-lg ${view === 'searchReview' ? 'bg-blue-500 text-white' : 'bg-gray-100 text-gray-900'}`}
                        >
                            搜索评论
                        </button>
                        <button
                            onClick={() => setView('recommendation')}
                            className={`px-4 py-2 rounded-lg ${view === 'recommendation' ? 'bg-blue-500 text-white' : 'bg-gray-100 text-gray-900'}`}
                        >
                            推荐商品
                        </button>
                    </div>

                    {view !== 'recommendation' && (
                        <div className="max-w-4xl mx-auto space-y-6">
                            <div className="flex gap-4 justify-center mb-4">
                                <select
                                    value={searchConfig.backend}
                                    onChange={(e) => setSearchConfig({
                                        ...searchConfig,
                                        backend: e.target.value
                                    })}
                                    className="px-4 py-2 rounded-lg border border-gray-200"
                                >
                                    <option value="psql">PostgreSQL</option>
                                    <option value="elastic">Elasticsearch</option>
                                </select>

                                <label className="flex items-center">
                                    <span className="mr-2">Top K:</span>
                                    <input
                                        type="number"
                                        value={searchConfig.topk}
                                        onChange={(e) => setSearchConfig({
                                            ...searchConfig,
                                            topk: parseInt(e.target.value)
                                        })}
                                        className="px-4 py-2 rounded-lg border border-gray-200"
                                        min="1"
                                    />
                                </label>

                                <label className="flex items-center">
                                    <input
                                        type="checkbox"
                                        checked={searchConfig.exact}
                                        onChange={(e) => setSearchConfig({
                                            ...searchConfig,
                                            exact: e.target.checked
                                        })}
                                        className="mr-2"
                                    />
                                    精确匹配
                                </label>
                            </div>

                            <div className="relative">
                                <input
                                    type="text"
                                    placeholder={view === 'searchProduct' ? "搜索商品..." : "请输入搜索内容 (关键字)"}
                                    className="w-full px-8 py-4 rounded-2xl bg-white/70 backdrop-blur-xl border border-gray-200
                    text-lg focus:outline-none focus:ring-2 focus:ring-gray-900 focus:border-transparent
                    transition-all duration-300 shadow-sm"
                                    value={searchTerm}
                                    onChange={(e) => setSearchTerm(e.target.value)}
                                    onKeyPress={(e) => e.key === 'Enter' && handleSearch()}
                                />
                                <button
                                    onClick={handleSearch}
                                    className="absolute right-4 top-1/2 transform -translate-y-1/2 text-gray-400 hover:text-gray-600"
                                >
                                    <svg
                                        width="24"
                                        height="24"
                                        viewBox="0 0 24 24"
                                        fill="none"
                                        stroke="currentColor"
                                        strokeWidth="2"
                                        strokeLinecap="round"
                                        strokeLinejoin="round"
                                    >
                                        <circle cx="11" cy="11" r="8"></circle>
                                        <line x1="21" y1="21" x2="16.65" y2="16.65"></line>
                                    </svg>
                                </button>
                            </div>

                            {view === 'searchReview' && (
                                <div className="flex gap-4 justify-center mb-4">
                                    <div className="flex flex-col gap-2">
                                        <label htmlFor="productId" className="text-sm font-medium text-gray-700">商品ID:</label>
                                        <input
                                            id="productId"
                                            type="number"
                                            value={productId}
                                            onChange={(e) => setProductId(parseInt(e.target.value))}
                                            className="px-4 py-2 rounded-lg border border-gray-200"
                                        />
                                    </div>

                                    <div className="flex flex-col gap-2">
                                        <label htmlFor="userId" className="text-sm font-medium text-gray-700">用户ID (可选):</label>
                                        <input
                                            id="userId"
                                            type="number"
                                            value={userId}
                                            onChange={(e) => setUserId(parseInt(e.target.value))}
                                            className="px-4 py-2 rounded-lg border border-gray-200"
                                        />
                                    </div>
                                </div>
                            )}
                        </div>
                    )}

                    {view === 'recommendation' && (
                        <div className="max-w-4xl mx-auto space-y-6">
                            <div className="flex gap-6 items-center">
                                <div className="flex flex-col gap-2">
                                    <label htmlFor="userId" className="text-sm font-medium text-gray-700">用户ID:</label>
                                    <input
                                        id="userId"
                                        type="number"
                                        value={userId}
                                        onChange={(e) => setUserId(parseInt(e.target.value))}
                                        className="px-4 py-2 rounded-lg border border-gray-200"
                                        min="1"
                                    />
                                </div>

                                <div className="flex flex-col gap-2">
                                    <label htmlFor="method" className="text-sm font-medium text-gray-700">推荐方法:</label>
                                    <select
                                        id="method"
                                        onChange={(e) => setSearchConfig({ ...searchConfig, method: e.target.value })}
                                        className="px-4 py-2 rounded-lg border border-gray-200"
                                    >
                                        <option value="related">Related</option>
                                        <option value="embedding">Embedding</option>
                                        <option value="related_embedding">Related Embedding</option>
                                    </select>
                                </div>

                                <div className="flex flex-col gap-2">
                                    <label htmlFor="topk" className="text-sm font-medium text-gray-700">Top K:</label>
                                    <input
                                        id="topk"
                                        type="number"
                                        value={searchConfig.topk}
                                        onChange={(e) => setSearchConfig({
                                            ...searchConfig,
                                            topk: parseInt(e.target.value)
                                        })}
                                        className="px-4 py-2 rounded-lg border border-gray-200"
                                        min="1"
                                    />
                                </div>

                                <button
                                    onClick={handleSearch}
                                    className="bg-blue-500 text-white px-4 py-2 rounded-lg"
                                >
                                    获取推荐
                                </button>
                            </div>
                        </div>
                    )}
                </div>

                {isLoading ? (
                    <div className="flex justify-center items-center h-64">
                        <div className="animate-spin rounded-full h-12 w-12 border-4 border-gray-900 border-t-transparent"></div>
                    </div>
                ) : (
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                        {view === 'recommendation'
                            ? recommendations.map((product) => (
                                <ProductCard key={product.product_id} product={product} handleUserIdClick={handleUserIdClick} />
                            ))
                            : view === 'searchReview'
                                ? reviews.map((review) => (
                                    <RatingCard key={review.product_id + review.user_id} review={review} />
                                ))
                                : products.map((product) => (
                                    <ProductCard key={product.product_id} product={product} handleUserIdClick={handleUserIdClick} />
                                ))
                        }
                    </div>
                )}

                {execTime && (
                    <div className="text-center mt-8 text-gray-500">
                        耗时: {execTime.toFixed(2)}ms
                    </div>
                )}
            </div>
        </main>
    )
}
