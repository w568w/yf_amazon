import React from 'react';

interface Review {
    product_id: number;
    user_id: number;
    rating: number;
    timestamp: number;
    title: string;
    comment: string;
    name: string;
}

const RatingCard: React.FC<{ review: Review }> = ({ review }) => {
    const formattedDate = new Date(review.timestamp * 1000).toLocaleDateString();

    return (
        <div className="bg-white p-4 rounded-lg shadow-lg border border-gray-200 space-y-4">
            <div className="flex items-center justify-between">
                <h3 className="text-lg font-semibold text-gray-900">{review.title}</h3>
                <div className="flex items-center">
                    <span className="text-yellow-500">{'★'.repeat(review.rating)}</span>
                    <span className="ml-2 text-gray-500">({review.rating})</span>
                </div>
            </div>

            <div className="text-sm text-gray-500">
                <span>{review.name}</span>
            </div>

            <p className="text-gray-700">{review.comment}</p>

            <div className="flex justify-between text-sm text-gray-500">
                <span>用户ID: {review.user_id}</span>
                <span>评论时间: {formattedDate}</span>
            </div>
        </div>
    );
};

export default RatingCard;
