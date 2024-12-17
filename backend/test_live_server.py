import json
from typing import Any, Dict

import requests

BASE_URL = "http://localhost:8000"


def print_response(response: requests.Response) -> None:
    """Pretty print the response"""
    print(f"Status Code: {response.status_code}")
    try:
        print("Response:", json.dumps(response.json(), indent=2))
    except:
        print("Response:", response.text)
    print("-" * 50)


def test_search(
    keyword: str = "camera",
    exact: bool = False,
    product_id: int = -1,
    backend: str = "psql",
    top_k: int = 5,
) -> Dict[str, Any]:
    """Test the search endpoint"""
    print(f"\nTesting search with keyword: {keyword}")

    response = requests.post(
        f"{BASE_URL}/search",
        params={
            "keyword": keyword,
            "exact": exact,
            "product_id": product_id,
            "backend": backend,
            "top_k": top_k,
        },
    )
    print_response(response)
    return response.json()


def test_ratings(
    product_id: int = None, rating_min: float = None, top_k: int = 5
) -> Dict[str, Any]:
    """Test the ratings endpoint"""
    print("\nTesting ratings endpoint")

    params = {"top_k": top_k}
    if product_id is not None:
        params["product_id"] = product_id
    if rating_min is not None:
        params["rating_min"] = rating_min

    response = requests.get(f"{BASE_URL}/ratings", params=params)
    print_response(response)
    return response.json()


def test_recommend(
    user_id: int = 1, method: str = "related", top_k: int = 5
) -> Dict[str, Any]:
    """Test the recommend endpoint"""
    print(f"\nTesting recommend with method: {method}")

    data = {"user_id": user_id, "method": method, "top_k": top_k}

    response = requests.post(f"{BASE_URL}/recommend", json=data)
    print_response(response)
    return response.json()


def run_all_tests():
    """Run all test cases"""
    try:
        # Test search with different parameters
        test_search(keyword="camera")
        test_search(keyword="digital", backend="elastic")
        test_search(keyword="good", product_id=1)  # Search comments for product_id=1

        # Test ratings with different filters
        test_ratings()
        test_ratings(product_id=1)
        test_ratings(rating_min=4.0)

        # Test recommendations with different methods
        test_recommend(method="related")
        test_recommend(method="related_embedding")
        test_recommend(method="embedding")

    except requests.exceptions.ConnectionError:
        print(
            "Error: Could not connect to the server. Make sure it's running at",
            BASE_URL,
        )
    except Exception as e:
        print(f"Error occurred: {str(e)}")


if __name__ == "__main__":
    run_all_tests()
