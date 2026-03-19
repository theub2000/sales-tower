import os
import re
import time
import random
import requests
from datetime import datetime, timezone
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

NAVER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://search.shopping.naver.com/",
}

def get_products():
    res = supabase.table("products").select("id,name,url").eq("active", True).execute()
    return res.data

def get_stock(url):
    try:
        res = requests.get(url, headers=NAVER_HEADERS, timeout=10)
        res.raise_for_status()
        html = res.text
        match = re.search(r'"stockQuantity"\s*:\s*(\d+)', html)
        if match:
            return int(match.group(1))
        match2 = re.search(r'simpleProductForDetailPage.*?"stockQuantity"\s*:\s*(\d+)', html, re.DOTALL)
        if match2:
            return int(match2.group(1))
        return None
    except Exception as e:
        print(f"  수집 실패: {e}")
        return None

def save_log(product_id, stock):
    supabase.table("stock_logs").insert({
        "product_id": product_id,
        "stock": stock,
        "collected_at": datetime.now(timezone.utc).isoformat()
    }).execute()

def main():
    print(f"크롤링 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    products = get_products()
    print(f"추적 상품 수: {len(products)}개")

    success = 0
    fail = 0

    for product in products:
        pid  = product["id"]
        name = product["name"]
        url  = product["url"]

        print(f"  수집 중: {name[:30]}...")
        stock = get_stock(url)

        if stock is not None:
            save_log(pid, stock)
            print(f"  재고: {stock:,}")
            success += 1
        else:
            print(f"  수집 실패")
            fail += 1

        time.sleep(random.uniform(3, 6))

    print(f"완료 - 성공: {success}개 / 실패: {fail}개")

if __name__ == "__main__":
    main()
