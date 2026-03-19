import os
import re
import time
import random
import requests
from datetime import datetime, timezone

SUPABASE_URL = os.environ["SUPA_URL"]
SUPABASE_KEY = os.environ["SUPA_KEY"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://search.shopping.naver.com/",
}

def get_products():
    """Supabase에서 활성 상품 목록 조회"""
    res = requests.get(
        f"{SUPABASE_URL}/rest/v1/products?active=eq.true&select=id,name,url",
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    )
    res.raise_for_status()
    return res.json()

def get_stock(url):
    """네이버 상품 페이지에서 재고 수집"""
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        res.raise_for_status()
        html = res.text

        # __PRELOADED_STATE__ 에서 stockQuantity 추출
        match = re.search(r'"stockQuantity"\s*:\s*(\d+)', html)
        if match:
            return int(match.group(1))

        # 백업: simpleProductForDetailPage 블록에서 추출
        match2 = re.search(r'simpleProductForDetailPage.*?"stockQuantity"\s*:\s*(\d+)', html, re.DOTALL)
        if match2:
            return int(match2.group(1))

        return None
    except Exception as e:
        print(f"  ⚠️ 수집 실패: {e}")
        return None

def save_log(product_id, stock):
    """Supabase stock_logs에 저장"""
    res = requests.post(
        f"{SUPABASE_URL}/rest/v1/stock_logs",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal"
        },
        json={
            "product_id": product_id,
            "stock": stock,
            "collected_at": datetime.now(timezone.utc).isoformat()
        }
    )
    res.raise_for_status()

def main():
    print(f"🚀 크롤링 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    products = get_products()
    print(f"📦 추적 상품 수: {len(products)}개")

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
            print(f"  ✅ 재고: {stock:,}")
            success += 1
        else:
            print(f"  ❌ 수집 실패")
            fail += 1

        # 3~6초 랜덤 딜레이 (봇 감지 방지)
        time.sleep(random.uniform(3, 6))

    print(f"\n✅ 완료 - 성공: {success}개 / 실패: {fail}개")

if __name__ == "__main__":
    main()
