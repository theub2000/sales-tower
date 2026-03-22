import os
import re
import json
import time
import requests
from datetime import datetime, timezone
from supabase import create_client
from concurrent.futures import ThreadPoolExecutor, as_completed

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
BRIGHT_KEY   = os.environ["BRIGHT"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_products(product_id=None):
    query = supabase.table("products").select("id,name,url").eq("active", True)
    if product_id:
        query = query.eq("id", int(product_id))
    return query.execute().data

def extract_product_no(url):
    match = re.search(r'/products/(\d+)', url)
    return match.group(1) if match else None

def bright_get(url):
    """Bright Data로 URL 호출"""
    response = requests.post(
        "https://api.brightdata.com/request",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {BRIGHT_KEY}"
        },
        json={"zone": "web_unlocker1", "url": url, "format": "raw"},
        timeout=60
    )
    response.raise_for_status()
    return response

def get_stock_via_options_api(product_no, is_brand):
    """옵션 API로 price==0인 옵션 재고 합산"""
    if is_brand:
        api_url = f"https://brand.naver.com/n/v1/products/{product_no}/options"
    else:
        api_url = f"https://smartstore.naver.com/i/v1/products/{product_no}/options"

    try:
        res = bright_get(api_url)
        print(f"  🔍 옵션 API 상태: {res.status_code}")
        data = res.json()
        print(f"  🔍 옵션 API 응답 샘플: {str(data)[:200]}")

        # optionCombinations 탐색
        options = None
        if isinstance(data, list):
            options = data
        elif isinstance(data, dict):
            options = data.get('optionCombinations') or \
                      data.get('options') or \
                      data.get('combinations')

        if options:
            base = [o for o in options if o.get('price', -1) == 0 and o.get('stockQuantity') is not None]
            if base:
                total = sum(o['stockQuantity'] for o in base)
                print(f"  📦 기본옵션 {len(base)}개 합산: {total}")
                return total
            min_p = min(o.get('price', 0) for o in options)
            base = [o for o in options if o.get('price') == min_p]
            total = sum(o.get('stockQuantity', 0) for o in base)
            print(f"  📦 최저가({min_p}원) {len(base)}개 합산: {total}")
            return total
    except Exception as e:
        print(f"  ⚠️ 옵션 API 오류: {e}")
    return None

def get_stock_via_html(url):
    """HTML 폴백 - stockQuantity 합산"""
    try:
        res = bright_get(url)
        html = res.text
        match = re.search(
            r'"simpleProductForDetailPage"\s*:\s*\{.*?"stockQuantity"\s*:\s*(\d+)',
            html, re.DOTALL
        )
        if match:
            val = int(match.group(1))
            print(f"  📦 HTML 폴백: {val}")
            return val
    except Exception as e:
        print(f"  ⚠️ HTML 오류: {e}")
    return None

def get_stock(url):
    product_no = extract_product_no(url)
    is_brand = 'brand.naver.com' in url

    if product_no:
        stock = get_stock_via_options_api(product_no, is_brand)
        if stock is not None:
            return stock

    print(f"  ⚠️ 옵션 API 실패, HTML 폴백")
    return get_stock_via_html(url)

def crawl_product(product):
    pid  = product["id"]
    name = product["name"]
    url  = product["url"]

    print(f"  수집 중: {name[:30]}...")
    stock = get_stock(url)

    if stock is not None:
        supabase.table("stock_logs").insert({
            "product_id": pid,
            "stock": stock,
            "collected_at": datetime.now(timezone.utc).isoformat()
        }).execute()
        print(f"  ✅ {name[:20]}: {stock:,}")
        return True
    else:
        print(f"  ❌ {name[:20]}: 수집 실패")
        return False

def main():
    product_id = os.environ.get("PRODUCT_ID", "").strip() or None
    print(f"크롤링 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if product_id:
        print(f"단일 상품 수집 (ID: {product_id})")

    products = get_products(product_id)
    print(f"추적 상품 수: {len(products)}개")

    success = 0
    fail = 0

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(crawl_product, p): p for p in products}
        for future in as_completed(futures):
            if future.result():
                success += 1
            else:
                fail += 1

    print(f"\n완료 - 성공: {success}개 / 실패: {fail}개")

if __name__ == "__main__":
    main()
