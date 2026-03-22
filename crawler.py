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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Referer": "https://smartstore.naver.com/",
}

def get_products(product_id=None):
    query = supabase.table("products").select("id,name,url").eq("active", True)
    if product_id:
        query = query.eq("id", int(product_id))
    return query.execute().data

def extract_product_no(url):
    """URL에서 productNo 추출"""
    match = re.search(r'/products/(\d+)', url)
    return match.group(1) if match else None

def get_stock_via_api(url):
    """네이버 상품 API 직접 호출"""
    product_no = extract_product_no(url)
    if not product_no:
        return None

    # brand.naver.com 또는 smartstore.naver.com 구분
    if 'brand.naver.com' in url:
        api_url = f"https://brand.naver.com/n/v1/products/{product_no}"
    else:
        api_url = f"https://smartstore.naver.com/i/v1/products/{product_no}"

    try:
        res = requests.get(api_url, headers=HEADERS, timeout=10)
        print(f"  🔍 API 상태코드: {res.status_code}")
        if res.status_code == 200:
            data = res.json()
            # optionCombinations에서 price==0인 것만 합산
            options = data.get('optionCombinations') or \
                      data.get('detailAttribute', {}).get('optionInfo', {}).get('optionCombinations', [])
            if options:
                base = [o for o in options if o.get('price', -1) == 0 and o.get('stockQuantity') is not None]
                if base:
                    total = sum(o['stockQuantity'] for o in base)
                    print(f"  📦 API 기본옵션 {len(base)}개 합산: {total}")
                    return total
                # price==0 없으면 최저가
                min_p = min(o.get('price', 0) for o in options)
                base = [o for o in options if o.get('price') == min_p]
                total = sum(o.get('stockQuantity', 0) for o in base)
                print(f"  📦 API 최저가({min_p}원) {len(base)}개 합산: {total}")
                return total
            # 옵션 없으면 stockQuantity
            stock = data.get('stockQuantity')
            if stock is not None:
                print(f"  📦 API 단일 재고: {stock}")
                return stock
        else:
            print(f"  ⚠️ API 실패: {res.status_code}")
    except Exception as e:
        print(f"  ⚠️ API 오류: {e}")
    return None

def get_stock_via_brightdata(url, retry=2):
    """Bright Data 폴백"""
    for attempt in range(retry):
        try:
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
            html = response.text
            match = re.search(
                r'"simpleProductForDetailPage"\s*:\s*\{.*?"stockQuantity"\s*:\s*(\d+)',
                html, re.DOTALL
            )
            if match:
                val = int(match.group(1))
                print(f"  📦 BrightData 폴백: {val}")
                return val
        except Exception as e:
            print(f"  ⚠️ BrightData 오류 (시도 {attempt+1}/{retry}): {e}")
            if attempt < retry - 1:
                time.sleep(3)
    return None

def get_stock(url):
    # 1순위: 네이버 API 직접 호출
    stock = get_stock_via_api(url)
    if stock is not None:
        return stock
    # 2순위: Bright Data 폴백
    print(f"  ⚠️ API 실패, BrightData 폴백 시도")
    return get_stock_via_brightdata(url)

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
