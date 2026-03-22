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

def get_base_stock_from_options(html):
    """optionCombinations에서 price==0인 옵션 재고 합산"""
    if 'optionCombinations' not in html:
        return None
    try:
        # PRELOADED_STATE 전체 파싱
        state_match = re.search(r'window\.__PRELOADED_STATE__\s*=\s*(\{.+\})\s*</script>', html, re.DOTALL)
        if state_match:
            state = json.loads(state_match.group(1))
            spd = state.get('simpleProductForDetailPage', {})
            for val in spd.values():
                if isinstance(val, dict) and 'optionCombinations' in val:
                    options = val['optionCombinations']
                    if options:
                        base = [o for o in options if o.get('price', -1) == 0 and o.get('stockQuantity') is not None]
                        if base:
                            total = sum(o['stockQuantity'] for o in base)
                            print(f"  📦 기본옵션 {len(base)}개 합산: {total}")
                            return total
                        min_p = min(o.get('price', 0) for o in options)
                        base = [o for o in options if o.get('price') == min_p]
                        total = sum(o.get('stockQuantity', 0) for o in base)
                        print(f"  📦 최저가옵션({min_p}원) {len(base)}개 합산: {total}")
                        return total
    except Exception as e:
        print(f"  ⚠️ 옵션 파싱 실패: {e}")
    return None

def get_stock(url, retry=2):
    product_no = extract_product_no(url)

    for attempt in range(retry):
        try:
            # JS 렌더링 ON으로 요청
            response = requests.post(
                "https://api.brightdata.com/request",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {BRIGHT_KEY}"
                },
                json={
                    "zone": "web_unlocker1",
                    "url": url,
                    "format": "raw",
                    "render_js": True  # JS 렌더링 활성화
                },
                timeout=90  # JS 렌더링은 더 오래 걸림
            )
            response.raise_for_status()
            html = response.text

            has_options = 'optionCombinations' in html
            print(f"  🔍 optionCombinations 존재: {has_options}")

            # 1순위: 옵션별 재고 (price==0)
            stock = get_base_stock_from_options(html)
            if stock is not None:
                return stock

            # 2순위: 폴백
            match = re.search(
                r'"simpleProductForDetailPage"\s*:\s*\{.*?"stockQuantity"\s*:\s*(\d+)',
                html, re.DOTALL
            )
            if match:
                val = int(match.group(1))
                print(f"  📦 폴백 재고: {val}")
                return val

            print(f"  ⚠️ 파싱 실패 (시도 {attempt+1}/{retry})")

        except Exception as e:
            print(f"  ⚠️ 오류 (시도 {attempt+1}/{retry}): {e}")
            if attempt < retry - 1:
                time.sleep(3)

    return None

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
