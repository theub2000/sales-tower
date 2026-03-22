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

def get_base_option_stock(html):
    """옵션가 +0원인 기본 옵션 재고만 합산"""
    try:
        # optionCombinations 배열 추출 (여러 패턴 시도)
        patterns = [
            r'"optionCombinations"\s*:\s*(\[.*?\])\s*,\s*"',
            r'"optionCombinations"\s*:\s*(\[[\s\S]+?\])\s*,\s*"(?:stock|price|id|use)',
        ]
        
        options = None
        for pattern in patterns:
            match = re.search(pattern, html)
            if match:
                try:
                    options = json.loads(match.group(1))
                    break
                except:
                    continue

        if not options:
            # PRELOADED_STATE에서 직접 추출 시도
            state_match = re.search(r'window\.__PRELOADED_STATE__\s*=\s*(\{[\s\S]+?\})\s*</script>', html)
            if state_match:
                try:
                    state = json.loads(state_match.group(1))
                    # simpleProductForDetailPage 내 옵션 탐색
                    for key in state.get('simpleProductForDetailPage', {}).values():
                        if isinstance(key, dict) and 'optionCombinations' in key:
                            options = key['optionCombinations']
                            break
                except:
                    pass

        if not options:
            return None

        # price == 0인 옵션만 필터링
        base_options = [o for o in options 
                       if isinstance(o, dict) 
                       and o.get('price', -1) == 0 
                       and o.get('stockQuantity') is not None]
        
        if base_options:
            total = sum(o['stockQuantity'] for o in base_options)
            print(f"  📦 기본옵션 {len(base_options)}개 합산: {total}")
            return total
        
        # price==0인 게 없으면 최저가 옵션들만 추적
        prices = [o.get('price', 0) for o in options if isinstance(o, dict)]
        if prices:
            min_price = min(prices)
            min_options = [o for o in options if isinstance(o, dict) and o.get('price') == min_price]
            total = sum(o.get('stockQuantity', 0) for o in min_options)
            print(f"  📦 최저가옵션({min_price}원) {len(min_options)}개 합산: {total}")
            return total

    except Exception as e:
        print(f"  ⚠️ 옵션 파싱 실패: {e}")
    return None

def get_stock(url, retry=2):
    for attempt in range(retry):
        try:
            response = requests.post(
                "https://api.brightdata.com/request",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {BRIGHT_KEY}"
                },
                json={
                    "zone": "web_unlocker1",
                    "url": url,
                    "format": "raw"
                },
                timeout=60
            )
            response.raise_for_status()
            html = response.text

            # 1순위: 기본옵션(+0원) 재고 합산
            stock = get_base_option_stock(html)
            if stock is not None:
                return stock

            # 2순위: 폴백 - simpleProductForDetailPage stockQuantity
            match = re.search(
                r'"simpleProductForDetailPage"\s*:\s*\{.*?"stockQuantity"\s*:\s*(\d+)',
                html, re.DOTALL
            )
            if match:
                print(f"  📦 폴백 재고 사용")
                return int(match.group(1))

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
