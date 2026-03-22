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
    query = supabase.table("products").select("id,name,url,has_paid_options").eq("active", True)
    if product_id:
        query = query.eq("id", int(product_id))
    return query.execute().data

def get_stock_and_options(url, retry=2):
    """재고 + 유료 옵션 여부 반환"""
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

            # 재고 추출
            stock = None
            match = re.search(
                r'"simpleProductForDetailPage"\s*:\s*\{.*?"stockQuantity"\s*:\s*(\d+)',
                html, re.DOTALL
            )
            if match:
                stock = int(match.group(1))

            # 유료 옵션 감지 (price > 0인 optionCombinations 존재 여부)
            has_paid_options = False
            try:
                state_match = re.search(r'window\.__PRELOADED_STATE__\s*=\s*(\{.+\})\s*</script>', html, re.DOTALL)
                if state_match:
                    state = json.loads(state_match.group(1))
                    spd = state.get('simpleProductForDetailPage', {})
                    for val in spd.values():
                        if isinstance(val, dict) and 'optionCombinations' in val:
                            options = val['optionCombinations']
                            if any(o.get('price', 0) > 0 for o in options):
                                has_paid_options = True
                                break
            except:
                pass

            return stock, has_paid_options

        except Exception as e:
            print(f"  ⚠️ 오류 (시도 {attempt+1}/{retry}): {e}")
            if attempt < retry - 1:
                time.sleep(3)

    return None, False

def crawl_product(product):
    pid  = product["id"]
    name = product["name"]
    url  = product["url"]
    current_has_paid = product.get("has_paid_options", False)

    print(f"  수집 중: {name[:30]}...")
    stock, has_paid_options = get_stock_and_options(url)

    if stock is not None:
        # 재고 저장
        supabase.table("stock_logs").insert({
            "product_id": pid,
            "stock": stock,
            "collected_at": datetime.now(timezone.utc).isoformat()
        }).execute()

        # 유료 옵션 여부 업데이트 (변경됐을 때만)
        if has_paid_options != current_has_paid:
            supabase.table("products").update(
                {"has_paid_options": has_paid_options}
            ).eq("id", pid).execute()
            print(f"  🔄 유료옵션 업데이트: {has_paid_options}")

        flag = "💰" if has_paid_options else "  "
        print(f"  ✅ {flag} {name[:20]}: {stock:,}")
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
