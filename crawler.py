import os
import re
import time
import random
import json
import requests
from datetime import datetime, timezone
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
BRIGHT_KEY   = os.environ["BRIGHT"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_products():
    res = supabase.table("products").select("id,name,url").eq("active", True).execute()
    return res.data

def get_stock(url):
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
            timeout=30
        )
        response.raise_for_status()
        html = response.text

        # __PRELOADED_STATE__ 전체를 JSON으로 파싱
        match = re.search(r'window\.__PRELOADED_STATE__\s*=\s*({.*?})(?:;|\s*</script>)', html, re.DOTALL)
        if match:
            try:
                state = json.loads(match.group(1))
                # simpleProductForDetailPage.A.stockQuantity 에서 읽기
                spd = state.get("simpleProductForDetailPage", {}).get("A", {})
                stock = spd.get("stockQuantity")
                if stock is not None:
                    print(f"  simpleProductForDetailPage 에서 재고: {stock}")
                    return int(stock)
            except Exception as e:
                print(f"  JSON 파싱 실패: {e}")

        # 백업: simpleProductForDetailPage 블록에서 stockQuantity 추출
        match2 = re.search(
            r'"simpleProductForDetailPage"\s*:\s*\{.*?"stockQuantity"\s*:\s*(\d+)',
            html, re.DOTALL
        )
        if match2:
            print(f"  백업 파싱 재고: {match2.group(1)}")
            return int(match2.group(1))

        print(f"  ⚠️ 재고 파싱 실패")
        return None

    except Exception as e:
        print(f"  ⚠️ 수집 실패: {e}")
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
            print(f"  ✅ 재고: {stock:,}")
            success += 1
        else:
            print(f"  ❌ 수집 실패")
            fail += 1

        time.sleep(random.uniform(2, 4))

    print(f"완료 - 성공: {success}개 / 실패: {fail}개")

if __name__ == "__main__":
    main()
