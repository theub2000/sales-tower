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
    query = supabase.table("products").select("id,name,url,channel_uid,is_brand,product_no,arrival_guarantee").eq("active", True)
    if product_id:
        query = query.eq("id", int(product_id))
    return query.execute().data

def get_stock(url, retry=2):
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
                return int(match.group(1)), html

            print(f"  ⚠️ 파싱 실패 (시도 {attempt+1}/{retry})")

        except Exception as e:
            print(f"  ⚠️ 오류 (시도 {attempt+1}/{retry}): {e}")
            if attempt < retry - 1:
                time.sleep(3)

    return None, None

def parse_product_info(html):
    try:
        parts = html.split('window.__PRELOADED_STATE__=')
        if len(parts) < 2:
            return None
        json_str = parts[1].split('</script>')[0]
        json_str = re.sub(r':\s*undefined\b', ': null', json_str)
        state = json.loads(json_str)
        p = state.get("simpleProductForDetailPage", {}).get("A") or state.get("product", {}).get("A") or {}
        ch = p.get("channel", {})
        channel_uid = ch.get("channelUid", "")
        product_no = str(p.get("id") or p.get("productNo") or "")
        is_brand = "brand.naver.com" in html or ch.get("storeExhibitionType") == "BRAND_STORE"

        # N배송 판별: arrivalGuarantee 또는 deliveryAttributeType으로 확인
        delivery = p.get("productDeliveryInfo", {})
        arrival_guarantee = (
            p.get("arrivalGuarantee") is True
            or delivery.get("deliveryAttributeType") == "ARRIVAL_GUARANTEE"
        )

        if channel_uid and product_no:
            return {
                "channel_uid": channel_uid,
                "product_no": product_no,
                "is_brand": is_brand,
                "arrival_guarantee": arrival_guarantee
            }
    except:
        pass
    return None

def crawl_product(product):
    pid  = product["id"]
    name = product["name"]
    url  = product["url"]

    print(f"  수집 중: {name[:30]}...")
    stock, html = get_stock(url)

    if stock is not None:
        supabase.table("stock_logs").insert({
            "product_id": pid,
            "stock": stock,
            "collected_at": datetime.now(timezone.utc).isoformat()
        }).execute()
        print(f"  ✅ {name[:20]}: {stock:,}")

        if not product.get("channel_uid") and html:
            info = parse_product_info(html)
            if info:
                try:
                    supabase.table("products").update({
                        "channel_uid": info["channel_uid"],
                        "is_brand": info["is_brand"],
                        "product_no": info["product_no"],
                        "arrival_guarantee": info["arrival_guarantee"]
                    }).eq("id", pid).execute()
                    print(f"  📝 상품 정보 저장됨 (N배송: {info['arrival_guarantee']})")
                except:
                    pass
        elif product.get("arrival_guarantee") is None and html:
            # 기존 상품이지만 arrival_guarantee가 아직 없으면 업데이트
            info = parse_product_info(html)
            if info:
                try:
                    supabase.table("products").update({
                        "arrival_guarantee": info["arrival_guarantee"]
                    }).eq("id", pid).execute()
                    print(f"  📝 N배송 정보 업데이트: {info['arrival_guarantee']}")
                except:
                    pass

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
