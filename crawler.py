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
    query = supabase.table("products").select("id,name,url,channel_uid,is_brand,product_no").eq("active", True)
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
        if channel_uid and product_no:
            return {"channel_uid": channel_uid, "product_no": product_no, "is_brand": is_brand}
    except:
        pass
    return None

def parse_options_from_html(html):
    """상품 페이지 HTML의 __PRELOADED_STATE__에서 옵션/추가상품 직접 파싱"""
    if not html:
        return None
    try:
        parts = html.split('window.__PRELOADED_STATE__=')
        if len(parts) < 2:
            return None
        json_str = parts[1].split('</script>')[0]
        json_str = re.sub(r':\s*undefined\b', ': null', json_str)
        state = json.loads(json_str)
        p = state.get("simpleProductForDetailPage", {}).get("A") or state.get("product", {}).get("A") or {}

        options = p.get("optionCombinations") or []
        supplements = p.get("supplementProducts") or []

        if not options and not supplements:
            return None

        return {"options": options, "supplements": supplements}
    except Exception as e:
        print(f"  ⚠️ 옵션 파싱 실패: {e}")
        return None

def save_options(pid, v2_data):
    """옵션/추가상품 DB 저장"""
    now = datetime.now(timezone.utc).isoformat()
    opt_count = 0
    sup_count = 0

    for opt in v2_data.get("options", []):
        if not opt.get("id"):
            continue
        existing = supabase.table("product_options").select("id").eq("product_id", pid).eq("naver_option_id", opt["id"]).limit(1).execute().data
        if not existing:
            names = []
            for i in range(1, 6):
                n = opt.get(f"optionName{i}")
                if n:
                    names.append(n)
            try:
                supabase.table("product_options").insert({
                    "product_id": pid,
                    "naver_option_id": opt["id"],
                    "option_name": " / ".join(names) or "default",
                    "price": opt.get("price", 0)
                }).execute()
            except:
                pass

        if opt.get("stockQuantity") is not None:
            try:
                supabase.table("option_stock_logs").insert({
                    "product_id": pid,
                    "naver_option_id": opt["id"],
                    "stock": opt["stockQuantity"],
                    "collected_at": now
                }).execute()
                opt_count += 1
            except:
                pass

    for sup in v2_data.get("supplements", []):
        if not sup.get("id"):
            continue
        existing = supabase.table("product_supplements").select("id").eq("product_id", pid).eq("naver_supplement_id", sup["id"]).limit(1).execute().data
        if not existing:
            try:
                supabase.table("product_supplements").insert({
                    "product_id": pid,
                    "naver_supplement_id": sup["id"],
                    "group_name": sup.get("groupName", ""),
                    "name": sup.get("name", ""),
                    "price": sup.get("price", 0)
                }).execute()
            except:
                pass

        if sup.get("stockQuantity") is not None:
            try:
                supabase.table("supplement_stock_logs").insert({
                    "product_id": pid,
                    "naver_supplement_id": sup["id"],
                    "stock": sup["stockQuantity"],
                    "collected_at": now
                }).execute()
                sup_count += 1
            except:
                pass

    return opt_count, sup_count

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
        print(f"  ✅ {name[:20]}: 총재고 {stock:,}")

        if not product.get("channel_uid") and html:
            info = parse_product_info(html)
            if info:
                try:
                    supabase.table("products").update({
                        "channel_uid": info["channel_uid"],
                        "is_brand": info["is_brand"],
                        "product_no": info["product_no"]
                    }).eq("id", pid).execute()
                    product["channel_uid"] = info["channel_uid"]
                    product["is_brand"] = info["is_brand"]
                    product["product_no"] = info["product_no"]
                    print(f"  📝 상품 정보 저장됨")
                except:
                    pass

        v2_data = parse_options_from_html(html)
        if v2_data:
            oc, sc = save_options(pid, v2_data)
            print(f"  📦 옵션 {oc}개 · 추가상품 {sc}개 수집")

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
