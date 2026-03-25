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
    """Bright Data로 HTML → stockQuantity + HTML 반환"""
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
    """HTML에서 channelUid, productNo, is_brand 추출"""
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
    except Exception as e:
        print(f"  ⚠️ 상품 정보 파싱 실패: {e}")
    return None

def fetch_v2_api(channel_uid, product_no, is_brand):
    """v2 API → optionCombinations + supplementProducts (Bright Data 안 씀)"""
    paths = [
        f"https://smartstore.naver.com/i/v2/channels/{channel_uid}/products/{product_no}?withWindow=false",
        f"https://brand.naver.com/n/v2/channels/{channel_uid}/products/{product_no}?withWindow=false",
    ] if not is_brand else [
        f"https://brand.naver.com/n/v2/channels/{channel_uid}/products/{product_no}?withWindow=false",
        f"https://smartstore.naver.com/i/v2/channels/{channel_uid}/products/{product_no}?withWindow=false",
    ]

    for api_url in paths:
        try:
            resp = requests.get(api_url, headers={
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }, timeout=15)
            if resp.status_code == 200 and resp.text.startswith("{"):
                data = resp.json()
                return {
                    "options": data.get("optionCombinations", []),
                    "supplements": data.get("supplementProducts", []),
                    "stock": data.get("stockQuantity")
                }
        except Exception as e:
            print(f"  ⚠️ API 실패 ({api_url[:50]}...): {e}")
    return None

def upsert_options(product_id, options):
    """옵션 마스터 upsert"""
    for opt in options:
        naver_id = opt.get("id")
        if not naver_id:
            continue
        names = [opt.get(f"optionName{i}") for i in range(1, 6) if opt.get(f"optionName{i}")]
        option_name = " / ".join(names) if names else "기본"
        try:
            existing = supabase.table("product_options").select("id").eq("product_id", product_id).eq("naver_option_id", naver_id).execute().data
            if not existing:
                supabase.table("product_options").insert({
                    "product_id": product_id,
                    "naver_option_id": naver_id,
                    "option_name": option_name,
                    "price": opt.get("price", 0)
                }).execute()
        except:
            pass

def upsert_supplements(product_id, supplements):
    """추가상품 마스터 upsert"""
    for sup in supplements:
        naver_id = sup.get("id")
        if not naver_id:
            continue
        try:
            existing = supabase.table("product_supplements").select("id").eq("product_id", product_id).eq("naver_supplement_id", naver_id).execute().data
            if not existing:
                supabase.table("product_supplements").insert({
                    "product_id": product_id,
                    "naver_supplement_id": naver_id,
                    "group_name": sup.get("groupName", ""),
                    "name": sup.get("name", ""),
                    "price": sup.get("price", 0)
                }).execute()
        except:
            pass

def save_option_logs(product_id, options):
    """옵션별 재고 로그 저장"""
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for opt in options:
        naver_id = opt.get("id")
        stock = opt.get("stockQuantity")
        if naver_id and stock is not None:
            rows.append({"product_id": product_id, "naver_option_id": naver_id, "stock": stock, "collected_at": now})
    if rows:
        supabase.table("option_stock_logs").insert(rows).execute()

def save_supplement_logs(product_id, supplements):
    """추가상품별 재고 로그 저장"""
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for sup in supplements:
        naver_id = sup.get("id")
        stock = sup.get("stockQuantity")
        if naver_id and stock is not None:
            rows.append({"product_id": product_id, "naver_supplement_id": naver_id, "stock": stock, "collected_at": now})
    if rows:
        supabase.table("supplement_stock_logs").insert(rows).execute()

def crawl_product(product):
    pid  = product["id"]
    name = product["name"]
    url  = product["url"]

    print(f"  수집 중: {name[:30]}...")

    # ── STEP 1: 기존 방식 — HTML에서 총재고 수집 ──
    stock, html = get_stock(url)

    if stock is not None:
        supabase.table("stock_logs").insert({
            "product_id": pid,
            "stock": stock,
            "collected_at": datetime.now(timezone.utc).isoformat()
        }).execute()
        print(f"  ✅ {name[:20]}: 총재고 {stock:,}")
    else:
        print(f"  ❌ {name[:20]}: 수집 실패")
        return False

    # ── STEP 2: 상품 정보 추출 (최초 1회 → DB 저장) ──
    channel_uid = product.get("channel_uid")
    is_brand = product.get("is_brand", False)
    product_no = product.get("product_no")

    if not channel_uid or not product_no:
        if html:
            info = parse_product_info(html)
            if info:
                channel_uid = info["channel_uid"]
                product_no = info["product_no"]
                is_brand = info["is_brand"]
                try:
                    supabase.table("products").update({
                        "channel_uid": channel_uid,
                        "is_brand": is_brand,
                        "product_no": product_no
                    }).eq("id", pid).execute()
                    print(f"  📝 상품 정보 저장됨")
                except:
                    pass
            else:
                print(f"  ⚠️ 상품 정보 추출 실패 (기본 재고만 저장)")
                return True

    if not channel_uid or not product_no:
        return True

    # ── STEP 3: v2 API → 옵션별 + 추가상품별 재고 ──
    v2 = fetch_v2_api(channel_uid, product_no, is_brand)
    if not v2:
        print(f"  ⚠️ v2 API 실패 (기본 재고만 저장)")
        return True

    options = v2["options"]
    supplements = v2["supplements"]

    if options:
        upsert_options(pid, options)
        save_option_logs(pid, options)
        print(f"  📊 옵션 {len(options)}개 저장")

    if supplements:
        upsert_supplements(pid, supplements)
        save_supplement_logs(pid, supplements)
        print(f"  🛒 추가상품 {len(supplements)}개 저장")

    return True

def main():
    product_id = os.environ.get("PRODUCT_ID", "").strip() or None
    print(f"═══ Sales Tower 크롤러 v2.0 (옵션+추가상품) ═══")
    print(f"시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if product_id:
        print(f"단일 상품 수집 (ID: {product_id})")

    products = get_products(product_id)
    print(f"추적 상품 수: {len(products)}개\n")

    success = 0
    fail = 0

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(crawl_product, p): p for p in products}
        for future in as_completed(futures):
            if future.result():
                success += 1
            else:
                fail += 1

    print(f"\n═══ 완료 - 성공: {success}개 / 실패: {fail}개 ═══")

if __name__ == "__main__":
    main()
