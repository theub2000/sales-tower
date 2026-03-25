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

# Residential Proxy (South Korea) — v2 API용
PROXY_HOST = "brd.superproxy.io:33335"
PROXY_USER = "brd-customer-hl_f2f1b232-zone-residential_proxy2-country-kr"
PROXY_PASS = os.environ.get("BRIGHT_PROXY_PASS", "ubjp8ry1z4j6")

PROXIES = {
    "http":  f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}",
    "https": f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}",
}

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_products(product_id=None):
    query = supabase.table("products").select("id,name,url,channel_uid,is_brand,product_no").eq("active", True)
    if product_id:
        query = query.eq("id", int(product_id))
    return query.execute().data

def bright_fetch_html(url, retry=2):
    """Bright Data web_unlocker로 HTML 가져오기 (기존)"""
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
            return response.text
        except Exception as e:
            print(f"  ⚠️ HTML 오류 (시도 {attempt+1}/{retry}): {e}")
            if attempt < retry - 1:
                time.sleep(3)
    return None

def proxy_fetch_json(url, retry=2):
    """Residential Proxy (한국 IP)로 JSON API 호출"""
    for attempt in range(retry):
        try:
            resp = requests.get(url, proxies=PROXIES, headers={
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }, timeout=20, verify=False)
            if resp.status_code == 200 and resp.text.strip().startswith("{"):
                return resp.json()
        except Exception as e:
            print(f"  ⚠️ Proxy 오류 (시도 {attempt+1}/{retry}): {e}")
            if attempt < retry - 1:
                time.sleep(2)
    return None

def parse_stock_and_info(html):
    """HTML에서 stockQuantity + channelUid + productNo 추출"""
    stock_match = re.search(
        r'"simpleProductForDetailPage"\s*:\s*\{.*?"stockQuantity"\s*:\s*(\d+)',
        html, re.DOTALL
    )
    stock = int(stock_match.group(1)) if stock_match else None

    info = None
    try:
        parts = html.split('window.__PRELOADED_STATE__=')
        if len(parts) >= 2:
            json_str = parts[1].split('</script>')[0]
            json_str = re.sub(r':\s*undefined\b', ': null', json_str)
            state = json.loads(json_str)
            p = state.get("simpleProductForDetailPage", {}).get("A") or state.get("product", {}).get("A") or {}
            ch = p.get("channel", {})
            channel_uid = ch.get("channelUid", "")
            product_no = str(p.get("id") or p.get("productNo") or "")
            is_brand = "brand.naver.com" in html or ch.get("storeExhibitionType") == "BRAND_STORE"
            if channel_uid and product_no:
                info = {"channel_uid": channel_uid, "product_no": product_no, "is_brand": is_brand}
    except:
        pass

    return stock, info

def fetch_v2_api(channel_uid, product_no, is_brand):
    """v2 API → 옵션 + 추가상품 (Residential Proxy 경유)"""
    urls = [
        f"https://brand.naver.com/n/v2/channels/{channel_uid}/products/{product_no}?withWindow=false",
        f"https://smartstore.naver.com/i/v2/channels/{channel_uid}/products/{product_no}?withWindow=false",
    ] if is_brand else [
        f"https://smartstore.naver.com/i/v2/channels/{channel_uid}/products/{product_no}?withWindow=false",
        f"https://brand.naver.com/n/v2/channels/{channel_uid}/products/{product_no}?withWindow=false",
    ]

    for api_url in urls:
        data = proxy_fetch_json(api_url, retry=1)
        if data and (data.get("optionCombinations") is not None or data.get("stockQuantity") is not None):
            return {
                "options": data.get("optionCombinations", []),
                "supplements": data.get("supplementProducts", []),
            }
    return None

def upsert_options(product_id, options):
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

    # ── STEP 1: HTML → 총재고 (web_unlocker) ──
    html = bright_fetch_html(url)
    if not html:
        print(f"  ❌ {name[:20]}: HTML 수집 실패")
        return None

    stock, info = parse_stock_and_info(html)
    if stock is None:
        print(f"  ❌ {name[:20]}: 재고 파싱 실패")
        return None

    now = datetime.now(timezone.utc).isoformat()
    supabase.table("stock_logs").insert({
        "product_id": pid, "stock": stock, "collected_at": now
    }).execute()
    print(f"  ✅ {name[:20]}: 총재고 {stock:,}")

    # ── STEP 2: 상품정보 저장 (최초 1회) ──
    channel_uid = product.get("channel_uid")
    is_brand = product.get("is_brand", False)
    product_no = product.get("product_no")

    if not channel_uid or not product_no:
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
            except:
                pass

    return {
        "pid": pid, "name": name,
        "channel_uid": channel_uid, "product_no": product_no, "is_brand": is_brand
    }

def process_v2(p):
    """v2 API로 옵션/추가상품 수집 (단건)"""
    pid = p["pid"]
    name = p["name"]
    channel_uid = p.get("channel_uid")
    product_no = p.get("product_no")
    is_brand = p.get("is_brand", False)

    if not channel_uid or not product_no:
        return

    v2 = fetch_v2_api(channel_uid, product_no, is_brand)
    if not v2:
        print(f"  ⚠️ {name[:20]}: v2 실패")
        return

    options = v2["options"]
    supplements = v2["supplements"]

    if options:
        upsert_options(pid, options)
        save_option_logs(pid, options)
        opt_summary = ", ".join([f'{o.get("optionName1","?")}:{o.get("stockQuantity",0)}' for o in options[:3]])
        if len(options) > 3:
            opt_summary += f" ...외 {len(options)-3}개"
        print(f"  📊 {name[:15]} 옵션 {len(options)}개 [{opt_summary}]")

    if supplements:
        upsert_supplements(pid, supplements)
        save_supplement_logs(pid, supplements)
        sup_summary = ", ".join([f'{s.get("name","?")[:10]}:{s.get("stockQuantity",0)}' for s in supplements[:3]])
        if len(supplements) > 3:
            sup_summary += f" ...외 {len(supplements)-3}개"
        print(f"  🛒 {name[:15]} 추가상품 {len(supplements)}개 [{sup_summary}]")

    if not options and not supplements:
        print(f"  ℹ️ {name[:15]}: 단일상품")

def main():
    product_id = os.environ.get("PRODUCT_ID", "").strip() or None
    print(f"═══ Sales Tower 크롤러 v2.3 ═══")
    print(f"시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if product_id:
        print(f"단일 상품 수집 (ID: {product_id})")

    products = get_products(product_id)
    print(f"추적 상품 수: {len(products)}개\n")

    # ── Phase 1: 병렬 HTML 수집 (web_unlocker) ──
    print("── Phase 1: 재고 수집 ──")
    phase1_results = []
    fail = 0

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(crawl_product, p): p for p in products}
        for future in as_completed(futures):
            result = future.result()
            if result:
                phase1_results.append(result)
            else:
                fail += 1

    print(f"  Phase 1 완료: {len(phase1_results)}개 성공 / {fail}개 실패\n")

    # ── Phase 2: v2 API (Residential Proxy, 병렬 3개) ──
    targets = [p for p in phase1_results if p.get("channel_uid") and p.get("product_no")]
    print(f"── Phase 2: 옵션/추가상품 수집 ({len(targets)}개) ──")

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(process_v2, p) for p in targets]
        for f in as_completed(futures):
            pass

    print(f"\n═══ 완료 ═══")
    print(f"  재고: {len(phase1_results)}개 성공 / {fail}개 실패")
    print(f"  옵션: {len(targets)}개 시도")

if __name__ == "__main__":
    main()
