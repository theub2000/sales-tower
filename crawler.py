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

def bright_fetch(url, retry=2):
    """Bright Data 프록시 경유 요청"""
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
            print(f"  ⚠️ 요청 오류 (시도 {attempt+1}/{retry}): {e}")
            if attempt < retry - 1:
                time.sleep(3)
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

def fetch_v2_via_bright(channel_uid, product_no, is_brand):
    """v2 API를 Bright Data 경유로 호출 (단건, 순차)"""
    # 브랜드 → /n/ 먼저, 일반 → /i/ 먼저
    urls = [
        f"https://brand.naver.com/n/v2/channels/{channel_uid}/products/{product_no}?withWindow=false",
        f"https://smartstore.naver.com/i/v2/channels/{channel_uid}/products/{product_no}?withWindow=false",
    ] if is_brand else [
        f"https://smartstore.naver.com/i/v2/channels/{channel_uid}/products/{product_no}?withWindow=false",
        f"https://brand.naver.com/n/v2/channels/{channel_uid}/products/{product_no}?withWindow=false",
    ]

    for api_url in urls:
        try:
            raw = bright_fetch(api_url, retry=1)
            if not raw or len(raw) < 10:
                continue
            if raw.strip().startswith("{"):
                data = json.loads(raw)
                options = data.get("optionCombinations", [])
                supplements = data.get("supplementProducts", [])
                if options or supplements or data.get("stockQuantity") is not None:
                    return {"options": options, "supplements": supplements}
            else:
                # rate limit이나 HTML 응답이면 스킵
                if "rate" in raw.lower() or raw.strip().startswith("<"):
                    continue
        except Exception as e:
            print(f"  ⚠️ v2 파싱 실패: {e}")
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

# ═══════════════════════════════════════════
# PHASE 1: HTML 수집 (병렬) — 기존과 동일
# ═══════════════════════════════════════════
def phase1_crawl(product):
    """HTML → 총재고 + 상품정보 추출"""
    pid  = product["id"]
    name = product["name"]
    url  = product["url"]

    print(f"  수집 중: {name[:30]}...")

    html = bright_fetch(url)
    if not html:
        print(f"  ❌ {name[:20]}: HTML 수집 실패")
        return None

    stock, info = parse_stock_and_info(html)
    if stock is None:
        print(f"  ❌ {name[:20]}: 재고 파싱 실패")
        return None

    # stock_logs 저장
    now = datetime.now(timezone.utc).isoformat()
    supabase.table("stock_logs").insert({
        "product_id": pid, "stock": stock, "collected_at": now
    }).execute()
    print(f"  ✅ {name[:20]}: 총재고 {stock:,}")

    # 상품정보 저장 (최초 1회)
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
                print(f"  📝 상품 정보 저장됨")
            except:
                pass

    return {
        "pid": pid,
        "name": name,
        "channel_uid": channel_uid,
        "product_no": product_no,
        "is_brand": is_brand
    }

# ═══════════════════════════════════════════
# PHASE 2: v2 API 순차 호출 (1.5초 간격)
# ═══════════════════════════════════════════
def phase2_options(products_info):
    """v2 API → 옵션 + 추가상품 (순차, rate limit 방지)"""
    print(f"\n── Phase 2: 옵션/추가상품 수집 ({len(products_info)}개) ──")

    success = 0
    for i, p in enumerate(products_info):
        pid = p["pid"]
        name = p["name"]
        channel_uid = p.get("channel_uid")
        product_no = p.get("product_no")
        is_brand = p.get("is_brand", False)

        if not channel_uid or not product_no:
            continue

        # rate limit 방지: 1.5초 대기
        if i > 0:
            time.sleep(1.5)

        v2 = fetch_v2_via_bright(channel_uid, product_no, is_brand)
        if not v2:
            print(f"  ⚠️ {name[:20]}: v2 실패")
            continue

        options = v2["options"]
        supplements = v2["supplements"]

        if options:
            upsert_options(pid, options)
            save_option_logs(pid, options)
            opt_summary = ", ".join([f'{o.get("optionName1","?")}:{o.get("stockQuantity",0)}' for o in options[:3]])
            if len(options) > 3:
                opt_summary += f" ...외 {len(options)-3}개"
            print(f"  📊 {name[:15]} 옵션 {len(options)}개 [{opt_summary}]")
            success += 1

        if supplements:
            upsert_supplements(pid, supplements)
            save_supplement_logs(pid, supplements)
            sup_summary = ", ".join([f'{s.get("name","?")[:10]}:{s.get("stockQuantity",0)}' for s in supplements[:3]])
            if len(supplements) > 3:
                sup_summary += f" ...외 {len(supplements)-3}개"
            print(f"  🛒 {name[:15]} 추가상품 {len(supplements)}개 [{sup_summary}]")
            if not options:
                success += 1

        if not options and not supplements:
            print(f"  ℹ️ {name[:15]}: 단일상품")
            success += 1

    return success

def main():
    product_id = os.environ.get("PRODUCT_ID", "").strip() or None
    print(f"═══ Sales Tower 크롤러 v2.3 ═══")
    print(f"시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if product_id:
        print(f"단일 상품 수집 (ID: {product_id})")

    products = get_products(product_id)
    print(f"추적 상품 수: {len(products)}개\n")

    # ── Phase 1: 병렬 HTML 수집 ──
    print("── Phase 1: 재고 수집 (병렬) ──")
    phase1_results = []
    fail = 0

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(phase1_crawl, p): p for p in products}
        for future in as_completed(futures):
            result = future.result()
            if result:
                phase1_results.append(result)
            else:
                fail += 1

    print(f"\n  Phase 1 완료: 성공 {len(phase1_results)}개 / 실패 {fail}개")

    # ── Phase 2: 순차 v2 API ──
    v2_success = phase2_options(phase1_results)

    print(f"\n═══ 완료 ═══")
    print(f"  재고 수집: {len(phase1_results)}개 성공 / {fail}개 실패")
    print(f"  옵션 수집: {v2_success}개 성공")

if __name__ == "__main__":
    main()
