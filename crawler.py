import os
import re
import json
import time
from datetime import datetime, timezone
from supabase import create_client
from playwright.sync_api import sync_playwright

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
BROWSER_WS   = os.environ["SCRAPING_BROWSER_WS"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_products(product_id=None):
    query = supabase.table("products").select("id,name,url").eq("active", True)
    if product_id:
        query = query.eq("id", int(product_id))
    return query.execute().data

def get_stock(page, url):
    try:
        # 네트워크 응답 캡처
        option_data = {}

        def handle_response(response):
            try:
                if 'optionCombination' in response.url or 'option' in response.url.lower():
                    print(f"  🔍 옵션 API 감지: {response.url[:80]}")
                if response.status == 200 and 'json' in response.headers.get('content-type', ''):
                    body = response.json()
                    if isinstance(body, dict) and 'optionCombinations' in str(body):
                        option_data['body'] = body
                        print(f"  🔍 JSON 응답에 옵션 데이터 발견!")
            except:
                pass

        page.on("response", handle_response)

        # 페이지 로드 - networkidle로 JS 완전 실행까지 대기
        page.goto(url, wait_until="networkidle", timeout=90000)
        page.wait_for_timeout(5000)

        # 방법1: 캡처된 API 응답에서 옵션 추출
        if option_data.get('body'):
            data = option_data['body']
            options = data.get('optionCombinations', [])
            if options:
                base = [o for o in options if o.get('price', -1) == 0]
                if base:
                    total = sum(o.get('stockQuantity', 0) for o in base)
                    print(f"  📦 API캡처 기본옵션 {len(base)}개: {total}")
                    return total

        # 방법2: JS 실행으로 상태 추출
        data = page.evaluate("""() => {
            try {
                const state = window.__PRELOADED_STATE__;
                if (!state) return {error: 'no state'};
                const keys = Object.keys(state);
                return {keys: keys.slice(0,10), hasSpd: !!state.simpleProductForDetailPage};
            } catch(e) { return {error: String(e)}; }
        }""")
        print(f"  🔍 상태 확인: {data}")

        # 방법3: HTML에서 직접 파싱
        html = page.content()
        print(f"  🔍 HTML 길이: {len(html)}")

        if 'optionCombinations' in html:
            print(f"  🔍 HTML에 optionCombinations 있음!")
            match = re.search(r'"optionCombinations"\s*:\s*(\[[\s\S]+?\])\s*,\s*"(?:useStock|stock|id)', html)
            if match:
                try:
                    options = json.loads(match.group(1))
                    base = [o for o in options if o.get('price', -1) == 0]
                    if base:
                        total = sum(o.get('stockQuantity', 0) for o in base)
                        print(f"  📦 HTML파싱 기본옵션 {len(base)}개: {total}")
                        return total
                except Exception as e:
                    print(f"  ⚠️ HTML 파싱 실패: {e}")

        # 방법4: stockQuantity 폴백
        match = re.search(r'"stockQuantity"\s*:\s*(\d+)', html)
        if match:
            val = int(match.group(1))
            print(f"  📦 폴백: {val}")
            return val

        return None

    except Exception as e:
        print(f"  ⚠️ 오류: {e}")
        return None

def main():
    product_id = os.environ.get("PRODUCT_ID", "").strip() or None
    print(f"크롤링 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if product_id:
        print(f"단일 상품 수집 (ID: {product_id})")

    products = get_products(product_id)
    print(f"추적 상품 수: {len(products)}개")

    success = 0
    fail = 0

    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(BROWSER_WS)
        context = browser.new_context()

        for product in products:
            pid  = product["id"]
            name = product["name"]
            url  = product["url"]

            print(f"  수집 중: {name[:30]}...")
            page = context.new_page()
            try:
                stock = get_stock(page, url)
                if stock is not None:
                    supabase.table("stock_logs").insert({
                        "product_id": pid,
                        "stock": stock,
                        "collected_at": datetime.now(timezone.utc).isoformat()
                    }).execute()
                    print(f"  ✅ {name[:20]}: {stock:,}")
                    success += 1
                else:
                    print(f"  ❌ {name[:20]}: 수집 실패")
                    fail += 1
            finally:
                page.close()

        context.close()
        browser.close()

    print(f"\n완료 - 성공: {success}개 / 실패: {fail}개")

if __name__ == "__main__":
    main()
