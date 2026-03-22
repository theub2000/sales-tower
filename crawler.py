import os
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
        # 1. 불필요한 리소스 차단
        def block_media(route):
            # CSS는 허용 (옵션 버튼 렌더링에 필요)
            if route.request.resource_type in ["image", "media", "font"]:
                route.abort()
            else:
                route.continue_()
        page.route("**/*", block_media)

        # 2. 옵션 API 캡처
        captured_options = []

        def handle_response(response):
            try:
                if response.status == 200:
                    ct = response.headers.get('content-type', '')
                    if 'json' in ct:
                        body = response.json()
                        body_str = str(body)[:1500]
                        if 'optionCombination' in body_str or 'stockQuantity' in body_str:
                            print(f"  🎯 핵심 API 캡처: {response.url[:80]}")
                            captured_options.append({'url': response.url, 'data': body})
            except:
                pass

        page.on("response", handle_response)

        # 3. 페이지 로드
        page.goto(url, wait_until="load", timeout=30000)
        page.wait_for_timeout(2000)

        # 4. React 기반 옵션 버튼 강제 클릭
        selectors = [
            "a[role='button']:has-text('선택')",
            "button:has-text('옵션')",
            "a:has-text('옵션을 선택하세요')",
            "[class*='Option_button']",
            "[class*='option_button']",
            "a[class*='bd_']",
        ]
        for sel in selectors:
            try:
                if page.is_closed():
                    break
                el = page.wait_for_selector(sel, state="attached", timeout=2000)
                if el:
                    el.click(force=True)
                    page.wait_for_timeout(1500)
                    print(f"  🖱️ 옵션 클릭 성공: {sel}")
                    break
            except:
                continue
        else:
            if not page.is_closed():
                page.wait_for_timeout(2000)

        # 5. 캡처된 API에서 price==0 본품만 합산
        if captured_options:
            for item in captured_options:
                def find_options(obj):
                    if not isinstance(obj, (dict, list)):
                        return None
                    if isinstance(obj, dict):
                        if 'optionCombinations' in obj and isinstance(obj['optionCombinations'], list) and len(obj['optionCombinations']) > 0:
                            return obj['optionCombinations']
                        for v in obj.values():
                            r = find_options(v)
                            if r: return r
                    if isinstance(obj, list):
                        for i in obj:
                            r = find_options(i)
                            if r: return r
                    return None

                options = find_options(item['data'])
                if options:
                    base = [o for o in options if o.get('price', 0) == 0]
                    if base:
                        total = sum(o.get('stockQuantity', 0) for o in base)
                        print(f"  📦 API캡처 본품 {len(base)}개 합산: {total}")
                        return total

        # 6. JS 상태 폴백
        if page.is_closed():
            return None
        data = page.evaluate("""() => {
            try {
                let foundOptions = null;
                let singleStock = null;

                function findOpts(obj) {
                    if (!obj || typeof obj !== 'object' || foundOptions) return;
                    if (obj.optionCombinations && Array.isArray(obj.optionCombinations) && obj.optionCombinations.length > 0) {
                        foundOptions = obj.optionCombinations;
                        return;
                    }
                    if (obj.stockQuantity !== undefined && obj.stockQuantity !== null) {
                        singleStock = obj.stockQuantity;
                    }
                    Object.values(obj).forEach(findOpts);
                }

                // 네이버 3대 금고 순서대로 탐색
                if (window.__PRELOADED_STATE__) findOpts(window.__PRELOADED_STATE__);
                if (!foundOptions && window.__INITIAL_STATE__) findOpts(window.__INITIAL_STATE__);
                const nextEl = document.getElementById('__NEXT_DATA__');
                if (!foundOptions && nextEl) findOpts(JSON.parse(nextEl.textContent));

                if (foundOptions) {
                    const base = foundOptions.filter(o => (o.price || 0) === 0);
                    if (base.length > 0)
                        return {type: 'options_base_only', total: base.reduce((s,o) => s+(o.stockQuantity||0), 0)};
                }
                if (singleStock !== null)
                    return {type: 'single_stock', total: singleStock};

                return {type: 'no_options'};
            } catch(e) {
                return {type: 'error', error: e.toString()};
            }
        }""")

        print(f"  🔍 JS결과: {data}")
        if data and data.get('total') is not None and data.get('type') != 'no_options':
            return data['total']

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
        for product in products:
            pid  = product["id"]
            name = product["name"]
            url  = product["url"]

            print(f"\n  수집 중: {name[:30]}...")

            try:
                # 매 상품마다 웹소켓 새로 연결 = 새 IP 발급
                browser = p.chromium.connect_over_cdp(BROWSER_WS)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
                    viewport={'width': 390, 'height': 844}
                )
                page = context.new_page()

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
            except Exception as e:
                print(f"  ⚠️ 브라우저 연결 실패 (건너뜀): {e}")
                fail += 1
            finally:
                if 'page' in locals() and not page.is_closed():
                    page.close()
                if 'context' in locals():
                    context.close()
                if 'browser' in locals():
                    browser.close()

    print(f"\n완료 - 성공: {success}개 / 실패: {fail}개")

if __name__ == "__main__":
    main()
