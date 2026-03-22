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
        # 1. 불필요한 리소스 전면 차단 (이미지/폰트/CSS/미디어)
        def block_media(route):
            if route.request.resource_type in ["image", "media", "font", "stylesheet"]:
                route.abort()
            else:
                route.continue_()
        page.route("**/*", block_media)

        # 2. 옵션 API 응답 캡처용 저장소
        captured_options = []

        def handle_response(response):
            try:
                url_lower = response.url.lower()
                # 옵션 관련 API 패턴 감지
                if any(k in url_lower for k in ['optioncombination', 'option', 'stock', 'product']):
                    if response.status == 200:
                        ct = response.headers.get('content-type', '')
                        if 'json' in ct:
                            body = response.json()
                            body_str = str(body)
                            if 'optionCombination' in body_str or 'stockQuantity' in body_str:
                                print(f"  🎯 옵션 API 발견: {response.url[:80]}")
                                captured_options.append({'url': response.url, 'data': body})
            except:
                pass

        page.on("response", handle_response)

        # 3. 페이지 로드
        page.goto(url, wait_until="domcontentloaded", timeout=30000)

        # 4. 옵션 선택 영역 클릭 시도
        try:
            # 옵션 드롭다운 선택자들 시도
            selectors = [
                "select[class*='option']",
                "[class*='option'] select",
                "select[title*='선택']",
                "[class*='SelectBox']",
                "[class*='select_box']",
            ]
            clicked = False
            for sel in selectors:
                try:
                    el = page.wait_for_selector(sel, timeout=2000)
                    if el:
                        el.click()
                        page.wait_for_timeout(1500)
                        clicked = True
                        print(f"  🖱️ 옵션 클릭 성공: {sel}")
                        break
                except:
                    continue

            if not clicked:
                print(f"  ⚠️ 옵션 선택자 못 찾음, 페이지 대기")
                page.wait_for_timeout(2000)

        except Exception as e:
            print(f"  ⚠️ 클릭 실패: {e}")

        # 5. 캡처된 옵션 API 데이터 처리
        if captured_options:
            print(f"  🎯 캡처된 API: {len(captured_options)}개")
            for item in captured_options:
                data = item['data']
                print(f"  🔍 API URL: {item['url'][:80]}")
                # optionCombinations 재귀 탐색
                def find_options(obj):
                    if not isinstance(obj, (dict, list)):
                        return None
                    if isinstance(obj, dict):
                        if 'optionCombinations' in obj and isinstance(obj['optionCombinations'], list):
                            return obj['optionCombinations']
                        for v in obj.values():
                            result = find_options(v)
                            if result:
                                return result
                    if isinstance(obj, list):
                        for item in obj:
                            result = find_options(item)
                            if result:
                                return result
                    return None

                options = find_options(data)
                if options:
                    min_price = min(o.get('price', 0) for o in options)
                    base = [o for o in options if (o.get('price') or 0) == min_price]
                    total = sum(o.get('stockQuantity', 0) for o in base)
                    print(f"  📦 API캡처 최저가({min_price}원) {len(base)}개 합산: {total}")
                    return total

        # 6. 폴백: JS 상태에서 직접 추출
        data = page.evaluate("""() => {
            try {
                if (window.__PRELOADED_STATE__) {
                    const spd = window.__PRELOADED_STATE__.simpleProductForDetailPage;
                    if (spd) {
                        for (const key of Object.keys(spd)) {
                            const val = spd[key];
                            if (!val) continue;
                            if (val.optionCombinations && val.optionCombinations.length > 0) {
                                const opts = val.optionCombinations;
                                const minPrice = Math.min(...opts.map(o => o.price || 0));
                                const base = opts.filter(o => (o.price || 0) === minPrice);
                                return {type: 'preloaded_options',
                                        total: base.reduce((s,o) => s+(o.stockQuantity||0), 0),
                                        count: base.length, minPrice};
                            }
                            if (val.stockQuantity !== undefined) {
                                return {type: 'preloaded_single', total: val.stockQuantity};
                            }
                        }
                    }
                }
                const nextEl = document.getElementById('__NEXT_DATA__');
                if (nextEl) {
                    const json = JSON.parse(nextEl.textContent);
                    let found = null;
                    function findOpts(obj) {
                        if (!obj || typeof obj !== 'object' || found) return;
                        if (obj.optionCombinations && Array.isArray(obj.optionCombinations) && obj.optionCombinations.length > 0) {
                            found = obj.optionCombinations; return;
                        }
                        Object.values(obj).forEach(findOpts);
                    }
                    findOpts(json);
                    if (found) {
                        const minPrice = Math.min(...found.map(o => o.price || 0));
                        const base = found.filter(o => (o.price || 0) === minPrice);
                        return {type: 'next_options',
                                total: base.reduce((s,o) => s+(o.stockQuantity||0), 0),
                                count: base.length, minPrice};
                    }
                }
                return {type: 'no_options'};
            } catch(e) { return {type: 'error', error: e.toString()}; }
        }""")

        print(f"  🔍 JS결과: {data}")
        if data:
            total = data.get('total')
            t = data.get('type', '')
            if total is not None and 'options' in t or (total is not None and 'single' in t):
                return total

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
