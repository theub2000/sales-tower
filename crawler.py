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
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        try:
            page.wait_for_selector("script[id='__NEXT_DATA__']", timeout=3000)
        except:
            pass
        page.wait_for_timeout(2000)

        data = page.evaluate("""() => {
            try {
                // 1. 구형 스마트스토어 (__PRELOADED_STATE__)
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
                                const total = base.reduce((s,o) => s + (o.stockQuantity||0), 0);
                                return {type: 'preloaded_options', total, count: base.length, minPrice};
                            }
                            if (val.stockQuantity !== undefined) {
                                return {type: 'preloaded_single', total: val.stockQuantity};
                            }
                        }
                    }
                }

                // 2. 신형 Next.js 스토어 (__NEXT_DATA__) - 재귀 탐색
                const nextEl = document.getElementById('__NEXT_DATA__');
                if (nextEl) {
                    const json = JSON.parse(nextEl.textContent);
                    let foundOptions = null;

                    function findOptions(obj) {
                        if (!obj || typeof obj !== 'object' || foundOptions) return;
                        if (obj.optionCombinations && Array.isArray(obj.optionCombinations) && obj.optionCombinations.length > 0) {
                            foundOptions = obj.optionCombinations;
                            return;
                        }
                        Object.values(obj).forEach(findOptions);
                    }
                    findOptions(json);

                    if (foundOptions) {
                        const minPrice = Math.min(...foundOptions.map(o => o.price || 0));
                        const base = foundOptions.filter(o => (o.price || 0) === minPrice);
                        const total = base.reduce((s,o) => s + (o.stockQuantity||0), 0);
                        return {type: 'next_options', total, count: base.length, minPrice};
                    }

                    // optionCombinations 없으면 stockQuantity 재귀 탐색
                    let foundStock = null;
                    function findStock(obj, depth) {
                        if (!obj || typeof obj !== 'object' || depth > 10 || foundStock !== null) return;
                        if (typeof obj.stockQuantity === 'number' && obj.stockQuantity > 0) {
                            foundStock = obj.stockQuantity;
                            return;
                        }
                        Object.values(obj).forEach(v => findStock(v, depth+1));
                    }
                    findStock(json, 0);

                    if (foundStock !== null) {
                        return {type: 'next_single', total: foundStock};
                    }

                    return {type: 'next_no_data'};
                }

                return {type: 'no_state'};
            } catch(e) {
                return {type: 'error', error: e.toString()};
            }
        }""")

        print(f"  🔍 결과: {data}")

        if data:
            t = data.get('type', '')
            total = data.get('total')
            if total is not None and t not in ('no_state', 'error', 'next_no_data'):
                if 'options' in t:
                    print(f"  📦 최저가({data.get('minPrice')}원) {data.get('count')}개 합산: {total}")
                else:
                    print(f"  📦 단일재고: {total}")
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
                time.sleep(1)

        context.close()
        browser.close()

    print(f"\n완료 - 성공: {success}개 / 실패: {fail}개")

if __name__ == "__main__":
    main()
