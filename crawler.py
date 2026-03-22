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
        # domcontentloaded로 변경 (networkidle은 쇼핑몰에서 타임아웃 남)
        page.goto(url, wait_until="domcontentloaded", timeout=30000)

        # 옵션 영역 또는 스크립트 로딩 대기
        try:
            page.wait_for_selector("script[id='__NEXT_DATA__']", timeout=5000)
        except:
            pass
        page.wait_for_timeout(2000)

        data = page.evaluate("""() => {
            try {
                // 방법1: __PRELOADED_STATE__ (구형 네이버)
                if (window.__PRELOADED_STATE__) {
                    const state = window.__PRELOADED_STATE__;
                    const spd = state.simpleProductForDetailPage;
                    if (spd) {
                        for (const key of Object.keys(spd)) {
                            const val = spd[key];
                            if (val && val.optionCombinations && val.optionCombinations.length > 0) {
                                const options = val.optionCombinations;
                                const base = options.filter(o => o.price === 0);
                                if (base.length > 0) {
                                    return {type: 'preloaded_options', 
                                            total: base.reduce((s,o) => s + (o.stockQuantity||0), 0),
                                            count: base.length};
                                }
                                const minPrice = Math.min(...options.map(o => o.price || 0));
                                const minOpts = options.filter(o => o.price === minPrice);
                                return {type: 'preloaded_min', 
                                        total: minOpts.reduce((s,o) => s + (o.stockQuantity||0), 0),
                                        count: minOpts.length, minPrice};
                            }
                            if (val && val.stockQuantity !== undefined) {
                                return {type: 'preloaded_stock', total: val.stockQuantity};
                            }
                        }
                    }
                    return {type: 'preloaded_no_data', keys: Object.keys(state).slice(0,5)};
                }

                // 방법2: __NEXT_DATA__ (신형 Next.js 네이버)
                const nextEl = document.getElementById('__NEXT_DATA__');
                if (nextEl) {
                    const nextData = JSON.parse(nextEl.textContent);
                    const raw = JSON.stringify(nextData).substring(0, 500);
                    // optionCombinations 탐색
                    const str = JSON.stringify(nextData);
                    if (str.includes('optionCombinations')) {
                        return {type: 'next_data_has_options', raw: raw};
                    }
                    return {type: 'next_data_no_options', raw: raw};
                }

                return {type: 'no_state', 
                        hasPreloaded: !!window.__PRELOADED_STATE__,
                        hasNextData: !!document.getElementById('__NEXT_DATA__')};
            } catch(e) {
                return {type: 'error', error: e.toString()};
            }
        }""")

        print(f"  🔍 결과: {data}")

        if data:
            t = data.get('type', '')
            if 'options' in t or 'min' in t or 'stock' in t:
                val = data.get('total', 0)
                print(f"  📦 재고: {val} (방식: {t}, 옵션수: {data.get('count','?')})")
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
