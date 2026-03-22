import os
import re
import json
import time
from datetime import datetime, timezone
from supabase import create_client
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    """Playwright로 페이지 로드 후 optionCombinations 추출"""
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)  # JS 렌더링 대기

        # PRELOADED_STATE에서 optionCombinations 추출
        data = page.evaluate("""() => {
            try {
                const state = window.__PRELOADED_STATE__;
                if (!state) return null;
                const spd = state.simpleProductForDetailPage;
                if (!spd) return null;
                for (const key of Object.keys(spd)) {
                    const val = spd[key];
                    if (val && val.optionCombinations) {
                        return {
                            options: val.optionCombinations,
                            stockQuantity: val.stockQuantity
                        };
                    }
                }
                return null;
            } catch(e) { return null; }
        }""")

        if data and data.get('options'):
            options = data['options']
            print(f"  🔍 옵션 {len(options)}개 발견")
            # price==0인 기본 옵션만
            base = [o for o in options if o.get('price', -1) == 0 and o.get('stockQuantity') is not None]
            if base:
                total = sum(o['stockQuantity'] for o in base)
                print(f"  📦 기본옵션 {len(base)}개 합산: {total}")
                return total
            # price==0 없으면 최저가
            min_p = min(o.get('price', 0) for o in options)
            base = [o for o in options if o.get('price') == min_p]
            total = sum(o.get('stockQuantity', 0) for o in base)
            print(f"  📦 최저가({min_p}원) {len(base)}개 합산: {total}")
            return total

        # 옵션 없는 단일 상품
        if data and data.get('stockQuantity') is not None:
            val = data['stockQuantity']
            print(f"  📦 단일 재고: {val}")
            return val

        print(f"  ⚠️ 데이터 없음")
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
