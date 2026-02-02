import asyncio
import json
import logging
import sys
import re
import os
from datetime import datetime
from dateutil import parser
from playwright.async_api import async_playwright

# --- Site metadata ---
site = "KKR"
section = "Insights"
company_site_id = "am-213"
country = "Global"
role = "Corporate"
BASE_URL = "https://www.kkr.com"

# --- Logging setup ---
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)

# --- Scraper class ---
class KKR_Global_co:
    def __init__(self, target_date, sleep_time=3):
        self.target_date = parser.parse(target_date, fuzzy=True).date()
        self.sleep_time = sleep_time
        self.items = []
        self.seen_urls = set()

    async def scrape(self, url):
        logger.info(f"Starting scraper for URL: {url}")
    async def scrape(self, url):
        logger.debug(f"DEBUG: Starting Playwright scraper for {url}")

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        "--disable-gpu",
                        "--no-sandbox",
                        "--single-process",
                        "--disable-dev-shm-usage",
                        "--no-zygote",
                        "--disable-setuid-sandbox",
                        "--disable-accelerated-2d-canvas",
                        "--no-first-run",
                        "--no-default-browser-check",
                        "--disable-background-networking",
                        "--disable-background-timer-throttling",
                        "--disable-client-side-phishing-detection",
                        "--disable-component-update",
                        "--disable-default-apps",
                        "--disable-domain-reliability",
                        "--disable-features=AudioServiceOutOfProcess",
                        "--disable-hang-monitor",
                        "--disable-ipc-flooding-protection",
                        "--disable-popup-blocking",
                        "--disable-prompt-on-repost",
                        "--disable-renderer-backgrounding",
                        "--disable-sync",
                        "--force-color-profile=srgb",
                        "--metrics-recording-only",
                        "--mute-audio",
                        "--no-pings",
                        "--use-gl=swiftshader",
                        "--window-size=1280,1696"
                        ])
            context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                    )
            page = await context.new_page()

            # Set headers
            await page.set_extra_http_headers({
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Cache-Control": "max-age=0",
                })

            await page.set_content("<meta http-equiv='X-Content-Type-Options' content='nosniff'>")

            # Step 1: Load page
            await page.goto(url, timeout=60000)
            await asyncio.sleep(self.sleep_time)

            # Accept cookies if present
            try:
                await page.locator("#onetrust-accept-btn-handler").click(timeout=5000)
                logger.debug("Cookie accept clicked")
                await asyncio.sleep(1)
            except Exception as e:
                logger.warning(f"Cookie accept not present or failed: {e}")

            # Determine total pages
            total_pages = 1
            try:
                await page.wait_for_selector(".cmp-insights-filter__page", timeout=5000)
                page_buttons = await page.locator(".cmp-insights-filter__page").all()
                pages_nums = []
                for btn in page_buttons:
                    dp = await btn.get_attribute("data-page")
                    if dp and dp.isdigit():
                        pages_nums.append(int(dp))
                if pages_nums:
                    total_pages = max(pages_nums)
                logger.info(f"Detected {total_pages} pagination pages")
            except Exception as e:
                logger.warning(f"Pagination not detected, defaulting to 1 page: {e}")

            # --- Loop through pages ---
            for p_no in range(1, total_pages + 1):
                logger.info(f"Scraping page {p_no}...")

                # Click page if not first
                if p_no > 1:
                    try:
                        await page.locator(f'span.cmp-insights-filter__page[data-page="{p_no}"]').click(timeout=10000)
                        await asyncio.sleep(self.sleep_time)
                        await page.wait_for_selector(".teaser", timeout=15000)
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await asyncio.sleep(2)
                    except Exception as e:
                        logger.warning(f"Failed to click or load page {p_no}: {e}")
                        continue

                # Collect article cards
                try:
                    cards = await page.locator("div.cmp-insights-filter__insights-container").locator("div.teaser").all()
                    logger.info(f"Page {p_no}: Found {len(cards)} article cards")
                except Exception as e:
                    logger.warning(f"No teaser cards found on page {p_no}: {e}")
                    continue

                # Extract each article info
                stop_scraping = False
                for idx, card in enumerate(cards, start=1):
                    try:
                        title = await card.locator("p.article-teaser__title span").text_content() or None
                        href = await card.locator("a.article-teaser__link").get_attribute("href")
                        if not href:
                            continue
                        url_full = href if href.startswith("http") else BASE_URL + href
                        if url_full in self.seen_urls:
                            continue
                        self.seen_urls.add(url_full)
                        slug = url_full.rstrip("/").split("/")[-1]

                        # parse teaser date (may be month-only)
                        date_attr = await card.locator("span.article-teaser__date time").get_attribute("datetime")
                        parsed_date = parser.parse(date_attr, fuzzy=True).date() if date_attr else None
                        logger.info(f"Date: {parsed_date}")

                        if parsed_date:
                            parsed_date = parser.parse(("-").join(str(parsed_date).split("-")[:2])+"-01").date()
                        logger.info(f"Date: {parsed_date}")

                        if parsed_date and parsed_date < self.target_date:
                            stop_scraping = True
                            logger.info(f"STOP: Found older article {parsed_date} < {self.target_date}")
                            break

                        self.items.append({
                            "company_site_id": company_site_id,
                            "company_site_country": country,
                            "company_site_role": role,
                            "article_source": site,
                            "article_section": section,
                            "article_date": str(parsed_date) if parsed_date else None,
                            "article_title": title,
                            "article_description": None,
                            "article_content": None,
                            "article_tags": [],
                            "article_slug": slug,
                            "article_url": url_full
                        })
                    except Exception as e:
                        logger.error(f"Error parsing card on page {p_no} #{idx}: {e}")
                        continue

                if stop_scraping:
                    break

            # --- Scrape article pages for full content ---
            await self.scrape_article_pages(context)

            await browser.close()
            logger.info(f"Finished scraping {len(self.items)} total articles")
            return self.items

    async def scrape_article_pages(self, context):
        logger.debug("Scraping individual article pages...")
        page = await context.new_page()
        for idx, item in enumerate(self.items, start=1):
            url = item.get("article_url")
            if not url:
                continue
            if url.lower().endswith(".pdf"):
                item["article_content"] = url
                item["article_date"] = item.get("article_date") or str(self.target_date)
                continue
            try:
                await page.goto(url, timeout=60000)
                await asyncio.sleep(self.sleep_time)

                # Extract description
                try:
                    desc = await page.locator('meta[name="description"]').get_attribute("content")
                    item["article_description"] = desc
                except:
                    item["article_description"] = None

                # Extract tags
                try:
                    tags = await page.locator(".cmp-category-navigation__list-item").all_text_contents()
                    item["article_tags"] = [t.strip() for t in tags if t.strip()]
                except:
                    item["article_tags"] = []

                # Extract main content
                content_text = None
                try:
                    sel_candidates = [
                        "div.aem-Grid.aem-Grid--12.aem-Grid--default--12",
                        "div.article-content",
                        "div.cmp-text.wysiwyg",
                        "div.cmp-content",
                        "article",
                        "main"
                    ]
                    texts = []
                    for sel in sel_candidates:
                        parts = await page.locator(sel).all_text_contents()
                        texts.extend([p.strip() for p in parts if p.strip()])
                    content_text = " ".join(texts).strip()
                except:
                    content_text = None

                # Attempt to extract date from article
                parsed_date = None
                try:
                    time_el = page.locator("time").first
                    time_attr = await time_el.get_attribute("datetime")
                    if time_attr:
                        parsed_date = parser.parse(time_attr, fuzzy=True).date()
                except:
                    parsed_date = None
                item["article_date"] = item.get("article_date") or str(parsed_date)
                item["article_content"] = content_text

            except Exception as e:
                logger.error(f"Failed to scrape article #{idx} ({url}): {e}")

        await page.close()


async def KKRGLOBALCO(target_date):
    results = []
    try:
        url = "https://www.kkr.com/insights"
        target_date=("-").join(target_date.split("-")[:2])+"-01"
        scraper = KKR_Global_co(target_date)
        results = await scraper.scrape(url)

  
        output_path=f"/tmp/{company_site_id}.json"

        with open(output_path, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=4, ensure_ascii=False)

        logger.info(f"Scraped {len(results)} articles after {target_date}")
        return 200
    except Exception as error:
        logger.error(f"Error: {error}")
        return 500


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-12-01"
    logger.info(f"Scraping KKR insights using target date: {target_date}")
    status = asyncio.run(KKRGLOBALCO(target_date=target_date))
    logger.info(f"Exit status: {status}")
