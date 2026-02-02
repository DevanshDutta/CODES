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
site = "State Street Global Advisors"
section = "Insights"
company_site_id = "am-264"
country = "United States"
role = "Financial Professional"
BASE_URL = "https://www.ssga.com"

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
class SSGAScraperUS:
    def __init__(self, target_date, sleep_time=5):
        self.target_date = parser.parse(target_date, fuzzy=True).date()
        self.sleep_time = sleep_time
        self.items = []
        self.stop_pagination = False   # <-- ADDED

    def normalize_date(self, raw):
        try:
            if re.match(r"^[A-Za-z]+\s+\d{4}$", raw.strip()):
                dt = parser.parse(raw, fuzzy=True)
                return datetime(dt.year, dt.month, 1).date()
            return parser.parse(raw, fuzzy=True).date()
        except:
            return self.target_date

    async def scrape(self, url):
        logger.info(f"Starting scraper for URL: {url}")

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
                "--disable-dev-shm-usage",
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
            ]
        )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
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

            await page.goto(url, timeout=60000)
            await asyncio.sleep(self.sleep_time)

            try:
                await page.wait_for_selector(
                    "#js-ssmp-clrButtonLabel",
                    timeout=5000
                )
                logger.info("Cookie / role popup detected — accepting")
                await page.locator("#js-ssmp-clrButtonLabel").click()
                await asyncio.sleep(2)
            except Exception as e:
                logger.debug(f"Cookie popup not shown or already accepted: {e}")

            try:
                await page.locator("#onetrust-close-btn-container").click(timeout=5000)
                await asyncio.sleep(1)
            except:
                pass

            while True:
                if self.stop_pagination:
                    break
                try:
                    load_more = page.locator("button.ssmp-load-more")
                    if await load_more.is_visible():
                        await load_more.click()
                        await asyncio.sleep(3)

                        cards_check = page.locator("li.result-item")
                        count = await cards_check.count()

                        for i in range(count):
                            try:
                                raw_date = await cards_check.nth(i).locator("p.date-author span").first.text_content()
                                parsed_date = self.normalize_date(raw_date)
                                if parsed_date < self.target_date:
                                    logger.info("Stopping pagination due to date cutoff.")
                                    self.stop_pagination = True   # <-- ADDED
                                    break
                            except:
                                continue
                    else:
                        break
                except:
                    break

            try:
                await page.wait_for_selector("li.result-item",
                                             state="attached", timeout=15000)
                cards = await page.locator("li.result-item").all()
                logger.info(f"Found {len(cards)} article cards on the page")
            except Exception as e:
                logger.error(f"Could not find article cards: {e}")
                await browser.close()
                return []

            for idx, card in enumerate(cards, start=1):

                try:
                    tag = await card.locator("p.eyebrow a").text_content()
                except:
                    tag = None

                try:
                    title = await card.locator("p.title a").text_content()
                except:
                    title = None

                try:
                    href = await card.locator("p.title a").get_attribute("href")
                    slug = href.rstrip("/").split("/")[-1] if href else None
                except:
                    href = slug = None

                try:
                    desc = await card.locator("p.desc").text_content()
                except:
                    desc = None

                try:
                    d_raw = await card.locator("p.date-author span").first.text_content()
                    parsed_date = self.normalize_date(d_raw)
                except:
                    parsed_date = self.target_date

                if parsed_date < self.target_date:
                    continue

                if not href:
                    continue

                url_full = href if href.startswith("http") else BASE_URL + href

                self.items.append({
                    "company_site_id": company_site_id,
                    "company_site_country": country,
                    "company_site_role": role,
                    "article_source": site,
                    "article_section": section,
                    "article_date": str(parsed_date),
                    "article_title": title,
                    "article_description": desc,
                    "article_content": None,
                    "article_tags": [tag] if tag else [],
                    "article_slug": slug,
                    "article_url": url_full
                })

            await self.scrape_article_pages(context)

            await browser.close()
            logger.info(f"Finished scraping {len(self.items)} total articles")
            self.items = [item for item in self.items if item["article_content"]]
            return self.items


    async def scrape_article_pages(self, context):
        logger.debug("Starting to scrape individual article pages...")

        page = await context.new_page()
        for idx, item in enumerate(self.items, start=1):
            url = item["article_url"]
            if not url:
                continue

            if url.endswith(".pdf"):
                item["article_content"] = url
                item["article_date"] = str(self.target_date)
                continue

            try:
                await page.goto(url, timeout=60000)
                await asyncio.sleep(self.sleep_time)

                content_el = await page.locator("div.ssmp-richtext").all_text_contents()
                full_text = " ".join(map(str.strip, content_el)).strip()

                item["article_content"] = full_text

            except Exception as e:
                logger.error(f"Failed to scrape article #{idx}: {e}")

        await page.close()


async def SSGAUSFP(target_date):
    results = []
    try:
        url = "https://www.ssga.com/us/en/intermediary/insights"
        scraper = SSGAScraperUS(target_date)
        results = await scraper.scrape(url)

        if not os.path.exists("/tmp"):
            os.makedirs("/tmp")

        output_path=f"/tmp/{company_site_id}.json"

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)

        logger.info(f"Scraped {len(results)} U.S. articles after {target_date}")
        return 200
    except Exception as error:
        logger.error(f"Error: {error}")
        return 500
 
if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-12-01"
    logger.info(f"Scraping U.S. articles using target date: {target_date}")
    asyncio.run(
        SSGAUSFP(
            target_date=target_date
        )
    )
