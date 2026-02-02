import asyncio
import json
import logging
import sys
import re
from datetime import datetime
from dateutil import parser
from playwright.async_api import async_playwright

# --- Site metadata ---
site = "Allspring Global Investments"
section = "Insights"
company_site_id = "am-338"
country = "United States"
role = "Financial Advisor"
BASE_URL = "https://www.allspringglobal.com"

# --- Logging setup ---
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)


class AllSpringScraperGlobal:
    def __init__(self, target_date, sleep_time=5):
        self.target_date = parser.parse(target_date, fuzzy=True).date()
        self.sleep_time = sleep_time
        self.items = []

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
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
            page = await context.new_page()

            # Headers
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

            # --- PAGINATION LOOP ---
            page_number = 0
            while True:

                paged_url = f"{url}?page={page_number}" if page_number > 0 else url
                logger.info(f"Scraping listing page: {paged_url}")

                await page.goto(paged_url, timeout=120000)
                await asyncio.sleep(self.sleep_time)

                try:
                    # Open country dropdown
                    await page.locator("#dropdown-location-button").click()
                    await asyncio.sleep(1)

                    # Select United States
                    await page.locator("#dropdown-location-option-us").click()
                    await asyncio.sleep(1)

                    # Select Financial Advisor
                    await page.locator("button.self-id__role:has(div.self-id__role-title:has-text('Financial Advisor'))").click()
                    await asyncio.sleep(1)

                    # Click Accept
                    await page.locator("button.self-id__footer-terms-actions--submit").click()
                    await asyncio.sleep(self.sleep_time)

                except Exception as e:
                    logger.debug(f"Self-ID popup not shown or already accepted: {e}")

                try:
                    await page.locator("#selector-form-confirm").click(timeout=3000)
                    await asyncio.sleep(self.sleep_time)
                except:
                    pass

                try:
                    await page.locator("#onetrust-close-btn-container").click(timeout=3000)
                    await asyncio.sleep(1)
                except:
                    pass

                try:
                    await page.wait_for_selector("a.card.insight-card", state="attached", timeout=15000)
                    cards = await page.locator("a.card.insight-card").all()
                except:
                    logger.info("No more cards found — stopping pagination.")
                    break

                if not cards:
                    break

                stop_pagination = False
                seen_newer = False

                for idx, card in enumerate(cards, start=1):

                    try:
                        title = await card.locator("p.card__heading").text_content()
                    except:
                        title = None

                    try:
                        href = await card.get_attribute("href")
                        url_full = BASE_URL + href if href and not href.startswith("http") else href
                        slug = href.rstrip("/").split("/")[-1] if href else None
                    except:
                        url_full = slug = None

                    # Correct date selector
                    try:
                        date_text = await card.locator("span.article__date").text_content()
                        parsed_listing_date = parser.parse(date_text, fuzzy=True).date()
                        parsed_listing_date_str = str(parsed_listing_date)
                    except:
                        parsed_listing_date = self.target_date
                        parsed_listing_date_str = str(self.target_date)

                    if parsed_listing_date < self.target_date and not seen_newer:
                        logger.info(f"Skipping early old article ({parsed_listing_date})")
                        continue

                    if parsed_listing_date < self.target_date and seen_newer:
                        logger.info(f"Hit old article ({parsed_listing_date}) < target → STOP")
                        stop_pagination = True
                        break

                    if parsed_listing_date >= self.target_date:
                        seen_newer = True

                    try:
                        description = await card.locator("div.card__sub-body p").text_content()
                    except:
                        description = None

                    try:
                        tag = await card.locator("span.article__topic").text_content()
                    except:
                        tag = None

                    self.items.append({
                        "company_site_id": company_site_id,
                        "company_site_country": country,
                        "company_site_role": role,
                        "article_source": site,
                        "article_section": section,
                        "article_date": parsed_listing_date_str,
                        "article_title": title,
                        "article_description": description,
                        "article_content": None,
                        "article_tags": [tag] if tag else [],
                        "article_slug": slug,
                        "article_url": url_full
                    })

                if stop_pagination:
                    break

                page_number += 1

            await self.scrape_article_pages(context)

            await browser.close()

            self.items = [item for item in self.items if item["article_content"]]
            return self.items

    async def scrape_article_pages(self, context):
        page = await context.new_page()

        for idx, item in enumerate(self.items, start=1):
            url = item["article_url"]
            if not url:
                continue

            if url.lower().endswith(".pdf"):
                item["article_content"] = url
                continue

            try:
                await page.goto(url, timeout=120000)
                await asyncio.sleep(self.sleep_time)

                try:
                    content_el = await page.locator("div.richtext__content").all_text_contents()
                    full_text = " ".join(map(str.strip, content_el)).strip()
                except:
                    full_text = ""

                try:
                    mark_paras = await page.locator("div.terms-normal p").all_text_contents()
                    mark_line = next((p.strip() for p in mark_paras if p.strip().startswith("MARK")), None)

                    if mark_line:
                        match = re.search(r"MARK-[\w-]*?(\d{4}-\d{2}-\d{2})", mark_line)
                        if match:
                            parsed_date = datetime.strptime(match.group(1), "%Y-%m-%d").date()
                        else:
                            dt = parser.parse(mark_line, fuzzy=True)
                            parsed_date = datetime(dt.year, dt.month, 1).date()
                    else:
                        parsed_date = parser.parse(item["article_date"], fuzzy=True).date()

                except:
                    parsed_date = parser.parse(item["article_date"], fuzzy=True).date()

                item["article_date"] = str(parsed_date)
                item["article_content"] = full_text

            except Exception as e:
                logger.error(f"Failed to scrape article #{idx}: {e}")

        await page.close()


async def ASGIUSFA(target_date):
    results = []
    try:
        url = "https://www.allspringglobal.com/insights/"
        scraper = AllSpringScraperGlobal(target_date)
        results = await scraper.scrape(url)

        output_path = f"/tmp/{company_site_id}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)

        logger.info(f"Scraped {len(results)} Global articles after {target_date}")
        return 200

    except Exception as error:
        logger.error(f"Error: {error}")
        return 500

if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2026-01-01"
    logger.info(f"Scraping Global articles using target date: {target_date}")
    asyncio.run(ASGIUSFA(target_date=target_date))
