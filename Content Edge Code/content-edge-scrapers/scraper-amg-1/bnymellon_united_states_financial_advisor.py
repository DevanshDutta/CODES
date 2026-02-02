import asyncio
import json
import logging
import sys
import re
from datetime import datetime
from dateutil import parser
from playwright.async_api import async_playwright

# --- Site metadata ---
site = "BNY Mellon Investment Management"
section = "Insights"
company_site_id = "am-255"
country = "United States"
role = "Financial Advisor"
BASE_URL = "https://www.bny.com"

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
class BNYMIMScraperUS:
    def __init__(self, target_date, sleep_time=5):
        self.target_date = parser.parse(target_date, fuzzy=True).date()
        self.sleep_time = sleep_time
        self.items = []

    async def scrape(self, url):
        logger.info(f"Starting scraper for URL: {url}")

        async with async_playwright() as p:
            browser = await p.chromium.launch(
            headless=False,
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

            # Step 2: Handle disclaimer
            try:
                await page.locator("#selector-form-confirm").click(timeout=5000)
                await page.locator("#im-jurisdiction").click(timeout=5000)
                await page.get_by_role("button", name="Proceed").click()
                logger.debug("Disclaimer accepted successfully")
                await asyncio.sleep(self.sleep_time)
            except Exception as e:
                logger.warning(f"Disclaimer skipped or not present: {e}")

            # Step 3: Close cookie banner
            try:
                await page.locator("#onetrust-close-btn-container").click(timeout=5000)
                logger.debug("Cookie banner closed")
                await asyncio.sleep(1)
            except Exception as e:
                logger.warning(f"Cookie banner not found: {e}")

            # Step 4: Collect all article cards
            try:
                await page.wait_for_selector(".cmp-articleindex__card-wrapper.card-wrapper",
                                             state="attached", timeout=15000)
                await page.evaluate("window.scrollTo(0,document.body.scrollHeight)")
                cards = await page.locator(".cmp-articleindex__card-wrapper.card-wrapper").all()
                logger.info(f"Found {len(cards)} article cards on the page")
            except Exception as e:
                logger.error(f"Could not find article cards: {e}")
                await browser.close()
                return []

            # Step 5: Extract article summaries
            for idx, card in enumerate(cards, start=1):
                try:
                    tag = await card.locator(".bny_section").text_content() or None
                except:
                    tag = None

                try:
                    title = await card.locator("h4").text_content() or None
                except:
                    title = None

                try:
                    href = await card.locator("a.bny_link").get_attribute("href")
                    slug = href.rstrip("/").split("/")[-1] if href else None
                except:
                    href = slug = None

                if not href:
                    logger.debug(f"Skipping card #{idx}: No URL found")
                    continue

                url_full = href if href.startswith("http") else BASE_URL + href
                logger.debug(f"#{idx}: {title} -> {url_full}")

                self.items.append({
                    "company_site_id": company_site_id,
                    "company_site_country": country,
                    "company_site_role": role,
                    "article_source": site,
                    "article_section": section,
                    "article_date": None,
                    "article_title": title,
                    "article_description": None,
                    "article_content": None,
                    "article_tags": [tag] if tag else [],
                    "article_slug": slug,
                    "article_url": url_full
                })

            # Step 6: Scrape article pages for content
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

            # Skip PDFs
            if url.lower().endswith(".pdf"):
                item["article_content"] = url
                item["article_date"] = str(self.target_date)
                logger.debug(f"#{idx}: Skipped PDF -> {url}")
                continue

            try:
                await page.goto(url, timeout=60000)
                await asyncio.sleep(self.sleep_time)

                # Extract article content
                content_el = await page.locator("div.cmp-text.wysiwyg").all_text_contents()
                full_text = " ".join(map(str.strip, content_el)).strip()

                #  date extraction logic 
                try:
                    mark_paras = await page.locator("div.terms-normal p").all_text_contents()
                    mark_line = next(
                        (p.strip() for p in mark_paras if p.strip().startswith("MARK")),
                        None
                    )

                    if mark_line:
                        logger.debug(f"Found MARK line: {mark_line}")
                        match = re.search(r"MARK-[\w-]*?(\d{4}-\d{2}-\d{2})", mark_line)
                        if match:
                            parsed_date = datetime.strptime(match.group(1), "%Y-%m-%d").date()
                        else:
                            try:
                                dt = parser.parse(mark_line, fuzzy=True)
                                parsed_date = datetime(dt.year, dt.month, 1).date()
                            except Exception:
                                parsed_date = self.target_date
                    else:
                        logger.debug("No MARK line found — using target_date")
                        parsed_date = self.target_date

                except Exception as e:
                    logger.warning(f"Could not extract MARK date: {e}")
                    parsed_date = self.target_date

                # Skip old articles
                if parsed_date < self.target_date:
                    logger.debug(f"Skipping outdated article: {parsed_date} < {self.target_date}")
                    #continue
                    break

                item["article_date"] = str(parsed_date)
                item["article_content"] = full_text

                logger.debug(f"Scraped article #{idx}: {item['article_title'][:60]} ({parsed_date})")

            except Exception as e:
                logger.error(f"Failed to scrape article #{idx}: {e}")

        await page.close()


async def BNYMIMUSFA(target_date):
    results = []
    try:
        url = "https://www.bnymellonim.com/us/en/intermediary/perspectives/all-perspectives.html"
        scraper = BNYMIMScraperUS(target_date)
        results = await scraper.scrape(url)

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
        BNYMIMUSFA(
            target_date=target_date
        )
    )

