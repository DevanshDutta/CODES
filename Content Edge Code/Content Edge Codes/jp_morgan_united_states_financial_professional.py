import asyncio
import json
import logging
import sys
import re
from datetime import datetime, timedelta
from dateutil import parser
from playwright.async_api import async_playwright

site = "J.P. Morgan Asset Management"
section = "Insights"
company_site_id = "am-247"
country = "United States"
role = "Financial Professional"
BASE_URL = "https://am.jpmorgan.com"

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
class JPMScraperGlobal:
    def __init__(self, target_date, sleep_time=5):
        self.target_date = parser.parse(target_date, fuzzy=True).date()
        self.sleep_time = sleep_time
        self.items = []
        self.seen_slugs = set()

    def parse_relative_date(self, raw):
        raw = raw.lower().strip()
        m = re.search(r"(\d+)\s+day", raw)
        if m:
            return datetime.today().date() - timedelta(days=int(m.group(1)))
        return None

    async def handle_popups(self, page):
        try:
            await page.wait_for_timeout(1000)

            try:
                await page.locator("#onetrust-accept-btn-handler").click(timeout=3000)
                logger.debug("OneTrust cookie accepted")
            except:
                pass

            for frame in page.frames:
                try:
                    accept_btn = frame.locator("a.accept")
                    if await accept_btn.count() > 0:
                        await accept_btn.first.click(timeout=3000)
                        logger.debug("Secondary accept popup closed")
                        return
                except:
                    continue
        except:
            pass
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

            await page.set_extra_http_headers({
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Connection": "keep-alive",
            })

            await page.set_content("<meta http-equiv='X-Content-Type-Options' content='nosniff'>")

            logger.info(f"Scraping listing page: {url}")
            await page.goto(url, timeout=120000)
            await asyncio.sleep(self.sleep_time)

            await self.handle_popups(page)
            while True:
                await asyncio.sleep(2)

                try:
                    cards = await page.locator("div.AMCard").all()

                    if cards:
                        logger.info(f"Currently loaded cards: {len(cards)}")

                        last_raw = await cards[-1].locator(
                            "span.EditorialLandingPage_tileFooterDate"
                        ).text_content()

                        last_raw = last_raw.strip()

                        if "-" in last_raw:
                            last_date = parser.parse(last_raw).date()
                        else:
                            last_date = self.parse_relative_date(last_raw) or self.target_date

                        if last_date < self.target_date:
                            logger.info(f"Stopping View More — hit old article ({last_date})")
                            break

                    try:
                        view_more_btn = page.get_by_role("button", name="View more")
                        if await view_more_btn.is_visible():
                            logger.info("Clicking VIEW MORE button")
                            await view_more_btn.click()
                            await asyncio.sleep(self.sleep_time)
                            await self.handle_popups(page)
                            continue
                    except:
                        pass

                    logger.info("No more View More button visible — stop loading.")
                    break

                except Exception as e:
                    logger.error(f"Error while expanding results: {e}")
                    break

            cards = await page.locator("div.AMCard").all()

            for card in cards:
                try:
                    title = await card.locator("div.AMCard_title div").text_content()
                    title = title.strip()
                except:
                    title = None

                try:
                    href = await card.locator("a.AMCard_button").get_attribute("href")
                    slug = href.rstrip("/").split("/")[-1] if href else None
                except:
                    href = slug = None

                if not href:
                    continue

                url_full = href if href.startswith("http") else BASE_URL + href

                if slug in self.seen_slugs:
                    continue
                self.seen_slugs.add(slug)

                try:
                    description = await card.locator("div.AMCard_description div").text_content()
                    description = description.strip()
                except:
                    description = None

                try:
                    raw_date = await card.locator("span.EditorialLandingPage_tileFooterDate").text_content()
                    raw_date = raw_date.strip()
                except:
                    raw_date = ""

                if "-" in raw_date:
                    parsed_date = parser.parse(raw_date).date()
                else:
                    parsed_date = self.parse_relative_date(raw_date) or self.target_date

                if parsed_date < self.target_date:
                    continue

                self.items.append({
                    "company_site_id": company_site_id,
                    "company_site_country": country,
                    "company_site_role": role,
                    "article_source": site,
                    "article_section": section,
                    "article_date": str(parsed_date),
                    "article_title": title,
                    "article_description": description,
                    "article_content": None,
                    "article_tags": [],
                    "article_slug": slug,
                    "article_url": url_full
                })

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

            try:
                await page.goto(url, timeout=120000)
                await asyncio.sleep(self.sleep_time)

                try:
                    blocks = await page.locator("div.jpm-am-editorial-rich-text-field").all_text_contents()
                    full_text = " ".join([x.strip() for x in blocks if x.strip()])
                except:
                    full_text = ""

                item["article_content"] = full_text

            except Exception as e:
                logger.error(f"Failed to scrape article #{idx}: {e}")

        await page.close()


async def JPMUSFP(target_date):
    results = []
    try:
        urls = [
            "https://am.jpmorgan.com/us/en/asset-management/adv/insights/market-insights/",
            "https://am.jpmorgan.com/us/en/asset-management/adv/insights/portfolio-insights/",
            "https://am.jpmorgan.com/us/en/asset-management/adv/insights/retirement-insights/",
            "https://am.jpmorgan.com/us/en/asset-management/adv/insights/etf-insights/",
        ]

        scraper = JPMScraperGlobal(target_date)
        for url in urls:
            logger.info(f"Starting scrape for: {url}")
            try:
                part_results = await scraper.scrape(url)
                results.extend(part_results)
            except Exception as e:
                logger.error(f"Failed scraping {url}: {e}")
        dedup = {}
        for item in results:
            dedup[item["article_slug"]] = item
        final_results = list(dedup.values())
        output_path = f"/tmp/{company_site_id}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(final_results, f, indent=4, ensure_ascii=False)
        logger.info(f"Scraped {len(final_results)} JPM articles after {target_date}")
        return 200
    except Exception as error:
        logger.error(f"Error: {error}")
        return 500


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-12-01"
    logger.info(f"Scraping JPM articles using target date: {target_date}")
    asyncio.run(JPMUSFP(target_date=target_date))
