import asyncio
import json
import logging
import sys
import re
from datetime import datetime
from dateutil import parser
from playwright.async_api import async_playwright

# --- Site metadata ---
site = "RBC Global Asset Management"
section = "Insights"
company_site_id = "am-404"
country = "United States"
role = "Intermediary"
BASE_URL = "https://usmutualfunds.rbcgam.com"

# --- Logging setup ---
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)


def extract_date(text):
    match = re.search(r"([A-Za-z]{3}\s+\d{1,2},\s+\d{4})", text)
    return match.group(1) if match else None


# --- Scraper class ---
class RBCUSIMScraper:
    def __init__(self, target_date, sleep_time=5):
        self.target_date = parser.parse(target_date).date()
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
                    "--window-size=1280,1696",
                ],
            )

            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

            )
            page = await context.new_page()

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

            # Step 1: Load page
            await page.goto(url, timeout=60000)
            await asyncio.sleep(self.sleep_time)

            # Step 2: Pagination
            while True:
                await page.wait_for_selector(".media-card-content-container", timeout=15000)
                cards = await page.locator(".media-card-content-container").all()
                logger.debug(f"Found {len(cards)} cards")

                raw_date = await cards[-1].locator(
                    ".media-card-footer-detailed-text"
                ).last.text_content()

                last_date = parser.parse(extract_date(raw_date)).date()
                logger.debug(f"Last card date: {last_date}")

                if last_date >= self.target_date:
                    load_more = page.locator("button:has-text('Load more insights')")
                    if await load_more.count() > 0:
                        logger.info("Clicking Load More")
                        await load_more.first.click()
                        await asyncio.sleep(self.sleep_time)
                    else:
                        break
                else:
                    logger.info("Target date reached — stopping pagination")
                    break

            # Step 3: Extract cards (FIXED SPEED ISSUE)
            for idx, card in enumerate(cards, start=1):
                try:
                    title = await card.locator("h5").text_content()
                    description = await card.locator(".media-card-summary").text_content()

                    link = card.locator("a.media-card-header-container")
                    if await link.count() == 0:
                        raise Exception("Link not found")

                    href = await link.get_attribute("href")
                    slug = href.rstrip("/").split("/")[-1]

                    raw_date = await card.locator(
                        ".media-card-footer-detailed-text"
                    ).last.text_content()
                    article_date = parser.parse(extract_date(raw_date)).date()

                    if article_date < self.target_date:
                        continue

                    self.items.append({
                        "company_site_id": company_site_id,
                        "company_site_country": country,
                        "company_site_role": role,
                        "article_source": site,
                        "article_section": section,
                        "article_date": str(article_date),
                        "article_title": title,
                        "article_description": description,
                        "article_content": None,
                        "article_tags": [],
                        "article_slug": slug,
                        "article_url": BASE_URL + href,
                    })

                except Exception as e:
                    logger.error(f"Card parse failed #{idx}: {e}")

            # Step 4: Scrape article pages
            await self.scrape_article_pages(context)

            await browser.close()
            logger.info(f"Finished scraping {len(self.items)} articles")
            return self.items

    async def scrape_article_pages(self, context):
        page = await context.new_page()

        for idx, item in enumerate(self.items, start=1):
            try:
                await page.goto(item["article_url"], timeout=60000)
                await asyncio.sleep(self.sleep_time)

                content = await page.locator(
                    "article.section-block.content-article"
                ).all_text_contents()

                item["article_content"] = " ".join(
                    t.strip() for t in content if t.strip()
                )

            except Exception as e:
                logger.error(f"Article scrape failed #{idx}: {e}")

        await page.close()


async def RBCUSIM(target_date):
    results = []
    try:
        url = "https://usmutualfunds.rbcgam.com/us/insights/"
        scraper = RBCUSIMScraper(target_date)
        results = await scraper.scrape(url)

        output_path = f"/tmp/{company_site_id}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)

        logger.info(f"Scraped {len(results)} articles after {target_date}")
        return 200

    except Exception as error:
        logger.error(f"Error: {error}")
        return 500


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-12-01"
    logger.info(f"Scraping articles using target date: {target_date}")
    asyncio.run(
        RBCUSIM(
            target_date=target_date
        )
    )
