import asyncio
import json
import logging
import sys
from datetime import date, datetime
from dateutil import parser
from playwright.async_api import async_playwright

site = "M&G Investments"
section = "Insights"
company_site_id = "am-313"
country = "UK"
role = "Financial Professional"
BASE_URL = "https://www.mandg.com"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)

class MANDGScraper:
    def __init__(self, target_date, sleep_time=5):
        self.target_date = parser.parse(target_date).date()
        self.sleep_time = sleep_time
        self.items = []

    async def scrape(self, url):
        logger.debug(f"DEBUG: Starting Playwright scraper for {url}")

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--disable-dev-shm-usage",
                        "--disable-gpu",
                        "--no-sandbox",
                        "--disable-setuid-sandbox",
                        "--disable-infobars",
                        "--window-size=1920,1080",
                        "--start-maximized",
                        "--single-process",
                        "--no-zygote",
                        ])
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                )
            )
            page = await context.new_page()

            # Load page
            await page.goto(url, timeout=60000)
            await asyncio.sleep(self.sleep_time)

            # Accept terms
            try:
                await page.get_by_role("button", name="Accept and continue").click()
                await page.get_by_role("button", name="Submit").click()
            except:
                pass

            # --------------------------
            # Pagination Loop
            # --------------------------
            while True:
                cards = await page.locator("li.search-page__grid-item--article").all()
                if not cards:
                    logger.info("No cards found.")
                    break

                last_card = cards[-1]
                date_text = await last_card.locator(".article-publish-date").inner_text()
                last_date = parser.parse(date_text).date()

                if last_date >= self.target_date:
                    try:
                        await page.get_by_role("button", name="Load more").click()
                        await asyncio.sleep(2)
                    except:
                        logger.info("No more 'Load more' button.")
                        break
                else:
                    logger.info("Article older than target_date — stopping pagination")
                    break

            # --------------------------
            # Extract cards
            # --------------------------
            cards = await page.locator("li.search-page__grid-item--article").all()

            for card in cards:
                title = await card.locator("[data-testid='search-item-heading']").inner_text()
                date_text = await card.locator(".article-publish-date").inner_text()
                article_date= parser.parse(date_text, fuzzy=True).date()
                if article_date < self.target_date:
                    continue
                href = await card.locator("a.search-page__tile-link").get_attribute("href")

                slug = href.rstrip("/").split("/")[-1] if href else None

                self.items.append({
                    "company_site_id": company_site_id,
                    "company_site_country": country,
                    "company_site_role": role,
                    "article_source": site,
                    "article_section": section,
                    "article_date": date_text,
                    "article_title": title,
                    "article_description": "",
                    "article_content": None,
                    "article_tags": "",
                    "article_slug": slug,
                    "article_url": href if href.startswith("http") else BASE_URL + href
                })

            await self.scrape_article_pages(context)

            return self.items

    async def scrape_article_pages(self, context):
        logger.debug("DEBUG: Scraping individual articles")

        for item in self.items:
            url = item["article_url"]
            if not url:
                continue

            if url.endswith(".pdf"):
                item["article_content"] = url
                continue

            page = await context.new_page()
            logger.info(f"Scraping {item["article_title"]}")
            try:
                await page.goto(url, timeout=60000)
                await asyncio.sleep(self.sleep_time)

                paragraphs = await page.locator("p").all_text_contents()
                text = " ".join(p.strip() for p in paragraphs if p.strip())
                item["article_content"] = text

            except Exception as e:
                logger.error(f"Failed to scrape {url}: {e}")
                item["article_content"] = None
            finally:
                await page.close()


async def MANDGUKFP(target_date):
    url = "https://www.mandg.com/investments/professional-investor/en-gb/insights/mandg-insights/latest-insights"
    scraper = MANDGScraper(target_date)
    results = await scraper.scrape(url)

    output_path = f"/tmp/{company_site_id}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    logger.info(f"Scraped {len(results)} articles after {target_date}")
    return 200


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-10-01"
    logger.info(f"Scraping articles using target date: {target_date}")
    asyncio.run(MANDGUKFP(target_date))

