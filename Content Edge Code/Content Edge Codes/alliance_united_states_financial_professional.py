import asyncio
import json
import logging
import sys
from datetime import datetime
from dateutil import parser
import re
from playwright.async_api import async_playwright

site = "Alliance Bernstein"
section = "Insights"
company_site_id = "am-289"
country = "United States"
role = "Financial Professional"
BASE_URL = "https://www.alliancebernstein.com"

def extract_date(text):
    if not text:
        return None
    pattern = r"([A-Za-z]{3,9}\s+\d{1,2}(?:st|nd|rd|th)?,?\s*\d{4}|\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]{3,9}\s*\d{4})"
    match = re.search(pattern, text)
    if not match:
        return None
    date_str = match.group(1)
    date_str = re.sub(r"(st|nd|rd|th)", "", date_str)
    return date_str.strip()

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)

class AllianceScraper:
    def __init__(self, target_date, sleep_time=5):
        self.target_date = parser.parse(target_date, fuzzy=True).date()
        self.sleep_time = sleep_time
        self.items = []
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
            await page.goto(url, timeout=60000)
            await asyncio.sleep(self.sleep_time)

            try:
                await page.get_by_role("link", name="View All Insights").click()
            except Exception as e:
                logger.debug(f"Error while selecting 'View All Insights': {e}")

            while True:
                await page.wait_for_selector(".abde-insights-card", timeout=15000)
                cards = await page.locator(".abde-insights-card").all()
                logger.debug(f"DEBUG: Found {len(cards)} articles on this page")

                stop_scraping = False

                for card in cards:

                    try:
                        raw_date = await card.locator(".abde-date").text_content()
                        date_text = extract_date(raw_date)
                        article_date = parser.parse(date_text).date()
                    except Exception:
                        article_date = None

                    if article_date and article_date < self.target_date:
                        logger.debug(f"Stopping: article date {article_date} older than target {self.target_date}")
                        stop_scraping = True
                        break
                    try:
                        tag_text = await card.locator("header[role='heading']").text_content() or ""
                        tag = [x.strip() for x in tag_text.split("|")]
                    except:
                        tag = []

                    title = await card.locator(".abde-h4").text_content() or ""
                    description = await card.locator("p").first.text_content() or ""
                    href = await card.locator("a.abde-image-container").get_attribute("href")
                    slug = href.rstrip("/").split("/")[-1] if href else None

                    self.items.append({
                        "company_site_id": company_site_id,
                        "company_site_country": country,
                        "company_site_role": role,
                        "article_source": site,
                        "article_section": section,
                        "article_date": str(article_date) if article_date else "",
                        "article_title": title,
                        "article_description": description,
                        "article_content": None,
                        "article_tags": tag,
                        "article_slug": slug,
                        "article_url": href if href.startswith("http") else BASE_URL + href
                    })
                if stop_scraping:
                    break

                next_btn = page.locator("button[data-testid='next-navigation']")
                if await next_btn.count() == 0:
                    logger.debug("No Next button. Pagination finished.")
                    break
                is_disabled = await next_btn.get_attribute("disabled")
                if is_disabled:
                    logger.debug("Next button disabled. Pagination finished.")
                    break
                logger.debug("Clicking Next")
                await next_btn.click()
                await asyncio.sleep(5)

            await self.scrape_article_pages(context)
            await browser.close()
            return [
                i for i in self.items
                if i["article_date"]
                and parser.parse(i["article_date"]).date() >= self.target_date
            ]

    async def scrape_article_pages(self, context):
        logger.debug("DEBUG: Scraping individual articles")

        for item in self.items:
            url = item["article_url"]
            if not url:
                continue
            if url.endswith(".pdf"):
                item["article_content"] = url
                continue
            logger.info(f"Scraping url :{url}")
            page = await context.new_page()
            try:
                await page.goto(url, timeout=60000)
                await asyncio.sleep(self.sleep_time)

                paragraphs = await page.locator("div.ab-title-teaser p").all_text_contents()
                full_text = " ".join(p.strip() for p in paragraphs if p.strip())
                item["article_content"] = full_text

            except Exception as e:
                logger.error(f"ERROR scraping article {url}: {e}")
                item["article_content"] = None

            finally:
                await page.close()

async def ABUSFP(target_date):
    url = "https://www.alliancebernstein.com/us/en-us/investments/insights-landing.html"
    scraper = AllianceScraper(target_date)
    results = await scraper.scrape(url)
    print(results)
    output_path = f"/tmp/{company_site_id}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    logger.info(f"Scraped {len(results)} articles newer than {target_date}")
    return 200

if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-12-01"
    logger.info(f"Scraping articles using target date: {target_date}")
    asyncio.run(ABUSFP(target_date))

