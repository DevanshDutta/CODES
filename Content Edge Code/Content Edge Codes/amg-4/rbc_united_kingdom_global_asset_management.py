import asyncio
import json
import logging
import sys
from datetime import datetime
from dateutil import parser
import re
from playwright.async_api import async_playwright

site = "RBC Global Asset Management"
section = "Insights"
company_site_id = "am-405"
country = "United Kingdom"
role = "Wholesale Investment Professional"
BASE_URL = "https://www.rbcbluebay.com"


def extract_date(text):
    pattern = r"([A-Za-z]+\s+\d{1,2},?\s+\d{4})"
    match = re.search(pattern, text)
    return match.group(1) if match else None


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)


class RBCBlueBayUKScraper:
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
                    "--window-size=1280,1696",
                ],
            )

            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
            page = await context.new_page()

            await page.goto(url, timeout=60000)
            await asyncio.sleep(self.sleep_time)

            try:
                await page.evaluate("""
                    const modal = document.querySelector('#siteEntryModal');
                    if (modal) modal.remove();
                    document.querySelectorAll('.modal-backdrop').forEach(el => el.remove());
                    document.body.classList.remove('modal-open');
                    document.body.style.overflow = 'auto';
                """)
                logger.info("Popup force-removed after page load")
            except Exception:
                pass


            current_page = 1
            last_seen_date = None

            while True:
                await page.wait_for_selector(".insights-item", timeout=15000)
                cards = await page.locator(".insights-item").all()
                logger.debug(f"DEBUG: Page {current_page} – Found {len(cards)} cards")

                for card in cards:
                    try:
                        raw_date = await card.locator(".bb-author--date").text_content()
                        clean_date = extract_date(raw_date)
                        date = parser.parse(clean_date).date() if clean_date else None
                    except Exception:
                        date = None

                    if not date or date < self.target_date:
                        continue

                    title = await card.locator("h4").text_content() or ""
                    href = await card.locator("a.bb-card--link").first.get_attribute("href")
                    slug = href.rstrip("/").split("/")[-1] if href else None

                    self.items.append({
                        "company_site_id": company_site_id,
                        "company_site_country": country,
                        "company_site_role": role,
                        "article_source": site,
                        "article_section": section,
                        "article_date": str(date),
                        "article_title": title,
                        "article_description": None,
                        "article_content": None,
                        "article_tags": [],
                        "article_slug": slug,
                        "article_url": BASE_URL + href if href and href.startswith("/") else href,
                    })


                try:
                    last_card = cards[-1]
                    raw_date = await last_card.locator(".bb-author--date").text_content()
                    clean_date = extract_date(raw_date)
                    last_date = parser.parse(clean_date).date()
                    logger.debug(f"Last card date: {last_date}")
                except Exception as e:
                    logger.error(f"Date parse failed: {e}")
                    break

                if last_seen_date == last_date:
                    logger.info("Pagination stopped — same date detected again")
                    break

                last_seen_date = last_date

                if last_date < self.target_date:
                    logger.info("Target date reached — stopping pagination")
                    break

                next_page = page.locator(
                    f"ul.page-count-wrapper a.page-link:not(.active):has-text('{current_page + 1}')"
                )

                if await next_page.count() == 0:
                    logger.info("No next page found — stopping pagination")
                    break

                logger.info(f"Moving to page {current_page + 1}")

                try:
                    await page.evaluate("""
                        const modal = document.querySelector('#siteEntryModal');
                        if (modal) modal.remove();
                        document.querySelectorAll('.modal-backdrop').forEach(el => el.remove());
                        document.body.classList.remove('modal-open');
                        document.body.style.overflow = 'auto';
                    """)
                except Exception:
                    pass

                await next_page.first.click()
                await asyncio.sleep(self.sleep_time)
                current_page += 1

            await self.scrape_article_pages(context)
            await browser.close()
            return self.items

    async def scrape_article_pages(self, context):
        logger.debug("DEBUG: Starting to scrape individual articles")

        for item in self.items:
            url = item["article_url"]
            if not url:
                continue
            page = await context.new_page()
            try:
                await page.goto(url, timeout=60000)
                await asyncio.sleep(self.sleep_time)

                container = page.locator("div.container.bb-rich-text")

                try:
                    description = await container.locator("h2").first.text_content()
                    item["article_description"] = description.strip()
                except Exception:
                    item["article_description"] = None

                texts = await container.locator("p, li, h3").all_text_contents()
                item["article_content"] = " ".join(t.strip() for t in texts if t.strip())

            except Exception as e:
                logger.error(f"ERROR scraping {url}: {e}")
                item["article_content"] = None
                item["article_description"] = None
            finally:
                await page.close()


async def RBCUKIP(target_date):
    url = "https://www.rbcbluebay.com/en-gb/wholesale/what-we-think/insights/"
    scraper = RBCBlueBayUKScraper(target_date)
    results = await scraper.scrape(url)

    output_path = f"/tmp/{company_site_id}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    logger.info(f"Scraped {len(results)} articles after {target_date}")
    return 200


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-12-01"
    logger.info(f"Scraping articles using target date: {target_date}")
    asyncio.run(RBCUKIP(target_date))
