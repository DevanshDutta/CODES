import asyncio
import json
import logging
import sys
from datetime import datetime
from dateutil import parser
import re
from playwright.async_api import async_playwright


site = "Charles Schwab Investment Management"
section = "Insights"
company_site_id = "am-424"
country = "Global"
role = "Corporate"
BASE_URL = "https://www.schwabassetmanagement.com"


def extract_date(text):
    try:
        return parser.parse(text, fuzzy=True).date()
    except Exception:
        return None


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)


class SchwabScraper:
    def __init__(self, target_date, sleep_time=5):
        self.target_date = parser.parse(target_date, fuzzy=True).date()
        self.sleep_time = sleep_time
        self.items = []
        self.seen_urls = set()

    async def scrape(self, url):
        logger.debug(f"DEBUG: Starting Playwright scraper for {url}")

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                channel="chrome",
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
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/91.0.4472.124 Safari/537.36"
                ),
                locale="en-US",
                timezone_id="America/New_York",
            )

            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)

            page = await context.new_page()

            await page.goto(url, timeout=60000, wait_until="networkidle")
            await asyncio.sleep(self.sleep_time)

            try:
                await page.wait_for_selector(
                    "a.proxy-option[data-segment='1931']",
                    timeout=20000
                )
                await page.click("a.proxy-option[data-segment='1931']")
                logger.info("Clicked Advisor role")

                await page.wait_for_selector(
                    "button.ui-button.csim-btn--primary--color-1",
                    timeout=15000
                )
                await page.click(
                    "button.ui-button.csim-btn--primary--color-1"
                )
                logger.info("Clicked Confirm button")
                await asyncio.sleep(self.sleep_time)
            except Exception:
                pass

            while True:
                await page.wait_for_selector(
                    "div.card.card--default",
                    timeout=20000
                )

                cards = await page.locator("div.card.card--default").all()
                logger.debug(f"DEBUG: Found {len(cards)} cards on current page")

                stop_pagination = False

                for card in cards:
                    try:
                        raw_date = await card.locator(
                            "div.field--name-field-first-published time"
                        ).text_content()
                        date = extract_date(raw_date)
                    except Exception:
                        continue

                    if not date:
                        continue

                    if date < self.target_date:
                        stop_pagination = True
                        continue

                    title_el = card.locator("h3.card__title a")
                    href = await title_el.get_attribute("href")

                    if not href or href in self.seen_urls:
                        continue

                    self.seen_urls.add(href)

                    title = await title_el.text_content()
                    description = await card.locator(
                        "div.card__body div.field--name-body"
                    ).text_content()

                    self.items.append({
                        "company_site_id": company_site_id,
                        "company_site_country": country,
                        "company_site_role": role,
                        "article_source": site,
                        "article_section": section,
                        "article_date": str(date),
                        "article_title": title.strip(),
                        "article_description": description.strip(),
                        "article_content": None,
                        "article_tags": [],
                        "article_slug": href.rstrip("/").split("/")[-1],
                        "article_url": BASE_URL + href
                    })

                if stop_pagination:
                    logger.info("Reached older articles — stopping pagination")
                    break

                next_page = page.locator("li.pager__item--next a")
                if await next_page.count() > 0:
                    logger.info("Moving to next page")
                    await next_page.first.click()
                    await asyncio.sleep(self.sleep_time)
                else:
                    logger.info("No next page found — stopping.")
                    break

            await self.scrape_article_pages(context)
            await browser.close()
            return self.items

    async def scrape_article_pages(self, context):
        logger.debug("DEBUG: Scraping article detail pages")

        for item in self.items:
            page = await context.new_page()
            try:
                await page.goto(
                    item["article_url"],
                    timeout=60000,
                    wait_until="networkidle"
                )
                await asyncio.sleep(self.sleep_time)

                paragraphs = await page.locator(
                    "div.field--name-body p"
                ).all_text_contents()

                item["article_content"] = " ".join(
                    p.strip() for p in paragraphs if p.strip()
                )

            except Exception as e:
                logger.error(f"ERROR scraping {item['article_url']}: {e}")
                item["article_content"] = None
            finally:
                await page.close()


async def CSIMGC(target_date):
    url = "https://www.schwabassetmanagement.com/section/investment-insights"
    scraper = SchwabScraper(target_date)
    results = await scraper.scrape(url)

    output_path = f"/tmp/{company_site_id}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    logger.info(f"Scraped {len(results)} articles after {target_date}")
    return 200


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-12-01"
    logger.info(f"Scraping articles using target date:{target_date}")
    asyncio.run(CSIMGC(target_date))
