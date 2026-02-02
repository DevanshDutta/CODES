import asyncio
import json
import logging
import sys
from datetime import datetime
from dateutil import parser
from playwright.async_api import async_playwright


# ---------------- SITE METADATA ----------------
site = "Dimensional Fund Advisors (DFA)"
section = "Insights"
company_site_id = "am-420"
country = "United Kingdom"
role = "Finance Professional"
BASE_URL = "https://www.dimensional.com"

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

class DimensionalScraper:
    def __init__(self, target_date, sleep_time=5):
        self.target_date = parser.parse(target_date, fuzzy=True).date()
        self.sleep_time = sleep_time
        self.items = []
        self.seen_urls = set()

    async def scrape(self, url):
        logger.info(f"Starting scraper → {url}")

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
                await page.wait_for_selector(
                    "div.audience-selector-global-splash-container",
                    timeout=10000
                )

                await page.click(
                    "button[data-qa='audience-button-professional']"
                )

                await page.wait_for_selector(
                    "input[data-qa='professional-affirmation-checkbox']",
                    timeout=10000
                )

                await page.check(
                    "input[data-qa='professional-affirmation-checkbox']",
                    force=True
                )

                await page.click(
                    "button[data-qa='professional-affirmation-button']"
                )

                await page.wait_for_selector(
                    "div.audience-selector-global-splash-container",
                    state="detached",
                    timeout=15000
                )

                logger.info("Audience selector handled")

            except Exception:
                logger.debug("Audience selector not shown")

            await asyncio.sleep(self.sleep_time)

            while True:
                await page.wait_for_selector(
                    "a.coveo-headless-content-card", timeout=15000
                )

                cards = await page.locator(
                    "a.coveo-headless-content-card"
                ).all()

                logger.info(f"Found {len(cards)} cards")
                stop_pagination = False

                for card in cards:
                    try:
                        href = await card.get_attribute("href")
                        if not href:
                            continue

                        article_url = (
                            href if href.startswith("http") else BASE_URL + href
                        )

                        if article_url in self.seen_urls:
                            continue

                        self.seen_urls.add(article_url)

                        title = await card.locator(
                            "h3.t-heading-md"
                        ).text_content()

                        logger.debug(f"Scraping article → {title}")

                        article_page = await context.new_page()
                        await article_page.goto(article_url, timeout=60000)
                        await asyncio.sleep(self.sleep_time)

                        raw_date = await article_page.locator(
                            "p.pdf--date"
                        ).first.text_content()

                        article_date = extract_date(raw_date)

                        if not article_date or article_date < self.target_date:
                            stop_pagination = True
                            await article_page.close()
                            break

                        try:
                            description = await article_page.locator(
                                "div.rtf-container p"
                            ).first.text_content()
                        except Exception:
                            description = None

                        tags = await article_page.locator(
                            "div.content-page-metadata-tags a"
                        ).all_text_contents()

                        content = await article_page.locator(
                            "div.content-column"
                        ).inner_text()

                        self.items.append(
                            {
                                "company_site_id": company_site_id,
                                "company_site_country": country,
                                "company_site_role": role,
                                "article_source": site,
                                "article_section": section,
                                "article_date": str(article_date),
                                "article_title": title.strip(),
                                "article_description": description.strip()
                                if description
                                else None,
                                "article_content": content.strip()
                                if content
                                else None,
                                "article_tags": tags,
                                "article_slug": article_url.rstrip("/").split("/")[-1],
                                "article_url": article_url,
                            }
                        )

                        await article_page.close()

                    except Exception as e:
                        logger.error(f"ERROR scraping card: {e}")

                if stop_pagination:
                    logger.info("Target date reached — stopping pagination")
                    break

                load_more = page.locator(
                    "button.coveo-headless-results-show-more-button"
                )

                if await load_more.count() > 0:
                    logger.info("Clicking More Results")
                    await load_more.first.click()
                    await asyncio.sleep(self.sleep_time)
                else:
                    logger.info("No More Results button — stopping")
                    break

            await browser.close()
            return self.items

async def DFAUKFP(target_date):
    url = "https://www.dimensional.com/gb-en/insights#t=catAll&sort=@publishdate%20descending"
    scraper = DimensionalScraper(target_date)
    results = await scraper.scrape(url)

    output_path = f"/tmp/{company_site_id}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    logger.info(f"Scraped {len(results)} articles after {target_date}")
    return 200

if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-12-01"
    logger.info(f"Scraping articles using target date: {target_date}")
    asyncio.run(DFAUKFP(target_date))