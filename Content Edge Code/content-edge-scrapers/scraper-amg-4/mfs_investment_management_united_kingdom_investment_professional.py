import asyncio
import json
import logging
import sys
from datetime import datetime
from dateutil import parser
from playwright.async_api import async_playwright

site = "MFS Investment Management"
section = "Insights"
company_site_id = "am-433"
country = "United Kingdom"
role = "Investment Professional"
BASE_URL = "https://www.mfs.com"


def extract_date(text):
    try:
        return parser.parse(text.strip(), fuzzy=True).date()
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


class MFSScraper:
    def __init__(self, target_date, sleep_time=3):
        self.target_date = parser.parse(target_date, fuzzy=True).date()
        self.sleep_time = sleep_time
        self.items = []
        self.seen_urls = set()

    async def scrape(self, url):
        logger.info(f"Starting scraper for {url}")

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
                viewport={"width": 1280, "height": 1696},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            )

            page = await context.new_page()
            await page.goto(url, timeout=60000, wait_until="networkidle")
            await asyncio.sleep(self.sleep_time)

            try:
                if await page.locator("button.cta-primary.continue-btn").count() > 0:
                    logger.info("Popup detected — clicking CONTINUE")
                    await page.click("button.cta-primary.continue-btn", timeout=8000)
                    await asyncio.sleep(2)

                if await page.locator("button.cta-primary.acceptCTA").count() > 0:
                    logger.info("Accept popup detected — clicking ACCEPT & SAVE")
                    await page.click("button.cta-primary.acceptCTA", timeout=8000)
                    await asyncio.sleep(2)

                await page.wait_for_load_state("networkidle")
            except Exception:
                logger.info("Popup not shown — continuing")

            last_height = await page.evaluate("document.body.scrollHeight")

            while True:
                cards = await page.locator("div.filter-insights__results-item").all()
                logger.debug(f"Found {len(cards)} cards")

                if cards:
                    try:
                        last_card = cards[-1]
                        raw_date = await last_card.locator(
                            "div.filter-insights__result-date"
                        ).text_content()
                        last_date = extract_date(raw_date)
                        logger.debug(f"Last article date: {last_date}")

                        if last_date and last_date < self.target_date:
                            logger.info("Target date reached — stopping scroll")
                            break
                    except Exception:
                        pass

                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(self.sleep_time)

                new_height = await page.evaluate("document.body.scrollHeight")
                if new_height == last_height:
                    logger.info("No more content to load")
                    break

                last_height = new_height

            cards = await page.locator("div.filter-insights__results-item").all()
            logger.info(f"Total cards collected: {len(cards)}")

            for card in cards:
                try:
                    raw_date = await card.locator(
                        "div.filter-insights__result-date"
                    ).text_content()
                    article_date = extract_date(raw_date)
                except Exception:
                    continue

                if not article_date or article_date < self.target_date:
                    continue

                title_el = card.locator("a.heading-3")
                title = await title_el.text_content()
                href = await title_el.get_attribute("href")

                if not href or href in self.seen_urls:
                    continue

                self.seen_urls.add(href)

                try:
                    description = await card.locator(
                        "div.js-result-copy p"
                    ).text_content()
                except Exception:
                    description = None

                slug = href.rstrip("/").split("/")[-1]

                self.items.append({
                    "company_site_id": company_site_id,
                    "company_site_country": country,
                    "company_site_role": role,
                    "article_source": site,
                    "article_section": section,
                    "article_date": str(article_date),
                    "article_title": title.strip(),
                    "article_description": description.strip() if description else None,
                    "article_content": None,
                    "article_tags": [],
                    "article_slug": slug,
                    "article_url": href if href.startswith("http") else BASE_URL + href,
                })

            await self.scrape_article_pages(context)
            await browser.close()

            return self.items

    async def scrape_article_pages(self, context):
        logger.info("Scraping article detail pages")

        for item in self.items:
            page = await context.new_page()
            try:
                await page.goto(item["article_url"], timeout=60000, wait_until="networkidle")
                await asyncio.sleep(self.sleep_time)

                content_blocks = await page.locator(
                    "div.col-md-9.right-section div.rich-text"
                ).all_text_contents()

                full_text = " ".join(
                    block.strip() for block in content_blocks if block.strip()
                )

                item["article_content"] = full_text

            except Exception as e:
                logger.error(f"ERROR scraping {item['article_url']}: {e}")
                item["article_content"] = None
            finally:
                await page.close()


async def MFSUKIP(target_date):
    url = "https://www.mfs.com/en-gb/investment-professional/insights.html"
    scraper = MFSScraper(target_date)
    results = await scraper.scrape(url)

    output_path = f"/tmp/{company_site_id}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    logger.info(f"Scraped {len(results)} articles newer than {target_date}")
    return 200


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2026-01-01"
    logger.info(f"Scraping articles using target date: {target_date}")
    asyncio.run(MFSUKIP(target_date))
