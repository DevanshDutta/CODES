import asyncio
import json
import logging
import sys
from datetime import datetime
from dateutil import parser
from playwright.async_api import async_playwright


site = "Janus Henderson Investors"
section = "Insights"
company_site_id = "am-426"
country = "United States"
role = "Financial Professional"
BASE_URL = "https://www.janushenderson.com"


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


class JanusHendersonScraper:
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
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
            )

            page = await context.new_page()
            await page.goto(url, timeout=60000)
            await asyncio.sleep(self.sleep_time)

            # ---- LOAD MORE INSIGHTS LOOP ----
            while True:
                try:
                    await page.wait_for_selector(
                        "span.post-date", timeout=15000
                    )

                    cards = await page.locator("article").all()
                    logger.debug(f"DEBUG: Currently Found {len(cards)} articles")

                    last_card = cards[-1]
                    raw_date = await last_card.locator(
                        "span.post-date"
                    ).text_content()
                    last_date = extract_date(raw_date)

                    logger.debug(f"Last visible article date: {last_date}")

                    if last_date and last_date >= self.target_date:
                        load_more = page.locator(
                            "a.jh-btn.tertiary"
                        )
                        if await load_more.count() > 0:
                            logger.info("Clicking Load More")
                            await load_more.first.click()
                            await asyncio.sleep(self.sleep_time)
                        else:
                            logger.info("No Load More button — stopping.")
                            break
                    else:
                        logger.debug("Last Date exceeded")
                        break

                except Exception as e:
                    logger.error(f"Pagination error: {e}")
                    break

            # ---- COLLECT FINAL CARDS ----
            cards = await page.locator("article").all()
            logger.debug(f"DEBUG: Found {len(cards)} articles")

            for idx, card in enumerate(cards, start=1):
                try:
                    raw_date = await card.locator(
                        "span.post-date"
                    ).text_content()
                    date = extract_date(raw_date)
                except Exception:
                    date = ""

                if date and date < self.target_date:
                    continue

                # =====================================================
                # ✅ TITLE FIX — ONLY ADDITION / CHANGE
                # =====================================================
                title = ""

                title_el = card.locator("h2.article-title a")
                if await title_el.count() > 0:
                    title = (await title_el.inner_text()).strip()

                if not title:
                    fallback = card.locator("div.insights_title a")
                    if await fallback.count() > 0:
                        title = (await fallback.inner_text()).strip()
                # =====================================================

                description = await card.locator(
                    "div.hover_excerpt p"
                ).text_content() or ""

                href = await title_el.get_attribute("href")
                slug = href.rstrip("/").split("/")[-1] if href else None

                logger.debug(f"DEBUG: Article #{idx}: {title[:50]}...")

                self.items.append({
                    "company_site_id": company_site_id,
                    "company_site_country": country,
                    "company_site_role": role,
                    "article_source": site,
                    "article_section": section,
                    "article_date": str(date),
                    "article_title": title,
                    "article_description": description.strip(),
                    "article_content": None,
                    "article_tags": [],
                    "article_slug": slug,
                    "article_url": href if href and href.startswith("http") else BASE_URL + href if href else None,
                })

            await self.scrape_article_pages(context)
            await browser.close()

            return [
                i for i in self.items
                if i["article_date"]
                and parser.parse(i["article_date"]).date() >= self.target_date
            ]

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

                paragraphs = await page.locator(
                    "div.main__body-content.insights-single__the-content p"
                ).all_text_contents()

                item["article_content"] = " ".join(
                    p.strip() for p in paragraphs if p.strip()
                )

            except Exception as e:
                logger.error(f"ERROR scraping {url}: {e}")
                item["article_content"] = None
            finally:
                await page.close()


async def JHIUSFP(target_date):
    url = "https://www.janushenderson.com/en-us/advisor/insights/"
    scraper = JanusHendersonScraper(target_date)
    results = await scraper.scrape(url)

    output_path = f"/tmp/{company_site_id}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    logger.info(f" Scraped {len(results)} articles after {target_date}")
    return 200


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2026-01-01"
    logger.info(f"Scraping articles using target date:{target_date}")
    asyncio.run(JHIUSFP(target_date=target_date))
