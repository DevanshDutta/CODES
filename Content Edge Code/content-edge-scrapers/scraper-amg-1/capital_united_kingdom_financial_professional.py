import asyncio
import json
import logging
import sys
from dateutil import parser
from playwright.async_api import async_playwright

# --- Site metadata ---
site = "Capital Group"
section = "Insights"
country = "United Kingdom"
role = "Financial Professional"
company_site_id = "am-224"
BASE_URL = "https://www.capitalgroup.com"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)


class CapitalScraper:
    def __init__(self, target_date, sleep_time=5):
        self.target_date = parser.parse(target_date, fuzzy=True).date()
        self.sleep_time = sleep_time
        self.items = []

    async def scrape(self, url):
        logger.info(f"Starting scraper for: {url}")

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
                    ]
            )

            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
            page = await context.new_page()

            # Load page
            await page.goto(url, timeout=60000)
            await asyncio.sleep(self.sleep_time)

            # Disclaimer
            try:
                await page.locator("#attestationAccept").click(timeout=2000)
                logger.debug("Accepted disclaimer")
            except:
                logger.debug("No disclaimer found")

            # Pagination loop (BNY Mellon logic copied)
            while True:
                await page.wait_for_selector(".gds-card-base__wrapper")
                cards = await page.locator(".gds-card-base__wrapper",has=page.locator(".gds-card-base__publish-date")).all()

                logger.info(f"Found {len(cards)} cards so far...")

                # --- Parse last card date ---
                try:
                    last_card = cards[-1]
                    date_text = await last_card.locator(".gds-card-base__publish-date").text_content()
                    last_date = parser.parse(date_text, fuzzy=True).date()
                    logger.debug(f"Last card date: {last_date}")
                except Exception as e:
                    logger.error(f"Failed to get last card date: {e}")
                    break

                # --- Continue loading while cards are still newer than target_date ---
                if last_date >= self.target_date:
                    load_btn = page.locator("button.cmp-editorialCard--load-more-btn")

                    if await load_btn.count() > 0:
                        logger.info("Clicking Load More")
                        await load_btn.first.click()
                        await asyncio.sleep(self.sleep_time)
                    else:
                        logger.info("Load More not found â€” scrolling...")
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await asyncio.sleep(2)

                        if await load_btn.count() > 0:
                            logger.info("Load More appeared after scroll â€” clicking")
                            await load_btn.first.click()
                            await asyncio.sleep(self.sleep_time)
                        else:
                            logger.info("No more Load More button â€” stopping")
                            break
                else:
                    logger.info("Last date below threshold â€” stopping pagination")
                    break

            # Final fetch
            cards = await page.locator(".gds-card-base__wrapper",has=page.locator(".gds-card-base__publish-date")).all()
            logger.info(f"Total collected cards: {len(cards)}")

            # Extract data
            for card in cards:
                try:
                    date_text = await card.locator(".gds-card-base__publish-date").text_content()
                    article_date = parser.parse(date_text, fuzzy=True).date()
                except:
                    continue

                title = await card.locator(".gds-base__headline").text_content() or ""
                try:
                    tag = await card.locator(".gds-base__eyebrow").text_content(timeout=100) 
                except:
                    tag=""
                description = None

                href = await card.get_attribute("data-href") or ""
                article_url = (BASE_URL + href) if href.startswith("/") else href
                slug = href.rstrip("/").split("/")[-1] if href else None

                logger.info(f"Found Title {title} dated {str(article_date)}")

                if article_date < self.target_date:
                    continue

                item={
                    "company_site_id": company_site_id,
                    "company_site_country": country,
                    "company_site_role": role,
                    "article_source": site,
                    "article_section": section,
                    "article_date": str(article_date),
                    "article_title": title,
                    "article_description": description,
                    "article_content": None,
                    "article_tags": [tag] if tag else [],
                    "article_slug": slug,
                    "article_url": article_url
                }
                self.items.append(item)

            await self.scrape_article_pages(context)
            await browser.close()
            return self.items

    async def scrape_article_pages(self, context):
        logger.debug("Scraping individual articles...")

        for item in self.items:
            url = item["article_url"]
            if not url:
                continue

            if url.endswith(".pdf"):
                item["article_content"] = url
                continue

            page = await context.new_page()
            try:
                await page.goto(url, timeout=60000)
                await asyncio.sleep(self.sleep_time)

                await page.wait_for_selector(".text", timeout=5000)

                paragraphs = await page.locator(".text p").all_text_contents()

                full_text = " ".join(p.strip() for p in paragraphs if p.strip())
                item["article_content"] = full_text
                logger.debug(f"Succesfully scrape :{url}")

            except Exception as e:
                logger.error(f"Failed to scrape {url}: {e}")
                item["article_content"] = None
            finally:
                await page.close()


async def CapitalUKFP(target_date):
    url = "https://www.capitalgroup.com/intermediaries/gb/en/insights.html"
    scraper = CapitalScraper(target_date)
    results = await scraper.scrape(url)
    output_path=f"/tmp/{company_site_id}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    logger.info(f" Scraped {len(results)} articles after {target_date}")
    return 200 


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-09-01"
    logger.info(f"Scraping articles using target date:{ target_date}")
    asyncio.run(CapitalUKFP(target_date=target_date))

