import asyncio
import json
import logging
import sys
from dateutil import parser
from playwright.async_api import async_playwright


site = "Natixis Investment Managers"
section = "Insights"
company_site_id = "am-308"
country = "Singapore"
role = "Financial Professional"
BASE_URL="https://www.im.natixis.com"


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger=logging.getLogger(company_site_id)

class NatixisScraper:
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

            await page.wait_for_selector("ntx-card-insight", timeout=15000)
            cards = await page.locator("ntx-card-insight").all()
            logger.debug(f"DEBUG: Found {len(cards)} articles")
            for idx, card in enumerate(cards, start=1):
                try:
                    raw_date = await card.locator("ntx-card-info").get_attribute("date")
                    date = parser.parse(raw_date,fuzzy=True).date()
                    if date < self.target_date:
                        break
                except Exception:
                    date = ""

                title = await card.locator(".ntx-insight-card-title span").inner_text() or ""
                href = await card.locator("a").get_attribute("href")
                slug = href.rstrip("/").split("/")[-1] if href else None

                try:
                    description = (
                        await card.locator(".ntx-insight-card-description").inner_text()
                    ).strip()
                except Exception:
                    description = None

                tags = []
                try:
                    tag_nodes = await card.locator(
                        ".ntx-insight-card-topic span"
                    ).all()
                    for t in tag_nodes:
                        txt = (await t.inner_text()).strip()
                        if txt:
                            tags.append(txt)
                except Exception:
                    pass

                self.items.append({
                    "company_site_id": company_site_id,
                    "company_site_country": country,
                    "company_site_role": role,
                    "article_source": site,
                    "article_section": section,
                    "article_date": str(date),
                    "article_title": title,
                    "article_description": description,
                    "article_content": None,
                    "article_tags": tags,
                    "article_slug": slug,
                    "article_url": href if href.startswith("http") else BASE_URL + href,
                })

            await self.scrape_article_pages(context)
            await browser.close()
            return self.items

    async def scrape_article_pages(self, context):
        logger.debug("DEBUG: Starting to scrape individual articles")
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

                paragraphs = await page.locator(".text").all_text_contents()
                full_text = " ".join(p.strip() for p in paragraphs if p.strip())
                item["article_content"] = full_text

            except Exception as e:
                logger.error(f"ERROR: Failed to scrape {url}: {e}")
                item["article_content"] = None
            finally:
                await page.close()


async def NatixisSGFP(target_date):
    url = "https://www.im.natixis.com/en-sg/insights"
    scraper = NatixisScraper(target_date)
    results = await scraper.scrape(url)
    output_path=f"/tmp/{company_site_id}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    logger.info(f" Scraped {len(results)} articles after {target_date}")
    return 200


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-11-01"
    asyncio.run(NatixisSGFP(target_date))
