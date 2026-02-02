import asyncio
import json
import logging
import sys
from dateutil import parser
from playwright.async_api import async_playwright

# --- Site metadata ---
site = "Goldman Sachs AM International"
section = "Insights"
company_site_id = "am-271"
country = "Singapore"
role = "Financial Intermediary"
BASE_URL = "https://am.gs.com"

# --- Logging setup ---
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)


class GSAMScraperGlobal:
    def __init__(self, target_date, sleep_time=5):
        self.target_date = parser.parse(target_date, fuzzy=True).date()
        self.sleep_time = sleep_time
        self.items = []
        self.seen_slugs = set()

    async def handle_audience_popup(self, page):
        try:
            await page.wait_for_timeout(1500)

            card = page.locator(
                "div[data-testid='audience-card']",
                has_text="Financial Intermediary"
            )
            if await card.count() > 0:
                await card.first.click(timeout=4000)
                await page.wait_for_timeout(1500)

            accept_btn = page.locator(
                "span.gs-uitk-c-1lo0ztl--span",
                has_text="Accept and Continue"
            )
            if await accept_btn.count() > 0:
                await accept_btn.first.click(timeout=4000)
                await page.wait_for_timeout(1500)

        except Exception as e:
            logger.debug(f"Audience popup not shown: {e}")

    async def scrape(self, url):
        logger.info(f"Starting scraper for URL: {url}")

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-gpu",
                    "--no-sandbox",
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
                )
            )
            page = await context.new_page()
            article_page = await context.new_page() 

            await page.goto(url, timeout=120000)
            await asyncio.sleep(self.sleep_time)
            await self.handle_audience_popup(page)

            stop_all = False

            while True:
                cards = await page.locator(
                    "a[data-gs-uitk-component='link'][data-analytics-component-name='insight card']"
                ).all()

                logger.info(f"Visible cards: {len(cards)}")

                for card in cards:
                    href = await card.get_attribute("href")
                    if not href:
                        continue

                    slug = href.rstrip("/").split("/")[-1]
                    if slug in self.seen_slugs:
                        continue
                    self.seen_slugs.add(slug)
                    article_url = BASE_URL + href

                    try:
                        title = await card.locator("div.gs-card-title").text_content()
                        title = title.strip()
                    except:
                        title = None

                    await article_page.goto(article_url, timeout=120000)
                    await asyncio.sleep(self.sleep_time)

                    try:
                        raw_date = await article_page.locator(
                            "span.gs-text.gs-uitk-c-lkyj7q--text-root"
                        ).first.text_content()
                        raw_date = raw_date.split("|")[0].strip()
                        article_date = parser.parse(raw_date).date()
                    except:
                        article_date = None

                    if article_date and article_date < self.target_date:
                        stop_all = True
                        break

                    try:
                        description = await article_page.locator(
                            "span.gs-text.gs-uitk-c-13zhy10--text-root--text"
                        ).first.text_content()
                        description = description.strip()
                    except:
                        description = None

                    try:
                        blocks = await article_page.locator(
                            "div[data-testid='rich-text-component']"
                        ).all_text_contents()
                        content = " ".join(x.strip() for x in blocks if x.strip())
                    except:
                        content = ""

                    self.items.append({
                        "company_site_id": company_site_id,
                        "company_site_country": country,
                        "company_site_role": role,
                        "article_source": site,
                        "article_section": section,
                        "article_date": str(article_date) if article_date else None,
                        "article_title": title,
                        "article_description": description,
                        "article_content": content,
                        "article_tags": [],
                        "article_slug": slug,
                        "article_url": article_url,
                    })

                if stop_all:
                    break

                load_more = page.locator(
                    "button[data-testid='load-more-button-container']"
                )
                if await load_more.count() > 0 and await load_more.is_visible():
                    await load_more.click()
                    await asyncio.sleep(self.sleep_time)
                else:
                    break

            await article_page.close()
            await page.close()
            await context.close()
            await browser.close()

            return self.items


async def GSAMSGFI(target_date):
    scraper = GSAMScraperGlobal(target_date)
    results = await scraper.scrape(
        "https://am.gs.com/en-sg/advisors/insights/list"
    )

    with open(f"/tmp/{company_site_id}.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    logger.info(f"Scraped {len(results)} GSAM articles after {target_date}")
    return 200


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-12-01"
    asyncio.run(GSAMSGFI(target_date))
