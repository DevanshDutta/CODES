import asyncio
import json
import logging
import sys
import re
from datetime import datetime
from dateutil import parser
from playwright.async_api import async_playwright

# --- Site metadata ---
site = "Wellington Management Company"
section = "Insights"
company_site_id = "am-296"
country = "United Kingdom"
role = "Financial professional"
BASE_URL = "https://www.wellington.com"

# --- Logging setup ---
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(company_site_id)


class WellingtonScraper:
    def __init__(self, target_date, sleep_time=3):
        self.target_date = parser.parse(target_date, fuzzy=True).date()
        self.sleep_time = sleep_time
        self.items = []
        self.seen_slugs = set()

    async def scrape(self, url):
        logger.info(f"Starting scraper for URL: {url}")

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
                    "--disable-dev-shm-usage",
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
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            )

            page = await context.new_page()
            await page.goto(url, timeout=120000)
            await page.wait_for_load_state("networkidle")

            try:
                checkbox = page.locator("input#attestation-remember")
                if await checkbox.count():
                    await checkbox.check(force=True)
                try:
                    await page.locator("button.cmp-button.accept").click(force=True)
                except:
                    await page.locator("button.accept").click(force=True)
                await page.wait_for_load_state("networkidle")
            except:
                pass

            while True:
                try:
                    load_more = page.locator("a.load-more-btn")
                    if not await load_more.is_visible():
                        break

                    await load_more.scroll_into_view_if_needed()
                    await load_more.click(force=True)
                    await page.wait_for_load_state("networkidle")

                    cards = await page.locator("div.insight__content").all()
                    if not cards:
                        break

                    last_card = cards[-1]
                    try:
                        dt_text = await last_card.locator(
                            ".insight__foot .insight__date"
                        ).text_content()
                        last_date = parser.parse(dt_text, fuzzy=True).date()

                        if last_date < self.target_date:
                            logger.info("Reached old articles → stop loading immediately")
                            break
                    except:
                        pass

                except Exception as e:
                    logger.debug(f"Load more failed: {e}")
                    break

            cards = await page.locator("div.insight__content").all()
            logger.info(f"Total cards found: {len(cards)}")

            for card in cards:
                try:
                    title_el = card.locator("a.insight__title")
                    title = (await title_el.text_content() or "").strip()
                    href = await title_el.get_attribute("href")
                    url_full = BASE_URL + href if href and href.startswith("/") else href
                    slug = url_full.rstrip("/").split("/")[-1]
                except:
                    continue

                if slug in self.seen_slugs:
                    continue
                self.seen_slugs.add(slug)

                try:
                    desc = await card.locator(".insight__body").text_content()
                    desc = desc.strip()
                except:
                    desc = None

                try:
                    date_text = await card.locator(
                        ".insight__foot .insight__date"
                    ).text_content()
                    parsed_date = parser.parse(date_text, fuzzy=True).date()
                except:
                    continue

                if parsed_date < self.target_date:
                    continue

                self.items.append({
                    "company_site_id": company_site_id,
                    "company_site_country": country,
                    "company_site_role": role,
                    "article_source": site,
                    "article_section": section,
                    "article_date": str(parsed_date),
                    "article_title": title,
                    "article_description": desc,
                    "article_content": None,
                    "article_tags": [],
                    "article_slug": slug,
                    "article_url": url_full,
                })

            await self.scrape_article_pages(context)
            await browser.close()

            self.items = [i for i in self.items if i["article_content"]]
            return self.items

    async def scrape_article_pages(self, context):
        page = await context.new_page()

        for idx, item in enumerate(self.items, start=1):
            url = item["article_url"]
            if not url:
                continue

            if url.lower().endswith(".pdf"):
                item["article_content"] = url
                continue

            try:
                logger.info(f"Scraping article #{idx}: {url}")
                await page.goto(url, timeout=120000)
                await page.wait_for_load_state("networkidle")
                await asyncio.sleep(self.sleep_time)

                try:
                    contents = await page.locator("div.text__content").all_text_contents()
                    full_text = " ".join([x.strip() for x in contents if x.strip()])
                except:
                    full_text = ""

                item["article_content"] = full_text

            except Exception as e:
                logger.error(f"Failed scraping article #{idx}: {e}")

        await page.close()


async def WMGUKFP(target_date):
    results = []
    try:
        url = "https://www.wellington.com/en-gb/professional/insights"
        scraper = WellingtonScraper(target_date)
        results = await scraper.scrape(url)

        output_path = f"/tmp/{company_site_id}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)

        logger.info(f"Scraped {len(results)} Wellington articles after {target_date}")
        return 200

    except Exception as error:
        logger.error(f"Error: {error}")
        return 500


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-12-01"
    logger.info(f"Scraping Wellington articles using target date: {target_date}")
    asyncio.run(WMGUKFP(target_date=target_date))
