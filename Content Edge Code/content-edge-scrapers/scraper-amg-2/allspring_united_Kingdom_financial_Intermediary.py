import asyncio
import json
import logging
import sys
import re
from datetime import datetime
from dateutil import parser
from playwright.async_api import async_playwright

# --- Site metadata ---
site = "Allspring Global Investments"
section = "Insights"
company_site_id = "am-339"
country = "United Kingdom"
role = "Financial Intermediary"
BASE_URL = "https://www.allspringglobal.com"

# --- Logging setup ---
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)


class AllSpringScraperGlobal:
    def __init__(self, target_date, sleep_time=5):
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
                    "Chrome/91.0.4472.124 Safari/537.36"
                )
            )
            page = await context.new_page()

            await page.set_extra_http_headers(
                {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-User": "?1",
                    "Cache-Control": "max-age=0",
                }
            )

            await page.set_content("<meta http-equiv='X-Content-Type-Options' content='nosniff'>")

            page_index = 1
            while True:
                if page_index == 1:
                    logger.info(f"Scraping listing page {page_index}: {url}")
                    await page.goto(url, timeout=120000)
                    await asyncio.sleep(self.sleep_time)
                else:
                    logger.info(f"Clicking pagination button for page {page_index}")
                    try:
                        await page.locator(
                            f"nav.allspring-pagination button.score-button.text-secondary[data-page='{page_index}']"
                        ).click()
                        await asyncio.sleep(self.sleep_time)
                    except Exception as e:
                        logger.info(f"No pagination button for page {page_index} → stopping. Error: {e}")
                        break

                try:
                    await page.locator("#dropdown-location-button").click(timeout=3000)
                    await asyncio.sleep(1)

                    await page.locator("#dropdown-location-option-gb").click(timeout=3000)
                    await asyncio.sleep(1)

                    await page.locator(
                        "button.self-id__role:has(div.self-id__role-title:has-text('Financial Intermediary'))"
                    ).click(timeout=3000)
                    await asyncio.sleep(1)

                    await page.locator("button.self-id__footer-terms-actions--submit").click(timeout=3000)
                    await asyncio.sleep(self.sleep_time)

                except Exception as e:
                    logger.debug(f"Self-ID popup not shown or already accepted: {e}")

                try:
                    await page.locator("#onetrust-close-btn-container").click(timeout=2000)
                    await asyncio.sleep(1)
                except:
                    pass

                try:
                    await page.wait_for_selector("a.card.insight-card", state="attached", timeout=15000)
                    cards = await page.locator("a.card.insight-card").all()
                except Exception:
                    logger.info("No cards found — stopping pagination.")
                    break

                if not cards:
                    logger.info("Empty card list — stopping pagination.")
                    break

                logger.info(f"Found {len(cards)} cards on page {page_index}")

                stop_now = False

                for card in cards:
                    try:
                        href = await card.get_attribute("href")
                        if not href:
                            continue
                        url_full = href if href.startswith("http") else BASE_URL + href
                        slug = href.rstrip("/").split("/")[-1]
                    except:
                        continue

                    if slug in self.seen_slugs:
                        continue
                    self.seen_slugs.add(slug)

                    try:
                        title = await card.locator("p.card__heading").text_content()
                        if title:
                            title = title.strip()
                    except:
                        title = None

                    try:
                        date_text = await card.locator("span.article__date").text_content()
                        date_text = date_text.strip()
                        parsed_listing_date = parser.parse(date_text, dayfirst=True, fuzzy=True).date()
                    except:
                        parsed_listing_date = self.target_date

                    if parsed_listing_date < self.target_date:
                        stop_now = True
                        break

                    try:
                        description = await card.locator("div.card__sub-body p").text_content()
                        if description:
                            description = description.strip()
                    except:
                        description = None

                    try:
                        tag = await card.locator("span.article__topic").text_content()
                        if tag:
                            tag = tag.strip()
                    except:
                        tag = None

                    self.items.append(
                        {
                            "company_site_id": company_site_id,
                            "company_site_country": country,
                            "company_site_role": role,
                            "article_source": site,
                            "article_section": section,
                            "article_date": str(parsed_listing_date),
                            "article_title": title,
                            "article_description": description,
                            "article_content": None,
                            "article_tags": [tag] if tag else [],
                            "article_slug": slug,
                            "article_url": url_full,
                        }
                    )

                if stop_now:
                    logger.info("Hit old article → stopping pagination.")
                    break

                page_index += 1

            await self.scrape_article_pages(context)

            await browser.close()

            self.items = [item for item in self.items if item["article_content"]]
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
                await asyncio.sleep(self.sleep_time)

                try:
                    content_el_list = await page.locator("div.richtext__content").all_text_contents()
                    full_text = " ".join([c.strip() for c in content_el_list if c.strip()]).strip()
                except:
                    full_text = ""

                item["article_content"] = full_text

            except Exception as e:
                logger.error(f"Failed to scrape article #{idx}: {e}")

        await page.close()


async def ASGIUKFI(target_date):
    results = []
    try:
        url = "https://www.allspringglobal.com/insights/"
        scraper = AllSpringScraperGlobal(target_date)
        results = await scraper.scrape(url)

        output_path = f"/tmp/{company_site_id}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)

        logger.info(f"Scraped {len(results)} UK Financial Intermediary articles after {target_date}")
        return 200

    except Exception as error:
        logger.error(f"Error: {error}")
        return 500


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2026-01-01"
    logger.info(f"Scraping UK Financial Intermediary articles using target date: {target_date}")
    asyncio.run(ASGIUKFI(target_date=target_date))
