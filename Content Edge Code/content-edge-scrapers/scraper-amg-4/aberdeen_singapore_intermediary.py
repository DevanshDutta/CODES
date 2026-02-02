import asyncio
import json
import logging
import sys
from datetime import datetime
from dateutil import parser
import re 
from playwright.async_api import async_playwright


site = "Aberdeen Investments"
section = "Insights"
company_site_id = "am-409"
country = "Singapore"
role = "Intermediary"
BASE_URL = "https://www.aberdeeninvestments.com"


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


class AberdeenScraper:
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
                    "--window-size=1280,1696"
                ]
            )

            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )

            page = await context.new_page()
            await page.set_extra_http_headers({
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Cache-Control": "max-age=0",
            })

            await page.goto(url, timeout=60000)
            await asyncio.sleep(self.sleep_time)

            current_page = 1

            while True:
                await page.wait_for_selector(
                    "h5.ArticleCard_article-card__title__pOQTa",
                    timeout=15000
                )

                cards = await page.locator(
                    "a:has(h5.ArticleCard_article-card__title__pOQTa)"
                ).all()

                logger.debug(f"DEBUG: Currently Found {len(cards)} articles")

                try:
                    last_card = cards[-1]
                    raw_date = await last_card.locator("time").text_content()
                    last_date = extract_date(raw_date)
                    logger.debug(f"Last visible article date: {last_date}")
                except Exception as e:
                    logger.error(f"Couldn't parse last article date :{e}")
                    break

                if last_date and last_date >= self.target_date:
                    next_page = page.locator(
                        f"div.Pagination_tile__xNmrh:not(.Pagination_selected__TFQCn):has-text('{current_page + 1}')"
                    )
                    if await next_page.count() > 0:
                        logger.info(f"Moving to page {current_page + 1}")
                        await next_page.first.click()
                        await asyncio.sleep(self.sleep_time)
                        current_page += 1
                    else:
                        logger.info("No next page found — stopping.")
                        break
                else:
                    logger.debug("Last Date exceeded ")
                    break

            cards = await page.locator(
                "a:has(h5.ArticleCard_article-card__title__pOQTa)"
            ).all()

            logger.debug(f"DEBUG: Found {len(cards)} articles")

            for idx, card in enumerate(cards, start=1):
                try:
                    raw_date = await card.locator("time").text_content()
                    date = extract_date(raw_date)
                except Exception:
                    date = ""

                if date and date < self.target_date:
                    continue

                title = await card.locator(
                    "h5.ArticleCard_article-card__title__pOQTa"
                ).text_content() or ""

                description = await card.locator(
                    "p.body-small"
                ).text_content() or ""

                href = await card.get_attribute("href")
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
                    "article_description": description,
                    "article_content": None,
                    "article_tags": [],
                    "article_slug": slug,
                    "article_url": BASE_URL + href if href and href.startswith("/") else href
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

                paragraphs = await page.locator("p").all_text_contents()
                full_text = " ".join(p.strip() for p in paragraphs if p.strip())
                item["article_content"] = full_text

                logger.debug(f"Succesfully scraped url:{url}")

            except Exception as e:
                logger.error(f"ERROR: Failed to scrape {url}: {e}")
                item["article_content"] = None
            finally:
                await page.close()


async def ABSGI(target_date):
    url = "https://www.aberdeeninvestments.com/en-sg/investor/insights-and-research/insights"
    scraper = AberdeenScraper(target_date)
    results = await scraper.scrape(url)
    output_path = f"/tmp/{company_site_id}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    logger.info(f" Scraped {len(results)} articles after {target_date}")
    return 200


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-12-01"
    logger.info(f"Scraping articles using target date:{target_date}")
    asyncio.run(ABSGI(target_date=target_date))
