import asyncio
import json
import logging
import sys
import re
from datetime import datetime
from dateutil import parser
from playwright.async_api import async_playwright

site = "Franklin Templeton"
section = "Insights"
company_site_id = "am-280"
country = "Singapore"
role = "Financial Professional"
BASE_URL = "https://www.franklintempleton.com"

logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
        force=True,
)
logger = logging.getLogger(company_site_id)

class FTScraperSG:
    def __init__(self, target_date, sleep_time=5):
        self.target_date = parser.parse(target_date, fuzzy=True).date()
        self.sleep_time = sleep_time
        self.items = []
        self.stop_pagination = False


    def normalize_date(self, raw):
        try:
            raw = raw.strip()
            if re.match(r"^[A-Za-z]+\s+\d{4}$", raw):
                dt = parser.parse(raw, fuzzy=True)
                return datetime(dt.year, dt.month, 1).date()
            return parser.parse(raw, fuzzy=True).date()
        except:
            return self.target_date

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
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/91.0.4472.124 Safari/537.36"
                )
            )

            page = await context.new_page()
            await page.set_extra_http_headers({
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
            })

            await page.set_content("<meta http-equiv='X-Content-Type-Options' content='nosniff'>")
            await page.goto(url, timeout=60000)
            await asyncio.sleep(self.sleep_time)

            try:
                await page.wait_for_selector("#btnTermsAccept", timeout=5000)
                logger.info("Gateway modal detected — clicking ACCEPT")
                await page.locator("#btnTermsAccept").click()
                await asyncio.sleep(2)
            except Exception as e:
                logger.debug(f"Gateway modal not shown or already accepted: {e}")

            try:
                await page.locator("#onetrust-accept-btn-handler").click(timeout=3000)
                logger.debug("Cookie banner accepted")
            except:
                logger.debug("No cookie banner")

            while True:
                if self.stop_pagination:
                    break

                try:
                    await page.wait_for_selector("eds-card-article", timeout=15000)
                except:
                    break

                cards = page.locator("eds-card-article")
                count = await cards.count()
                logger.info(f"Found {count} article cards on page")

                if count == 0:
                    break

                for i in range(count):
                    try:
                        card = cards.nth(i)

                        date_raw = await card.locator(".card-article__date").text_content()
                        parsed_date = self.normalize_date(date_raw)

                        if parsed_date < self.target_date:
                            logger.info("Older date reached — stop pagination")
                            self.stop_pagination = True
                            break

                        title = await card.locator("eds-title h4").text_content()
                        desc = await card.locator(".card-article__description").text_content()

                        href = await card.locator("a.card-article__inner").get_attribute("href")
                        if not href:
                            continue

                        url_full = href if href.startswith("http") else BASE_URL + href
                        slug = url_full.rstrip("/").split("/")[-1]

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
                            "article_url": url_full
                        })

                    except Exception as e:
                        logger.error(f"Error parsing card #{i}: {e}")

                if self.stop_pagination:
                    break

                next_btn = page.locator("button.ft__btn--pagination.ft__btn >> text=Next")
                if await next_btn.count() > 0 and await next_btn.is_visible():
                    try:
                        await next_btn.click()
                        await asyncio.sleep(self.sleep_time)
                    except:
                        break
                else:
                    break

            await self.scrape_article_pages(context)
            await browser.close()

            logger.info(f"Finished scraping {len(self.items)} total articles")
            return self.items

    async def scrape_article_pages(self, context):
        page = await context.new_page()

        for idx, item in enumerate(self.items, start=1):
            url = item["article_url"]

            if url.endswith(".pdf"):
                item["article_content"] = url
                continue

            try:
                await page.goto(url, timeout=60000)
                await asyncio.sleep(self.sleep_time)

                contents = await page.locator(".articles-promo__glance-content").all_text_contents()
                content = " ".join([c.strip() for c in contents])

                item["article_content"] = content

            except Exception as e:
                logger.error(f"Failed to scrape article #{idx}: {e}")

        await page.close()

async def FTSGFP(target_date):
    try:
        url = "https://www.franklintempleton.com.sg/articles"
        scraper = FTScraperSG(target_date)
        results = await scraper.scrape(url)

        output_path = f"/tmp/{company_site_id}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)

        logger.info(f"Scraped {len(results)} SG articles after {target_date}")
        return 200

    except Exception as e:
        logger.error(f"Error: {e}")
        return 500

if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2026-01-01"
    logger.info(f"Scraping using target date: {target_date}")
    asyncio.run(FTSGFP(target_date))
