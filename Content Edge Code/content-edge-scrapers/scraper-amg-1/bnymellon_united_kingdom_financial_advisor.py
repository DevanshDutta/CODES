import asyncio
import json
import logging
import sys
from datetime import datetime
from dateutil import parser

from playwright.async_api import async_playwright


site = "BNY Mellon Investment Management"
section = "Insights"
company_site_id = "am-256"
country = "United Kingdom"
role = "Financial Advisor"
BASE_URL = "https://www.bnymellonim.com"

logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
        force=True,
)
logger=logging.getLogger(company_site_id)

class BNYMIMScraper:
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
                        ])
            context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                    )
            page = await context.new_page()

            # Set headers
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

            # Step 1: Load page
            await page.goto(url, timeout=60000)
            await asyncio.sleep(self.sleep_time)

            # Step 2: Handle disclaimer
            try:
                await page.locator("#selector-form-confirm").click(timeout=5000)
                await page.locator(".disclaimer-button").click(timeout=5000)
                logger.debug("DEBUG: Disclaimer accepted")
            except Exception as e:
                logger.warning("WARN: Disclaimer step skipped or already accepted:", e)

            # Step 3: Close cookie banner
            try:
                await page.locator("#onetrust-close-btn-container").click(timeout=5000)
                logger.debug("DEBUG: Cookie banner closed")
            except Exception as e:
                logger.warning(f"WARN: No cookie banner found:{e}" )

            # Step 4: Wait for articles
            while True:
                await page.wait_for_selector(".grid-item", timeout=15000)
                cards = await page.locator(".grid-item").all()
                logger.debug(f"DEBUG: Currently Found {len(cards)} articles")

                try:
                    last_card=cards[-1]
                    date_text=await last_card.locator(".small").text_content()
                    last_date=parser.parse(date_text.split("|")[0]).date()
                    logger.debug(f"Last visible article date: {last_date}")
                except Exception as e:
                    logger.error(f"Couldn't parse last article date :{e}")
                    break

                if last_date>= self.target_date:
                    try:
                        load_more = page.locator("button:has-text('Load More')")

                        # Check if button exists 
                        if await load_more.count() > 0:
                            logger.info("Clicking 'Load More' directly")
                            await load_more.first.click()
                            await asyncio.sleep(self.sleep_time)
                        else:
                            logger.info("Button not found — scrolling to bottom to render it...")
                            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            await asyncio.sleep(2)

                            if await load_more.count() > 0:
                                logger.info("Button appeared after scroll")
                                await load_more.first.click()
                                await asyncio.sleep(self.sleep_time)
                            else:
                                logger.info("Still no 'Load More' button — stopping.")
                                break

                    except Exception as e:
                        logger.warning(f"Load more failed: {e}")
                        break
                else:
                    logger.debug("Last Date exceeded ")
                    break

            await page.wait_for_selector(".grid-item", timeout=15000)
            cards = await page.locator(".grid-item").all()
            logger.debug(f"DEBUG: Found {len(cards)} articles")
            for idx, card in enumerate(cards, start=1):
                try:
                    date_text = await card.locator(".small").text_content()
                    date = parser.parse(date_text.split("|")[0]).date()
                except Exception:
                    date = None

                try:
                    tag_text = await card.locator(".eyebrow").text_content()
                    tag = [x.strip() for x in tag_text.split("|")] if tag_text else []
                except Exception:
                    tag = []

                title = await card.locator("h3").text_content() or ""
                description = await card.locator(".grid-desc").text_content() or ""
                href = await card.locator("a").first.get_attribute("href")
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
                    "article_tags": tag,
                    "article_slug": slug,
                    "article_url": href if href.startswith("http") else BASE_URL+href
                })

            # Step 5: Visit each article
            await self.scrape_article_pages(context)

            await browser.close()
            return [i for i in self.items if i["article_date"] and parser.parse(i["article_date"]).date() >= self.target_date]

    async def scrape_article_pages(self, context):
        logger.debug("DEBUG: Starting to scrape individual articles")
        for item in self.items:
            url = item["article_url"]
            #logger.debug(f"url:{url}")
            if not url:
                continue

            # Skip PDFs
            if url.endswith(".pdf"):
                item["article_content"] = url
                continue

            page = await context.new_page()
            try:
                await page.goto(url, timeout=60000)
                await asyncio.sleep(self.sleep_time)

                # Extract full content text
                paragraphs = await page.locator("p").all_text_contents()
                full_text = " ".join(p.strip() for p in paragraphs if p.strip())
                item["article_content"] = full_text
                logger.debug(f"Succesfully scraped url:{url}")

            except Exception as e:
                logger.error(f"ERROR: Failed to scrape {url}: {e}")
                item["article_content"] = None
            finally:
                await page.close()


async def BNYMIMUKFA(target_date):
    url = "https://www.bnymellonim.com/uk/en/adviser/news-and-insights/all-insights.html"
    scraper = BNYMIMScraper(target_date)
    results = await scraper.scrape(url)
    output_path=f"/tmp/{company_site_id}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    logger.info(f" Scraped {len(results)} articles after {target_date}")
    return 200


if __name__ == "__main__":

    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-09-01"
    logger.info(f"Scraping articles using target date:{ target_date}")
    asyncio.run(
        BNYMIMUKFA(
            target_date=target_date,
        ))


