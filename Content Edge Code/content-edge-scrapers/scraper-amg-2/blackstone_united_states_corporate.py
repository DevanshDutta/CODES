import asyncio
import json
import logging
import sys
import re
from datetime import datetime
from dateutil import parser
from playwright.async_api import async_playwright


site = "Blackstone Group LP"
section = "Insights"
company_site_id = "am-272"
country = "United States"
role = "Corporate"
BASE_URL = "https://www.blackstone.com"



logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)



class BlackstoneScraperUS:
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

            while True:
                await page.wait_for_selector("article.bx-article-column", timeout=20000)
                cards = await page.locator("article.bx-article-column").all()
                logger.debug(f"DEBUG: Currently Found {len(cards)} articles")

                try:
                    last_text = await cards[-1].locator("p.bx-article-post_date time").text_content()
                    last_date = parser.parse(last_text).date()
                except Exception as e:
                    logger.debug(f"Failed parsing last visible date: {e}")
                    break

                logger.debug(f"Last visible article date: {last_date}")

                if last_date < self.target_date:
                    logger.debug("Last date dropped below target — stopping pagination")
                    break

                load_more = page.locator("button.js-load-more-insight")
                if await load_more.count() > 0:
                    await load_more.click()
                    await asyncio.sleep(self.sleep_time)
                else:
                    break


            cards = await page.locator("article.bx-article-column").all()
            logger.debug(f"DEBUG: Found {len(cards)} articles after pagination")

            for idx, card in enumerate(cards, start=1):
                try:
                    title = await card.locator("h4.bx-article-title a").text_content()
                    href = await card.locator("h4.bx-article-title a").get_attribute("href")
                    tag = await card.locator("p.bx-article-term-title").text_content()
                    date_text = await card.locator("p.bx-article-post_date time").text_content()

                    pub_date = parser.parse(date_text).date()
                    if pub_date < self.target_date:
                        continue

                    url_full = href if href.startswith("http") else BASE_URL + href
                    slug = url_full.rstrip("/").split("/")[-1]

                    logger.debug(f"DEBUG: Article #{idx}: {title[:50]}...")

                    self.items.append({
                        "company_site_id": company_site_id,
                        "company_site_country": country,
                        "company_site_role": role,
                        "article_source": site,
                        "article_section": section,
                        "article_date": str(pub_date),
                        "article_title": title.strip(),
                        "article_description": None,
                        "article_content": None,
                        "article_tags": [tag] if tag else [],
                        "article_slug": slug,
                        "article_url": url_full
                    })

                except Exception as e:
                    logger.warning(f"Error parsing card: {e}")
                    continue


            await self.scrape_article_pages(context)
            await browser.close()

            return self.items

    async def scrape_article_pages(self, context):
        logger.debug("DEBUG: Starting to scrape individual Blackstone articles")

        for item in self.items:
            url = item["article_url"]
            if not url:
                continue

            page = await context.new_page()

            try:
                resp = await page.goto(url, timeout=60000)

                # PDF detection
                ct = resp.headers.get("content-type", "").lower()
                if "pdf" in ct:
                    item["article_content"] = url
                    await page.close()
                    continue

                await asyncio.sleep(self.sleep_time)

                # Extract content
                paragraphs = await page.locator("div.bx-article-content__content p").all_text_contents()
                if paragraphs:
                    item["article_description"] = paragraphs[0].strip()
                    item["article_content"] = " ".join(
                        p.strip() for p in paragraphs if p.strip()
                    )
                else:
                    item["article_description"] = await page.locator("meta[name='description']").get_attribute("content")
                    item["article_content"] = None

                logger.debug(f"Succesfully scraped url:{url}")

            except Exception as e:
                logger.error(f"ERROR: Failed to scrape {url}: {e}")
                item["article_content"] = None

            finally:
                await page.close()


async def BSUSCO(target_date):
    url = "https://www.blackstone.com/insights/"
    scraper = BlackstoneScraperUS(target_date)
    results = await scraper.scrape(url)

    output_path = f"/tmp/{company_site_id}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    logger.info(f"Scraped {len(results)} Blackstone articles after {target_date}")
    return 200


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2026-01-01"
    logger.info(f"Scraping articles using target date:{target_date}")
    asyncio.run(BSUSCO(target_date=target_date))
