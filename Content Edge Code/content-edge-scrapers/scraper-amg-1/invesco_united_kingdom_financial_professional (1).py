import asyncio
import json
import logging
import sys
from datetime import datetime
from dateutil import parser

from playwright.async_api import async_playwright


site = "Invesco"
section = "Insights"
company_site_id = "am-302"
country = "United Kingdom"
role = "Financial Professional"
BASE_URL = "https://www.invesco.com"

logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
        force=True,
)
logger=logging.getLogger(company_site_id)

class InvescoScraper:
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

                await page.get_by_role("button", name="Financial Professional").click()
                await page.get_by_role("button", name="Confirm").click()
                logger.info("Clicked Financial Professional")
            except Exception as e:
                logger.info(f"Error selecting Financial Professional:{e}")

            cards= await page.locator(".content-card.article-event-card").all()
            print(f"Found {len(cards)}")

            for card in cards:

                title= await card.locator(".content-card__headline").text_content()
                date_text=await card.locator(".content-card__date").text_content()
                date=parser.parse(date_text, fuzzy= True).date()
                if date < self.target_date:
                    logger.info("Older article found")
                    continue
                description = await card.locator("p.content-card__description").inner_text()
                tags= await card.locator(".content-card__eyebrow").inner_text()
                href= await card.locator("a.content-card__image-link").get_attribute("href")
                article_url = (BASE_URL + href) if href.startswith("/") else href 
                slug = href.rstrip("/").split("/")[-1] if href else None

                

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
                    "article_url": article_url
                })
            await self.scrape_article_pages(context)
            return self.items



    async def scrape_article_pages(self,context):
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
async def InvescoUKFA(target_date):
    url="https://www.invesco.com/uk/en/insights.html"
    scraper=InvescoScraper(target_date)
    results= await scraper.scrape(url)
    print(results)
    output_path =f"/tmp/{company_site_id}.json"

    with open(output_path,"w",encoding="utf-8") as f:
        json.dump(results,f, indent=4, ensure_ascii=False)

    logger.info(f"Scraped {len(results)} articles after {target_date}")
    return 200

if __name__=="__main__":
    target_date=sys.argv[1] if len(sys.argv) >1 else "2025-10-01"
    logger.info(f"Scraping articles using target date :{target_date}")
    asyncio.run(InvescoUKFA(target_date))

