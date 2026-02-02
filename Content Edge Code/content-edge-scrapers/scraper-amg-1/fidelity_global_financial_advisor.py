import asyncio
from asyncio.timeouts import timeout
import json
import logging
import sys
from datetime import datetime
from dateutil import parser

from playwright.async_api import async_playwright


site ="Fidelity International"
section = "Insights"
company_site_id = "am-274"
country = "Global"
role = "Financial Advisor"
BASE_URL="https://institutional.fidelity.com/"

logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
        force=True,
)
logger=logging.getLogger(company_site_id)

class FidelityScraper:
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

            while True:

                cards=await page.locator(".fxd-vCard").all()
                last_card=cards[-1]
                href=await last_card.locator("a").get_attribute("href")
                last_url= href if href.startswith("http") else BASE_URL+href 
                last_page= await context.new_page()
                
                await last_page.goto(last_url,timeout=60000)
                await asyncio.sleep(self.sleep_time)

                try:
                    date_text = (await last_page.locator(".fxd-byline__date").inner_text()).split(":")[-1].strip()
                except:
                    date_text=str(self.target_date)
                await last_page.close()
                last_date= parser.parse(date_text,fuzzy=True).date()
                if last_date < self.target_date:
                    break
                else:
                    await page.get_by_text("Load More", exact=True).click()
                    await asyncio.sleep(2)
                    logger.info("Loading More")


            cards = await page.locator(".fxd-vCard").all()
            for card in cards:
                tags = await card.locator(".fxd-vCard__eyebrow").inner_text()
                title= await card.locator(".fxd-vCard__title").inner_text()
                href= await card.locator("a").get_attribute("href")
                description= await card.locator(".fxd-vCard__long-description").text_content()
                article_url= href if href.startswith("http") else BASE_URL+href
                
                slug = href.rstrip("/").split("/")[-1] if href else None
                
                self.items.append({
                    "company_site_id": company_site_id,
                    "company_site_country": country,
                    "company_site_role": role,
                    "article_source": site,
                    "article_section": section,
                    "article_date": None,
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

                try:
                    date_text = (await page.locator("div.fxd-byline__date").inner_text()).split(":")[-1].strip()
                    date = parser.parse(date_text, fuzzy=True).date()
                    if date<self.target_date:
                        continue
                except: 
                    date_text=""
                item["article_date"]=date_text

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
async def FidelityGlobalFA(target_date):
    url="https://institutional.fidelity.com/advisors/insights/topics"
    scraper=FidelityScraper(target_date)
    results= await scraper.scrape(url)
    output_path =f"/tmp/{company_site_id}.json"

    with open(output_path,"w",encoding="utf-8") as f:
        json.dump(results,f, indent=4, ensure_ascii=False)

    logger.info(f"Scraped {len(results)} articles after {target_date}")
    return 200

if __name__=="__main__":
    target_date=sys.argv[1] if len(sys.argv) >1 else "2025-12-01"
    logger.info(f"Scraping articles using target date :{target_date}")
    asyncio.run(FidelityGlobalFA(target_date))