import asyncio
import json
import logging
import sys
import re
from datetime import datetime
from typing import Awaitable
from dateutil import parser
from playwright.async_api import async_playwright

# --- Site metadata ---
site = "Capital Group"    
section = "Insights"
country = "United States"
role = "Financial Professional"
company_site_id = "am-223"

# --- Logging setup ---
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)

class CapitalScaper:
    def __init__(self, target_date, sleep_time=5):
        self.target_date = parser.parse(target_date, fuzzy=True).date()
        self.sleep_time = sleep_time
        self.items = []

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
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
            page = await context.new_page()

                # Step 1: Load page
            await page.goto(url, timeout=60000)
            await asyncio.sleep(10)
            
            while True:
                cards= await page.locator(".search-card").all()
                for card in cards:

                    title =await card.locator("atomic-result-text[field='title']").text_content()
                    tag = await card.locator("atomic-result-text.category-text").text_content()
                    href= await card.locator("a").get_attribute("href")
                    slug = href.rstrip("/").split("/")[-1] if href else None

                    self.items.append({
                    "company_site_id": company_site_id,
                    "company_site_country": country,
                    "company_site_role": role,
                    "article_source": site,
                    "article_section": section,
                    "article_date": "",
                    "article_title": title,
                    "article_description": "",
                    "article_content": None,
                    "article_tags": tag,
                    "article_slug": slug,
                    "article_url": href 
                    })
                await self.scrape_article_pages(context)
                if parser.parse(self.items[-1]["article_date"]).date() >= self.target_date :
                    logger.info("Clicking Next Button")
                    await page.get_by_role("button", name="Next").click()
                    await asyncio.sleep(self.sleep_time)
                    continue
                else:
                    logger.info("No Pagination needed ")
                    break

            await browser.close()
            return [i for i in self.items if i["article_date"] and parser.parse(i["article_date"]).date()>= self.target_date]



    async def scrape_article_pages(self,context):
        logger.debug("DEBUG: Starting to scrape individual articles")
        for item in self.items:
            #Add article date from url locator(cmp-articleDate)
            url = item["article_url"]
            #logger.debug(f"url:{url}")

            if not url or item["article_content"]:
                continue

            # Skip PDFs
            if url.endswith(".pdf"):
                item["article_content"] = url
                continue

            page = await context.new_page()
            try:
                await page.goto(url, timeout=60000)
                await asyncio.sleep(self.sleep_time)
                
                date=await page.locator(".cmp-articleDate").text_content()
                article_date=parser.parse(date, fuzzy=True).date()
                item["article_date"]=article_date
                if article_date <self.target_date:
                    item["article_date"]=str(article_date)
                    logger.info("Skippping : article older than target date")
                    continue

                # Extract full content text
                date_text= await page.locator(".datedisplay").text_content()
            
                date= parser.parse(date_text, fuzzy= True).date()
                if date < self.target_date:
                    continue
                paragraphs = await page.locator(".text p").all_text_contents()
                full_text = " ".join(p.strip() for p in paragraphs if p.strip())
                item["article_content"] = full_text
                item["article_date"]= str(date)
                logger.debug(f"Succesfully scraped url:{url}")

            except Exception as e:
                logger.error(f"ERROR: Failed to scrape {url}: {e}")
                item["article_content"] = None
            finally:
                await page.close()

# Final Scraper 
async def CapitalUSFP(target_date):
    url="https://www.capitalgroup.com/advisor/insights.html"
    scraper =CapitalScaper(target_date)
    results = await scraper.scrape(url)
    output_path=f"/tmp/{company_site_id}.json"

    with open(output_path ,"w", encoding="utf-8") as f:
        json.dump(results ,f , indent=4 , ensure_ascii=False)
    logger.info(f"Scraped {len(results)} articles after {target_date}")

    return 200
    

if __name__=="__main__":
    target_date =sys.argv[1] if len(sys.argv) > 1 else "2025-09-01"
    logger.info(f"Scraping articles using target date:{target_date}")
    asyncio.run(CapitalUSFP(target_date))



