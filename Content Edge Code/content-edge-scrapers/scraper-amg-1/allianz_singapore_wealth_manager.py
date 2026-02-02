import asyncio
from asyncio.timeouts import timeout
import json
import logging
import sys
from datetime import date, datetime
from dateutil import parser

from playwright.async_api import async_playwright


site = "Allianz Global Investors"
section = "Insights"
company_site_id = "am-324"
country = "United Kingdom"
role = "Wealth Manager"

BASE_URL = "https://uk.allianzgi.com"

logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
        force=True,
)
logger=logging.getLogger(company_site_id)

class AllianzScraper:
    def __init__(self, target_date, sleep_time=5):
        self.target_date = parser.parse(target_date, fuzzy=True).date()
        self.sleep_time = sleep_time
        self.items = []

    async def scrape(self, url):
        self.items=[]
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


            await page.set_content("<meta http-equiv='X-Content-Type-Options' content='nosniff'>")

            # Step 1: Load page
            await page.goto(url, timeout=60000)
            await asyncio.sleep(self.sleep_time)

            # Step 2: Handle disclaimer
            try:
                await page.get_by_role("button", name="Accept All Cookies").click()
                await page.locator("label").filter(has_text="I have read and understood").scroll_into_view_if_needed()
                await page.locator("label").filter(has_text="I have read and understood").click()
                await page.get_by_role("button", name="OK", exact=True).click()

            except Exception as e:
                logger.info(f"Error selecting Financial Professional:{e}")

            while True:
                cards= await page.locator(".c-agi-tile").all()
                last_card=cards[-1]
                try:
                    date_text= await last_card.locator("span.u-margin-left-3xs").inner_text(timeout=30)
                    date_text= date_text.replace("|","").strip()
                except:
                    date_text= await last_card.locator(".c-agi-tile__author").inner_text()
                last_date= parser.parse(date_text, fuzzy=True).date()
                if last_date >= self.target_date:
                    await page.get_by_label("LOAD MORE").click()
                    await asyncio.sleep(4)
                else:
                    break
            cards= await page.locator(".c-agi-teaser").all()

            for card in cards[1:]:

                title= await card.locator(".c-heading").text_content()
                try:
                    date_raw = await card.locator("span.u-margin-left-3xs").inner_text(timeout=30)
                    date_cleaned = date_raw.replace("|", "").strip()
                except:
                    date_cleaned=await card.locator(".c-agi-tile__footer").inner_text()
                    
                date= parser.parse(date_cleaned, fuzzy=True).date()
                if date < self.target_date:
                    continue
                
                description = await card.locator(".c-agi-tile__text").inner_text()
                try:
                    tags= await card.locator(".c-agi-tile__tags").inner_text(timeout=30) 
                except :
                    tags=""
                href= await card.locator("a.c-link--block").get_attribute("href")
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
            return [i for i in self.items if i["article_date"] and parser.parse(i["article_date"]).date() >= self.target_date]



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
                paragraphs = await page.locator("section.l-grid__row").all_text_contents()
                full_text = " ".join(p.strip() for p in paragraphs if p.strip())
                item["article_content"] = full_text
                logger.debug(f"Succesfully scraped url:{url}")

            except Exception as e:
                logger.error(f"ERROR: Failed to scrape {url}: {e}")
                item["article_content"] = None
            finally:
                await page.close()
async def AllianzSGWM(target_date):
    urls = [
            "https://sg.allianzgi.com/en-sg/financial-advisor/insights/outlook-and-commentary",
            ]

    scraper = AllianzScraper(target_date)  

    all_items = []

    for url in urls:
        items = await scraper.scrape(url)
        all_items.extend(items)  # merge results

    output_path = f"/tmp/{company_site_id}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_items, f, indent=4, ensure_ascii=False)

    logger.info(f"Scraped total {len(all_items)} articles after {target_date}")
    return 200

if __name__=="__main__":
    target_date=sys.argv[1] if len(sys.argv) >1 else "2025-10-01"
    logger.info(f"Scraping articles using target date :{target_date}")
    asyncio.run(AllianzSGWM(target_date))

