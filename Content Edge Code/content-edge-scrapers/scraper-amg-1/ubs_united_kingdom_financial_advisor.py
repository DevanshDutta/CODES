import asyncio
from asyncio.timeouts import timeout
import json
import logging
import sys
from datetime import datetime, time
from typing_extensions import type_repr
from dateutil import parser
import re 
from playwright.async_api import async_playwright


site = "UBS Asset Management"
section = "Insights"
company_site_id = "am-260"
country = "United Kingdom"
role = "Financial Advisor"
BASE_URL="https://www.ubs.com"

def extract_date(text):
    pattern = r"\b([A-Za-z]{3,9}\s+\d{1,2}\s*,?\s*\d{4})\b"
    match = re.search(pattern, text)
    return match.group(1) if match else None

logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
        force=True,
)
logger=logging.getLogger(company_site_id)

class UBSScraper:
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
            try:
                logger.info("Selecting UK + Financial Advisor")
                await page.get_by_role("button", name="Agree to all").click()
                await page.get_by_text("Financial intermediaries").click()
                await asyncio.sleep(2)
                await page.keyboard.press("End")
                await asyncio.sleep(1)
                await page.get_by_role("button", name="Accept and continue").click()

            except Exception as e:
                logger.info(f"Error selecting: {e}")


            await asyncio.sleep(5)
            while True: #loop for loading insights 
                cards= await page.locator("li.sdactivitystream__listItem").all()
                last_card=cards[-1]
                raw_date= await last_card.locator(".sdactivitystreamtile__date").inner_text()
                last_date= parser.parse(raw_date, fuzzy=True).date()
                if last_date >= self.target_date:
                    await page.get_by_role("button", name="Show more").click()
                    await asyncio.sleep(5)
                    logger.info("Clicking Show more")
                else:
                    logger.info("Last date older than Target date")
                    break 

            await page.wait_for_selector("li.sdactivitystream__listItem", timeout=15000)
            cards = await page.locator("li.sdactivitystream__listItem").all()
            logger.debug(f"DEBUG: Found {len(cards)} articles")
            for idx, card in enumerate(cards, start=1):
                try:
                    raw_date = await card.locator(".sdactivitystreamtile__date").inner_text()
                    date = parser.parse(raw_date, fuzzy=True).date()
                    if date < self.target_date:
                        continue
                except Exception:
                    date = ""
                

                try:
                    tag_text = await card.locator(".sdactivitystreamtile__leadTag").text_content()
                    tag = [x.strip() for x in tag_text.split("|")] if tag_text else []
                except Exception:
                    tag = []

                title = await card.locator(".sdactivitystreamtile__linkHl").inner_text() or ""
                description = ""
                href = await card.locator(".sdactivitystreamtile__linkHl").get_attribute("href")
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

                try:
                    desc_p = await page.locator(
                        "div.textimage__richtext.richtext__base p"
                    ).first.text_content()
                    item["article_description"] = desc_p.strip() if desc_p else ""
                except Exception:
                    item["article_description"] = ""


                paragraphs = await page.locator("div.container__content").all_inner_texts()
                full_text = " ".join(p.strip() for p in paragraphs if p.strip())
                item["article_content"] = full_text
                logger.debug(f"Succesfully scraped url:{url}")

            except Exception as e:
                logger.error(f"ERROR: Failed to scrape {url}: {e}")
                item["article_content"] = None
            finally:
                await page.close()


async def UBSUKFA(target_date):
    url = "https://www.ubs.com/uk/en/assetmanagement/insights.html"
    scraper = UBSScraper(target_date)
    results = await scraper.scrape(url)
    output_path=f"/tmp/{company_site_id}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    logger.info(f" Scraped {len(results)} articles after {target_date}")
    return 200


if __name__ == "__main__":

    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-11-01"
    logger.info(f"Scraping articles using target date:{ target_date}")
    asyncio.run(
        UBSUKFA(
            target_date=target_date,
       ))
