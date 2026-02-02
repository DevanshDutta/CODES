import asyncio
import json
import logging
import sys
import re
from datetime import datetime
from dateutil import parser
from playwright.async_api import async_playwright

# --- Site metadata ---
site = "Federated Hermes"
section = "Insights"
company_site_id = "am-218"
country = "United States"
role = "Financial Advisor"
BASE_URL = "https://www.federatedhermes.com"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)


class FederatedHermesScraper:
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

            await page.goto(url, timeout=120000)
            await asyncio.sleep(self.sleep_time)

            logger.info("Starting infinite scroll…")

            stop_scroll = False
            last_height = None

            while True:
  
                cards = await page.locator("section.w3-card.insight").all()
                logger.info(f"Found {len(cards)} cards so far…")

                for card in cards:
                    try:
         
                        link = await card.locator("a[href*='/insights']").get_attribute("href")
                        if not link:
                            continue

                        url_full = BASE_URL + link
                        slug = link.rstrip("/").split("/")[-1]

                        if slug in self.seen_slugs:
                            continue
                        self.seen_slugs.add(slug)

                        try:
                            title = await card.locator("span.content-heading-3").text_content()
                            title = title.strip() if title else None
                        except:
                            title = None

                        desc = None
                        try:
       
                            if await card.locator("p[itemprop='description']").count():
                                raw = (await card.locator("p[itemprop='description']").text_content()) or ""
                                raw = raw.strip()
                                if raw:
                                    desc = raw

                         
                            if not desc:
                                p_texts = await card.locator("p").all_text_contents()
                                for p in p_texts:
                                    txt = (p or "").strip()
                                    if not txt:
                                        continue
                               
                                    if re.search(r'\d{1,2}\s?minute', txt, re.I):
                                        continue
                                    if re.match(r'^\d{1,2}[-/]\d{1,2}[-/]\d{2,4}$', txt):
                                        continue
                            
                                    if len(txt) < 10:
                                        continue
                                    desc = txt
                                    break
                        except Exception:
                            desc = None

                   
                        try:
                            date_text = await card.locator("time[itemprop='datePublished']").get_attribute("datetime")
                            parsed_listing_date = parser.parse(date_text, fuzzy=True).date()
                        except:
                            parsed_listing_date = self.target_date

                        if parsed_listing_date < self.target_date:
                            stop_scroll = True
                            break

                        tags = []
                        try:
                            tag_texts = await card.locator(".w3-tag-container .w3-tag").all_text_contents()
                            tags = [t.strip() for t in tag_texts if t and t.strip()]
                        except:
                            pass

                        self.items.append({
                            "company_site_id": company_site_id,
                            "company_site_country": country,
                            "company_site_role": role,
                            "article_source": site,
                            "article_section": section,
                            "article_date": str(parsed_listing_date),
                            "article_title": title,
                            "article_description": desc,
                            "article_content": None,
                            "article_tags": tags,
                            "article_slug": slug,
                            "article_url": url_full
                        })

                    except Exception:
          
                        continue

                if stop_scroll:
                    logger.info("Hit target-date → stopping infinite scroll.")
                    break

                new_height = await page.evaluate("document.body.scrollHeight")
                if last_height == new_height:
                    logger.info("No more scrollable content → stopping.")
                    break

                last_height = new_height
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                await asyncio.sleep(self.sleep_time)

            await self.scrape_article_pages(context)
            await browser.close()

            self.items = [x for x in self.items if x["article_content"]]
            return self.items

    async def scrape_article_pages(self, context):
        page = await context.new_page()

        for idx, item in enumerate(self.items, start=1):
            url = item["article_url"]

            if url.endswith(".pdf"):
                item["article_content"] = url
                continue

            try:
                logger.info(f"Scraping article #{idx}: {url}")
                await page.goto(url, timeout=120000)
                await asyncio.sleep(self.sleep_time)

                try:
                    content_blocks = await page.locator("div.teamsite.html").all_text_contents()
                    content = " ".join([c.strip() for c in content_blocks if c.strip()])
                except:
                    content = ""

                item["article_content"] = content

            except Exception as e:
                logger.error(f"Failed scraping article #{idx}: {e}")

        await page.close()


async def FHUSFA(target_date):
    results = []
    try:
        url = "https://www.federatedhermes.com/us/insights.do"
        scraper = FederatedHermesScraper(target_date)
        results = await scraper.scrape(url)

        output_path = f"/tmp/{company_site_id}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)

        logger.info(f"Scraped {len(results)} Federated Hermes articles after {target_date}")
        return 200

    except Exception as error:
        logger.error(f"Error: {error}")
        return 500


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-12-01"
    logger.info(f"Scraping Federated Hermes using target date: {target_date}")
    asyncio.run(FHUSFA(target_date=target_date))
