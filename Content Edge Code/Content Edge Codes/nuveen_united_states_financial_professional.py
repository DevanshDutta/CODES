import asyncio
import json
import logging
import sys
import os
from datetime import datetime
from dateutil import parser
from playwright.async_api import async_playwright

# --- Site metadata ---
site = "Nuveen Investments"
section = "Insights"
company_site_id = "am-201"
country = "United States"
role = "Financial Professional"
BASE_URL = "https://www.nuveen.com"

# --- Logging setup ---#Update
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)

# --- Scraper class ---
class NuveenScraperUS:
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
            await page.goto(url, timeout=60000)
            await asyncio.sleep(self.sleep_time)

            # Accept cookies if present
            try:
                await page.locator("button#onetrust-accept-btn-handler").click(timeout=5000)
                logger.debug("Cookie accept clicked")
                await asyncio.sleep(1)
            except:
                pass

            # Determine total pages 
            total_pages = 1
            try:
                await page.wait_for_selector(".nuv-pagination span", timeout=5000)
                page_buttons = await page.locator(".nuv-pagination span").all()
                pages_nums = []
                for btn in page_buttons:
                    txt = await btn.text_content()
                    # Filter for actual digit spans (e.g., '1', '2', '39', '40') and ignore '...'
                    if txt and txt.strip().isdigit():
                        pages_nums.append(int(txt.strip()))
                if pages_nums:
                    total_pages = max(pages_nums)
                logger.info(f"Detected {total_pages} pagination pages")
            except Exception as e:
                logger.warning(f"Pagination not detected, defaulting to 1 page: {e}")

            # --- Loop through pages ---
            stop_scraping = False
            for p_no in range(1, total_pages + 1):
                logger.info(f"Scraping page {p_no}...")

                if p_no > 1:
                    try:
                        page_selector = f'.nuv-pagination li:not(.active) span:text("{p_no}")'

                        # Wait for the selector to be available (it may take time to appear after the previous page load)
                        await page.wait_for_selector(page_selector, timeout=10000)
                        
                        # Click the specific element
                        await page.click(page_selector)
                        logger.info(f"Successfully clicked pagination button for page {p_no}")
                        
                        # Wait for the new results to load
                        await page.wait_for_selector(".nuv-search-results__list-item", timeout=15000)
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await asyncio.sleep(self.sleep_time) # Wait for page stability

                    except Exception as e:
                        logger.warning(f"Failed to click or load page {p_no}: {e}. Stopping pagination.")
                        # If navigation fails, break the page loop to avoid infinite attempts.
                        break 
                try:
                    await page.evaluate("window.scrollTo(0, 0)") 
                    await page.wait_for_selector(".nuv-search-results__list-item", timeout=10000)
                    cards = await page.locator(".nuv-search-results__list-item").all()
                    logger.info(f"Page {p_no}: Found {len(cards)} article cards")
                except Exception as e:
                    logger.warning(f"No article cards found on page {p_no} after load: {e}")
                    continue

                for idx, card in enumerate(cards, start=1):
                    try:
                        title_el = card.locator(".nuv-search-results__title a")
                        title = await title_el.text_content()
                        href = await title_el.get_attribute("href")
                        if not href:
                            continue
                        url_full = href if href.startswith("http") else BASE_URL + href
                        slug = url_full.rstrip("/").split("/")[-1]

                        # Use the last element with class .nuv-search-results__description for the date
                        date_text = await card.locator(".nuv-search-results__description").nth(-1).text_content()
                        parsed_date = parser.parse(date_text, fuzzy=True).date()

                        if parsed_date < self.target_date:
                            stop_scraping = True
                            logger.info(f"STOP: Found older article {parsed_date} < {self.target_date}")
                            break

                        self.items.append({
                            "company_site_id": company_site_id,
                            "company_site_country": country,
                            "company_site_role": role,
                            "article_source": site,
                            "article_section": section,
                            "article_date": str(parsed_date),
                            "article_title": title,
                            "article_description": None,
                            "article_content": None,
                            "article_tags": [],
                            "article_slug": slug,
                            "article_url": url_full
                        })
                    except Exception as e:
                        logger.error(f"Error parsing card on page {p_no} #{idx}: {e}")
                        continue

                if stop_scraping:
                    break

            # --- Scrape individual article pages ---
            await self.scrape_article_pages(context)

            await browser.close()
            logger.info(f"Finished scraping {len(self.items)} total articles")
            # Filter out articles that failed to get content
            self.items = [item for item in self.items if item["article_content"]] 
            return self.items

    async def scrape_article_pages(self, context):
        logger.debug("Starting to scrape individual article pages...")

        page = await context.new_page()
        for idx, item in enumerate(self.items, start=1):
            url = item["article_url"]
            if not url:
                continue

            try:
                # Add a conditional to skip if content is already present (e.g., if you run the scraper multiple times)
                if item.get("article_content") and item["article_content"] is not None:
                    continue
                    
                await page.goto(url, timeout=60000)
                await asyncio.sleep(self.sleep_time)

                # Extract description
                try:
                    desc_locator = page.locator('meta[name="description"]')
                    # Check if the locator exists before trying to get attribute
                    if await desc_locator.count() > 0:
                        desc = await desc_locator.get_attribute("content")
                        item["article_description"] = desc
                except:
                    item["article_description"] = None

                # Extract tags
                try:
                    tags = await page.locator(".nuv-header-article__tout").all_text_contents()
                    item["article_tags"] = [t.strip() for t in tags if t.strip()]
                except:
                    item["article_tags"] = []

                # Extract main content
                content_text = None
                try:
                    sel_candidates = [".nuv-article-content--center", "article", "main"]
                    texts = []
                    for sel in sel_candidates:
                        # Use a stricter locator to avoid capturing header/footer content outside the main article body
                        locator = page.locator(sel)
                        if await locator.count() > 0:
                             parts = await locator.all_text_contents()
                             texts.extend([p.strip() for p in parts if p.strip()])
                    
                    # Remove duplicates and join
                    content_text = " ".join(texts).strip()
                    # Simple cleanup to remove excess whitespace
                    content_text = ' '.join(content_text.split())
                except:
                    content_text = None
                
                item["article_content"] = content_text

                # Attempt to get exact article date from page
                try:
                    time_el = page.locator("time").first
                    if await time_el.count() > 0:
                        time_attr = await time_el.get_attribute("datetime")
                        if time_attr:
                            parsed_date = parser.parse(time_attr, fuzzy=True).date()
                            item["article_date"] = str(parsed_date)
                except:
                    pass

            except Exception as e:
                logger.error(f"Failed to scrape article #{idx} ({url}): {e}")

        await page.close()


async def NuveenUSFA(target_date):
    try:
        url = "https://www.nuveen.com/en-us/insights?type=us"
        scraper = NuveenScraperUS(target_date)
        results = await scraper.scrape(url)
        output_path = f"/tmp/{company_site_id}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)
        logger.info(f"Scraped {len(results)} U.S. articles after {target_date}")
        logger.info(f"Saved JSON at: {output_path}")
        return 200
    except Exception as error:
        logger.error(f"Error: {error}")
        return 500
    
if __name__ == "__main__":
    # If no argument is provided, use a reasonable default date
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-12-01" 
    logger.info(f"Scraping Nuveen U.S. articles using target date: {target_date}")
    asyncio.run(NuveenUSFA(target_date=target_date))