import asyncio
import json
import logging
import sys
import re
from dateutil import parser
from playwright.async_api import async_playwright

site = "Vanguard"
section = "Insights"
company_site_id = "am-205"
country = "United States"
role = "Financial Advisor"

# Logging setup
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)


class VanguardScraper:
    def __init__(self, target_date, sleep_time=2):
        self.target_date = parser.parse(target_date, fuzzy=True).date()
        self.sleep_time = sleep_time
        self.items = []

    async def scrape(self, url):
        logger.info(f"Starting scraper for {url}")

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

            seen_urls = set()
            stop = False
            page_count = 1

            while not stop:

                # Wait for article cards
                await page.wait_for_selector("section.article-card", timeout=8000)
                cards = await page.locator("section.article-card").all()

                logger.info(f"Page {page_count}: Found {len(cards)} article cards")

                for card in cards:

                    link_el = card.locator("a.article-card__title-link").first
                    href = await link_el.get_attribute("href")
                    if not href:
                        continue

                    article_url = "https://advisors.vanguard.com" + href

                    if article_url in seen_urls:
                        continue
                    seen_urls.add(article_url)

                    raw_date = await card.locator("p.article-card__date time").first.text_content()

                    try:
                        parsed_date = parser.parse(raw_date.split("|")[0]).date()
                    except:
                        parsed_date = None

                    logger.info(f"Date {parsed_date} → {article_url}")

                    # Stop if article date becomes older than target_date
                    if parsed_date and parsed_date < self.target_date:
                        logger.info(
                            f"Stopping: {parsed_date} < target_date {self.target_date}"
                        )
                        stop = True
                        break

                    title = (await link_el.text_content()).strip()

                    description_el = card.locator("p.article-card__description").first
                    description = (await description_el.text_content()).strip()

                    category_el = card.locator("div.article-card__categories li a.tag").first
                    category = await category_el.text_content() if await category_el.count() > 0 else None

                    content = await self.scrape_article(context, article_url)
                    self.items.append({
                        "company_site_id": company_site_id,
                        "company_site_country": country,
                        "company_site_role": role,
                        "article_source": site,
                        "article_section": section,
                        "article_date": str(parsed_date),
                        "article_title": title,
                        "article_description": description,
                        "article_content": content,
                        "article_tags": [category] if category else [],
                        "article_slug": article_url.rstrip("/").split("/")[-1],
                        "article_url": article_url,
                        })

                if stop:
                    break

                # CLICK NEXT PAGE BUTTON
                next_button = page.locator("button[aria-label='Next page']")

                if not await next_button.count():
                    logger.info("Next button not found → end of pagination.")
                    break

                # If disabled, stop
                disabled = await next_button.get_attribute("disabled")
                if disabled is not None:
                    logger.info("Next button is disabled → stopping.")
                    break

                logger.info(f"Clicking next page button (page {page_count + 1})")
                await next_button.click()

                # Allow content to load
                await asyncio.sleep(self.sleep_time)
                page_count += 1

            await browser.close()
            logger.info(f"Scraping complete — total {len(self.items)} articles collected")
            return self.items

    async def scrape_article(self, context, url):
        """Scrape full article content from detail page."""
        try:
            page = await context.new_page()
            await page.goto(url, timeout=60000)

            await page.wait_for_selector("div.vg-article-content, article", timeout=8000)

            paragraphs = await page.locator(
                "div.vg-article-content p, article p"
            ).all()

            content_list = []
            for p in paragraphs:
                txt = await p.text_content()
                if txt and txt.strip():
                    content_list.append(txt.strip())

            await page.close()
            return "\n".join(content_list)

        except Exception as e:
            logger.error(f"Error scraping article {url}: {e}")
            return None

async def VanguardUSFA(target_date):
    results=[]
    try:
        url = "https://advisors.vanguard.com/insights/all"
        scraper = VanguardScraper(target_date)
        results = await scraper.scrape(url)
        output_path=f"/tmp/{company_site_id}.json"

        with open(output_path,"w", encoding="utf-8") as f:
            json.dump(results, f , indent=4 , ensure_ascii=False)
        logger.info(f"Scraped {len(results)} articles")
        return 200
    except Exception as error:
        logger.error(f"Error{error}")
        return 500


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-11-01"
    logger.info(f"Scraping U.S. articles using target date: {target_date}")
    asyncio.run(VanguardUSFA(target_date))

