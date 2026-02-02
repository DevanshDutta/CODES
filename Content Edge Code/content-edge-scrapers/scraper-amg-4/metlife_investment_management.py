import asyncio
import json
import logging
import sys
from datetime import datetime
from dateutil import parser

from playwright.async_api import async_playwright

site = "MetLife Investment Management"
section = "Insights"
company_site_id = "am-400"
country = "Global"
role = "Corporate"
BASE_URL = "https://investments.metlife.com"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)


class MetLifeScraper:
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
                ]
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
            try:
                accept_btn = page.locator("button.overlay-accept-button")
                if await accept_btn.count() > 0:
                    logger.info("Clicking ACCEPT overlay button")
                await accept_btn.first.click(timeout=3000)
                await asyncio.sleep(1)
            except Exception as e:
                logger.debug(f"No ACCEPT overlay found: {e}")

            # Accept cookies
            try:
                await page.locator("#onetrust-accept-btn-handler").click(timeout=3000)
            except:
                pass

            while True:
                await page.wait_for_selector("div.article-list-item", timeout=15000)
                cards = await page.locator("div.article-list-item").all()

                logger.debug(f"DEBUG: Currently found {len(cards)} articles")

                try:
                    last_card = cards[-1]
                    last_date_text = await last_card.locator(
                        ".article-list-item-publishedDate"
                    ).text_content()
                    last_date = parser.parse(last_date_text.strip()).date()
                    logger.debug(f"DEBUG: Last visible article date: {last_date}")
                except Exception as e:
                    logger.warning(f"Could not parse last article date: {e}")
                    break

                if last_date >= self.target_date:
                    try:
                        load_more = page.locator("span.show-more-button")
                        if await load_more.count() > 0:
                            logger.info("Clicking Show More")
                            await load_more.first.scroll_into_view_if_needed()
                            await load_more.first.click()
                            await asyncio.sleep(self.sleep_time)
                        else:
                            logger.info("No Show More button found")
                            break
                    except Exception as e:
                        logger.warning(f"Show More click failed: {e}")
                        break
                else:
                    logger.info("Last article older than target date — stopping pagination")
                    break

            await page.wait_for_selector("div.article-list-item", timeout=15000)
            cards = await page.locator("div.article-list-item").all()

            for card in cards:
                try:
                    title = (await card.locator(
                        ".article-list-item-headline"
                    ).text_content()).strip()
                except:
                    title = None

                try:
                    date_text = (await card.locator(
                        ".article-list-item-publishedDate"
                    ).text_content()).strip()
                    date = parser.parse(date_text).date()
                except:
                    continue

                if date < self.target_date:
                    continue

                try:
                    href = await card.locator("a").first.get_attribute("href")
                    article_url = BASE_URL + href
                    slug = href.rstrip("/").split("/")[-1]
                except:
                    continue

                self.items.append({
                    "company_site_id": company_site_id,
                    "company_site_country": country,
                    "company_site_role": role,
                    "article_source": site,
                    "article_section": section,
                    "article_date": str(date),
                    "article_title": title,
                    "article_description": None,
                    "article_content": None,
                    "article_tags": [],
                    "article_slug": slug,
                    "article_url": article_url
                })

            await self.scrape_article_pages(context)
            await browser.close()

            return self.items

    async def scrape_article_pages(self, context):
        for item in self.items:
            page = await context.new_page()
            try:
                await page.goto(item["article_url"], timeout=60000)
                await asyncio.sleep(self.sleep_time)

                # DESCRIPTION
                try:
                    item["article_description"] = await page.locator(
                        "meta[name='description']"
                    ).get_attribute("content")
                except:
                    item["article_description"] = None

                # CONTENT
                paragraphs = await page.locator(
                    "div.richtext.richtext-wysiwyg p"
                ).all_text_contents()

                content = " ".join(p.strip() for p in paragraphs if p.strip())
                item["article_content"] = content if content else None

            except Exception as e:
                logger.error(f"Failed scraping article: {e}")
            finally:
                await page.close()


async def METLIFEIMCO(target_date):
    url = "https://investments.metlife.com/insights/"
    scraper = MetLifeScraper(target_date)
    results = await scraper.scrape(url)

    with open(f"/tmp/{company_site_id}.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    logger.info(f"Scraped {len(results)} articles after {target_date}")
    return 200


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-12-01"
    asyncio.run(METLIFEIMCO(target_date))
