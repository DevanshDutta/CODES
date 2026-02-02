import asyncio
import json
import logging
import sys
from datetime import datetime
from dateutil import parser
from playwright.async_api import async_playwright

# --- Site metadata ---
site = "BNP Paribas Asset Management"
section = "Insights"
company_site_id = "am-238"
country = "United Kingdom"
role = "Financial Intermediary"
BASE_URL = "https://www.bnpparibas-am.com"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)

class BNPUKScraper:
    def __init__(self, target_date, sleep_time=2):
        self.target_date = parser.parse(target_date, fuzzy=True).date()
        self.sleep_time = sleep_time
        self.items = []

    async def scrape(self):
        logger.info("Starting BNP UK FI scraper")

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
                ],
            )

            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ""AppleWebKit/537.36 (KHTML, like Gecko) ""Chrome/91.0.4472.124 Safari/537.36"
                )
            )
            page = await context.new_page()

            url_list = [
                "https://www.bnpparibas-am.com/en-gb/intermediaries/insights/category/front-of-mind/",
                "https://www.bnpparibas-am.com/en-gb/intermediaries/insights/category/portfolio-perspectives/",
                "https://www.bnpparibas-am.com/en-gb/intermediaries/insights/category/forward-thinking/",
            ]

            all_cards = []
            for url in url_list:
                logger.info(f"Processing category URL: {url}")
                cards = await self._scrape_listing_page(page, url)
                all_cards.extend(cards)

            await self.scrape_article_pages(context, all_cards)
            await browser.close()
            logger.info(f"BNP scraper finished with {len(self.items)} articles having content")
            return self.items

    async def _scrape_listing_page(self, page, url):
        """Replicates your Selenium listing logic with Playwright, Option B filtering."""
        cards_data = []

        await page.goto(url, timeout=120000)
        await asyncio.sleep(self.sleep_time)

        try:
            await page.locator("#onetrust-accept-btn-handler").click(timeout=5000)
            logger.debug("Accepted cookies")
            await asyncio.sleep(1)
        except Exception:
            logger.debug("Cookie banner not found")

        try:
            await page.locator('xpath=//*[@id="investor_types"]/button[2]').click(timeout=5000)
            logger.debug("Selected investor type: Intermediary")
            await asyncio.sleep(1)
        except Exception:
            logger.debug("Investor type selection not present")

        try:
            await page.locator(".accept-button").click(timeout=5000)
            logger.debug("Accepted disclaimer")
            await asyncio.sleep(1)
        except Exception:
            logger.debug("Disclaimer button not found")

        while True:

            try:
                load_more = page.locator("#load-more-posts")
                if await load_more.is_visible():
                    logger.info("Clicking 'Load more posts'")
                    await load_more.click()
                    await asyncio.sleep(self.sleep_time)
                else:
                    logger.info("No visible 'Load more posts' button - stopping pagination")
                    break
            except Exception:
                logger.info("No 'Load more posts' button found - stopping pagination")
                break

            try:
                posts = page.locator("a.post")
                count = await posts.count()
                if count == 0:
                    logger.info("No posts found on page - stopping pagination")
                    break

                last_post = posts.nth(count - 1)
                date_text = await last_post.locator(".date").text_content()
                if date_text:
                    date_text = date_text.strip()
                    last_date = parser.parse(date_text, dayfirst=True).date()
                    logger.debug(f"Last loaded article date: {last_date}")

                    if last_date < self.target_date:
                        logger.info(
                            f"Stopping pagination on this category: last_date {last_date} < target_date {self.target_date}"
                        )
                        break
            except Exception as e:
                logger.warning(f"Error checking last article date during pagination: {e}")
                continue

        posts = page.locator("a.post")
        count = await posts.count()
        logger.info(f"Found {count} article cards on listing for URL: {url}")

        for idx in range(count):
            post = posts.nth(idx)
            try:
                date_text = await post.locator(".date").text_content()
                if not date_text:
                    logger.debug("Skipping card with no date")
                    continue

                date_text = date_text.strip()
                article_date = parser.parse(date_text, dayfirst=True).date()

                if article_date < self.target_date:
                    logger.debug(f"Skipping outdated card: {article_date} < {self.target_date}")
                    continue
                title = await post.locator("h3").text_content()
                title = title.strip() if title else None
                href = await post.get_attribute("href")
                if not href:
                    logger.debug("Skipping card with no href")
                    continue
                article_url = href if href.startswith("http") else BASE_URL.rstrip("/") + href
                description = None
                try:
                    paragraphs = post.locator("p")
                    p_count = await paragraphs.count()
                    if p_count > 0:
                        last_p = await paragraphs.nth(p_count - 1).text_content()
                        description = last_p.strip() if last_p else None
                except Exception:
                    description = None
                slug = article_url.rstrip("/").split("/")[-1] if article_url else None
                card_data = {
                    "company_site_id": company_site_id,
                    "company_site_country": country,
                    "company_site_role": role,
                    "article_source": site,
                    "article_section": section,
                    "article_date": str(article_date),
                    "article_title": title,
                    "article_description": description,
                    "article_content": None,
                    "article_tags": [],
                    "article_slug": slug,
                    "article_url": article_url,
                }
                logger.debug(f"Collected listing card: {title} | {article_date} | {article_url}")
                cards_data.append(card_data)
            except Exception as e:
                logger.warning(f"Skipping article due to error: {e}")
                continue
        return cards_data

    async def scrape_article_pages(self, context, cards_data):
        """Deep scrape individual article pages (same logic as your Selenium version)."""
        logger.debug("Starting deep article scraping")
        page = await context.new_page()

        for idx, item in enumerate(cards_data, start=1):
            url = item.get("article_url")
            if not url:
                continue

            try:
                await page.goto(url, timeout=120000)
                await asyncio.sleep(self.sleep_time)

                try:
                    tag_elements = page.locator(".tag-item")
                    tag_count = await tag_elements.count()
                    tags = []
                    for i in range(tag_count):
                        txt = await tag_elements.nth(i).text_content()
                        if txt:
                            tags.append(txt.strip())
                except Exception:
                    tags = []

                try:
                    content = await page.locator(".content-wrapper").text_content()
                    content = content.strip() if content else None
                except Exception:
                    content = None
                try:
                    meta_desc = await page.locator('meta[name="description"]').get_attribute("content")
                    description = meta_desc.strip() if meta_desc else None
                except Exception:
                    description = item.get("article_description")
                try:
                    title = await page.locator("h1").text_content()
                    title = title.strip() if title else None
                except Exception:
                    title = item.get("article_title")
                item["article_title"] = title
                item["article_content"] = content
                item["article_description"] = description
                item["article_tags"] = tags

                if content:
                    self.items.append(item)
                logger.debug(f"Deep scraped article #{idx}: {title}")
            except Exception as e:
                logger.error(f"[Error:] Failed to scrape article page {url}: {e}")
                continue
        await page.close()

async def BNPUKFI(target_date):
    results = []
    try:
        scraper = BNPUKScraper(target_date)
        results = await scraper.scrape()
        output_path = f"/tmp/{company_site_id}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)
        logger.info(f"Scraped {len(results)} BNP UK FI articles after {target_date}")
        print(f"\nJSON saved at: {output_path}\n")
        return 200
    except Exception as error:
        logger.error(f"Error in BNPUKFI: {error}")
        return 500


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2026-01-01"
    logger.info(f"Scraping BNP UK FI articles using target date: {target_date}")
    asyncio.run(BNPUKFI(target_date=target_date))
