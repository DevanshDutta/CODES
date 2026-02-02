import asyncio
import json
import logging
import sys
from datetime import datetime
from dateutil import parser
from playwright.async_api import async_playwright

# --- Site metadata ---
site = "Invesco"
section = "Insights"
company_site_id = "am-301"
country = "United States"
role = "Financial Professional"
BASE_URL = "https://www.invesco.com"

URL_LIST = [
    "https://www.invesco.com/us/en/insights/topic/featured-insights.html",
    "https://www.invesco.com/us/en/insights/topic/market-and-economic-insights.html",
    "https://www.invesco.com/us/en/insights/topic/investment-related-insights.html",
    "https://www.invesco.com/us/en/solutions/invesco-etfs/etf-insights.html",
    "https://www.invesco.com/us/en/insights/topic/defined-contribution-insights.html",
]

# --- Logging setup ---
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)


class InvescoScraperUSFP:
    def __init__(self, target_date: str, sleep_time: int = 3):
        """
        target_date: string, e.g. "2025-11-01"
        Only articles with date >= target_date should be scraped.
        Pagination (Load More) stops as soon as the last card is older than target_date.
        """
        self.target_date = parser.parse(target_date, fuzzy=True).date()
        self.sleep_time = sleep_time

    async def scrape(self, url: str):
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
                    "--window-size=1280,1696",
                ],
            )

            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ""AppleWebKit/537.36 (KHTML, like Gecko) ""Chrome/91.0.4472.124 Safari/537.36"))
            page = await context.new_page()
            await page.goto(url, timeout=60000)
            try:
                await page.get_by_role("button", name="Financial Professional").click()
                logger.debug("Clicked Financial Professional")
            except :
                logger.debug("Failed To click button")
            await asyncio.sleep(self.sleep_time)
            try:
                cookie_accept = page.get_by_role("button", name="Accept")
                if await cookie_accept.is_visible():
                    await cookie_accept.click()
                    await asyncio.sleep(1)
                    logger.debug("Cookie banner closed")
            except Exception as e:
                logger.debug(f"No cookie banner handled: {e}")
            try:
                await page.wait_for_selector("article.content-card", state="attached", timeout=20000)
            except Exception as e:
                logger.error(f"No article cards found on page: {e}")
            await self.load_all_cards(page)
            items = await self.scrape_listing(page, context)

            await browser.close()
            logger.info(f"Finished scraping {len(items)} articles from: {url}")
            return items
    async def get_last_card_date(self, page):
        """
        Return the date of the last (oldest) visible card, or None if not parseable.
        """
        cards = page.locator("article.content-card")
        count = await cards.count()
        if count == 0:
            return None

        last_card = cards.nth(count - 1)
        try:
            date_text = await last_card.locator(".content-card__date").text_content()
            if date_text:
                card_date = parser.parse(date_text.strip(), fuzzy=True).date()
                return card_date
        except Exception as e:
            logger.debug(f"Unable to parse last card date: {e}")
        return None
    async def load_all_cards(self, page):
        """
        Uses scroll-to-bottom before each check to trigger lazy JS and ensure
        the Load More button is inserted for every section.
        """
        logger.debug("Starting pagination / Load More loop...")
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(2)

        while True:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1)
            last_date = await self.get_last_card_date(page)
            if last_date:
                logger.debug(f"Last card date on page: {last_date}, target_date: {self.target_date}")
                if last_date < self.target_date:
                    logger.info(
                        f"Stopping Load More: reached card older than target_date "
                        f"({last_date} < {self.target_date})"
                    )
                    break
            try:
                load_more_btn = page.locator("button.content-cards-section__loadMore")
                visible = await load_more_btn.is_visible()
                enabled = await load_more_btn.is_enabled()
                if not visible or not enabled:
                    logger.info("Load More button not visible/enabled; pagination finished.")
                    break
                await load_more_btn.click()
                logger.debug("Clicked Load More button")
                await asyncio.sleep(2)

            except Exception as e:
                logger.info(f"Load More button not clickable or missing; stopping pagination. {e}")
                break
        logger.debug("Finished pagination loop.")
    async def scrape_listing(self, page, context):
        """
        Scrape all currently loaded article cards on the listing page.
        Skip cards whose date < target_date.
        Assumes cards are sorted newest -> oldest.
        """
        items = []

        try:
            cards = page.locator("article.content-card")
            count = await cards.count()
            logger.info(f"Total cards found after pagination: {count}")

            for idx in range(count):
                card = cards.nth(idx)
                article_date = None
                parsed_date = None
                try:
                    date_text = await card.locator(".content-card__date").text_content()
                    if date_text:
                        parsed_date = parser.parse(date_text.strip(), fuzzy=True).date()
                        article_date = parsed_date
                except Exception as e:
                    logger.error(f"Error parsing date for card #{idx+1}: {e}")
                    parsed_date = None
                if parsed_date and parsed_date < self.target_date:
                    logger.debug(
                        f"Card #{idx+1} is older than target_date "
                        f"({parsed_date} < {self.target_date}); stopping listing loop."
                    )
                    break
                try:
                    href = await card.locator("a").get_attribute("href")
                    if href:
                        url = href if href.startswith("http") else (BASE_URL + href)
                    else:
                        url = None
                except Exception as e:
                    logger.error(f"Error getting URL for card #{idx+1}: {e}")
                    url = None
                try:
                    title = await card.locator(".content-card__headline").text_content()
                    title = title.strip() if title else None
                except Exception as e:
                    logger.error(f"Error getting title for card #{idx+1}: {e}")
                    title = None
                try:
                    description = await card.locator(".content-card__description").text_content()
                    description = description.strip() if description else None
                except Exception as e:
                    logger.error(f"Error getting description for card #{idx+1}: {e}")
                    description = None
                slug = None
                if url:
                    slug = url.rstrip("/").split("/")[-1]
                items.append(
                    {
                        "company_site_id": company_site_id,
                        "company_site_country": country,
                        "company_site_role": role,
                        "article_source": site,
                        "article_section": section,
                        "article_date": str(article_date) if article_date else None,
                        "article_title": title,
                        "article_description": description,
                        "article_content": None,
                        "article_tags": [],
                        "article_slug": slug,
                        "article_url": url,
                    }
                )
            logger.info(f"Collected {len(items)} listing items (before article page scraping).")
            await self.scrape_article_pages(context, items)
        except Exception as e:
            logger.error(f"Error scraping listing page: {e}")
        return items
    async def scrape_article_pages(self, context, items):
        logger.debug("Starting to scrape individual article pages...")
        page = await context.new_page()
        for idx, item in enumerate(items, start=1):
            url = item.get("article_url")
            if not url:
                continue
            if url.lower().endswith(".pdf"):
                item["article_content"] = url
                logger.debug(f"#{idx}: Skipped PDF -> {url}")
                continue
            try:
                await page.goto(url, timeout=60000)
                await asyncio.sleep(self.sleep_time)
                try:
                    content_blocks = await page.locator(
                        ".rich-text-editor, .rich-text-editor__inner"
                    ).all_text_contents()
                    full_text = " ".join(
                        t.strip() for t in content_blocks if t and t.strip()
                    ).strip()
                    item["article_content"] = full_text if full_text else None
                except Exception as e:
                    logger.error(f"Error getting content for article #{idx}: {e}")
                    item["article_content"] = None
                try:
                    tag_texts = await page.locator(".content-card__eyebrow").all_text_contents()
                    tags = [t.strip() for t in tag_texts if t and t.strip()]
                    item["article_tags"] = tags
                except Exception as e:
                    logger.error(f"Error getting tags for article #{idx}: {e}")
                    item["article_tags"] = []
                logger.debug(
                    f"Scraped article #{idx}: "
                    f"{(item.get('article_title') or '')[:60]} "
                    f"URL: {url}"
                )
            except Exception as e:
                logger.error(f"Failed to scrape article #{idx} ({url}): {e}")
        await page.close()
        logger.debug("Finished scraping individual article pages.")

async def InvescoUSFP(target_date: str):
    results = []
    try:
        for url in URL_LIST:
            scraper = InvescoScraperUSFP(target_date)
            items = await scraper.scrape(url)
            results.extend(items)
        unique = {}
        for item in results:
            key = item.get("article_url") or item.get("article_slug")
            if key and key not in unique:
                unique[key] = item
        final_results = list(unique.values())
        logger.info(
            f"Total unique Invesco USFP articles after target_date filter: {len(final_results)}"
        )

        output_path = f"/tmp/{company_site_id}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(final_results, f, indent=4, ensure_ascii=False)
        logger.info(f"Saved results to {output_path}")
        return 200
    except Exception as error:
        logger.error(f"Error in InvescoUSFP: {error}")
        return 500
    
if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-10-01"
    logger.info(f"Scraping Invesco USFP using target date: {target_date}")
    asyncio.run(
        InvescoUSFP(
            target_date=target_date
        )
    )
