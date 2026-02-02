import asyncio
import json
import logging
import sys
from dateutil import parser
from playwright.async_api import async_playwright


# ---------------- SITE METADATA ----------------
site = "PineBridge Investments"
section = "Insights"
company_site_id = "am-416"
country = "Singapore"
role = "Intermediary"
BASE_URL = "https://www.pinebridge.com"


def extract_date(text):
    try:
        return parser.parse(text, fuzzy=True).date()
    except Exception:
        return None

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)


class PineBridgeScraper:
    def __init__(self, target_date, sleep_time=5):
        self.target_date = parser.parse(target_date, fuzzy=True).date()
        self.sleep_time = sleep_time
        self.items = []
        self.seen_urls = set()

    async def scrape(self, url):
        logger.info(f"Starting scraper → {url}")

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=False,
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
                ],
            )

            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            )

            page = await context.new_page()
            await page.goto(url, timeout=60000)
            await asyncio.sleep(self.sleep_time)

            try:
                await page.wait_for_selector("button:has-text('Accept')", timeout=15000)
                await page.click("button:has-text('Accept')")
                await asyncio.sleep(self.sleep_time)
            except Exception:
                pass

            # -------- PAGINATION LOOP --------
            page_number = 1

            while True:
                logger.info(f"Scraping page {page_number}")

                await page.wait_for_selector("li.ais-Hits-item", timeout=15000)
                cards = page.locator("li.ais-Hits-item")
                total_cards = await cards.count()
                logger.debug(f"Found {total_cards} cards")

                for i in range(total_cards):
                    card = cards.nth(i)

                    try:
                        raw_date = await card.locator("div.text-pinebridgegrey-200").first.text_content()
                        date = extract_date(raw_date)
                    except Exception:
                        continue

                    if not date or date < self.target_date:
                        continue

                    title = await card.locator("div.text-pinebridgeblue-50.text-xl").text_content() or ""
                    href = await card.locator("a").first.get_attribute("href")
                    if not href:
                        continue

                    article_url = href if href.startswith("http") else BASE_URL + href
                    if article_url in self.seen_urls:
                        continue
                    self.seen_urls.add(article_url)
                    slug = href.rstrip("/").split("/")[-1]

                    self.items.append({
                        "company_site_id": company_site_id,
                        "company_site_country": country,
                        "company_site_role": role,
                        "article_source": site,
                        "article_section": section,
                        "article_date": str(date),
                        "article_title": title.strip(),
                        "article_description": None,
                        "article_content": None,
                        "article_tags": [],
                        "article_slug": slug,
                        "article_url": article_url,
                    })

                try:
                    last_card = cards.nth(total_cards - 1)
                    raw_date = await last_card.locator("div.text-pinebridgegrey-200").first.text_content()
                    last_date = extract_date(raw_date)
                except Exception:
                    break

                if last_date and last_date >= self.target_date:
                    next_btn = page.locator("li.ais-Pagination-item--nextPage a")
                    if await next_btn.count() > 0:
                        logger.info("Clicking next page")
                        await next_btn.first.click()
                        await page.wait_for_load_state("networkidle")
                        await asyncio.sleep(self.sleep_time)
                        page_number += 1
                    else:
                        break
                else:
                    logger.info("Reached older articles — stopping pagination")
                    break

            await self.scrape_article_pages(context)
            await browser.close()
            return self.items

    async def scrape_article_pages(self, context):
        logger.info("Scraping article pages")

        for item in self.items:
            page = await context.new_page()
            try:
                await page.goto(item["article_url"], timeout=60000)
                await asyncio.sleep(self.sleep_time)

                # ---- DESCRIPTION ----
                desc = await page.locator("div.undefined.false ul li p, div.undefined.false p").all_text_contents()
                desc_clean = [d.strip() for d in desc if d.strip()]
                item["article_description"] = " ".join(desc_clean) if desc_clean else None

                # ---- CONTENT ----
                paragraphs = await page.locator(
                    "div.undefined.false p, div.rich-content_isi__I1C6Q p, "
                    "div.rich-content_isi__I1C6Q h2, div.rich-content_isi__I1C6Q h3"
                ).all_text_contents()

                content_clean = [p.strip() for p in paragraphs if p.strip()]
                item["article_content"] = " ".join(content_clean) if content_clean else None

                if not content_clean:
                    logger.warning(f"No content found → {item['article_url']}")

            except Exception as e:
                logger.error(f"ERROR scraping {item['article_url']}: {e}")
            finally:
                await page.close()

async def PBISGI(target_date):
    url = "https://www.pinebridge.com/en-sg/intermediary-and-individual/all-insights"
    scraper = PineBridgeScraper(target_date)
    results = await scraper.scrape(url)

    output_path = f"/tmp/{company_site_id}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    logger.info(f"Scraped {len(results)} articles after {target_date}")
    return 200


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-12-01"
    logger.info(f"Scraping articles using target date: {target_date}")
    asyncio.run(PBISGI(target_date))
