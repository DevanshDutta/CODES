import asyncio
import json
import logging
import sys
from dateutil import parser
from playwright.async_api import async_playwright

site = "PGIM"
section = "Insights"
company_site_id = "am-428"
country = "United States"
role = "Intermediary"
BASE_URL = "https://www.pgim.com"


def extract_date(text):
    try:
        return parser.parse(text, fuzzy=True).date()
    except Exception:
        return None


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)


class PGIMScraper:
    def __init__(self, target_date, sleep_time=4):
        self.target_date = parser.parse(target_date).date()
        self.sleep_time = sleep_time
        self.items = []
        self.seen_urls = set()

    async def handle_attestation(self, page):
        try:
            await page.wait_for_selector("a.cmp-cta__link", timeout=5000)

            all_radio = page.locator("input#otc-all")
            if await all_radio.count() > 0:
                await all_radio.first.check()
                await asyncio.sleep(1)

            save_btn = page.locator("a.cmp-cta__link:has-text('Save')")
            if await save_btn.count() > 0:
                await save_btn.first.click()
                logger.info("Attestation accepted")
                await asyncio.sleep(self.sleep_time)

        except Exception:
            logger.debug("No attestation popup")

    async def remove_attestation_dom(self, page):
        """CRITICAL FIX: removes hidden overlay blocking clicks"""
        try:
            await page.evaluate("""
                document.querySelectorAll(
                    '#attestationMainDiv, .attestationHTML, .attestationForm, .modal'
                ).forEach(el => el.remove());

                document.body.style.pointerEvents = 'auto';
                document.body.classList.remove('modal-open');
            """)
            await asyncio.sleep(1)
            logger.info("Attestation DOM removed")
        except Exception:
            pass

    async def scrape(self, url):
        logger.info(f"Starting scraper → {url}")

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
                    "--window-size=1280,1696",
                ],
            )

            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            )

            page = await context.new_page()
            await page.goto(url, timeout=60000)
            await asyncio.sleep(self.sleep_time)

            await self.handle_attestation(page)
            await self.remove_attestation_dom(page)

            await page.wait_for_selector(
                "a.cmp-searchresult-link-wrapper", timeout=20000
            )

            page_index = 1

            while True:
                cards = await page.locator(
                    "a.cmp-searchresult-link-wrapper"
                ).all()

                logger.info(f"Page {page_index}: {len(cards)} cards")

                stop_pagination = False

                for card in cards:
                    href = await card.get_attribute("href")
                    if not href or href in self.seen_urls:
                        continue

                    self.seen_urls.add(href)

                    try:
                        raw_date = await card.locator(
                            "span.cmp-searchresult-date"
                        ).text_content()
                        date = extract_date(raw_date)
                    except Exception:
                        date = None

                    if date and date < self.target_date:
                        stop_pagination = True
                        continue

                    try:
                        title = (
                            await card.locator(
                                "h5.cmp-searchresult-title"
                            ).text_content()
                        ).strip()
                    except Exception:
                        title = ""

                    desc_locator = card.locator(
                        "p.cmp-searchresult-description"
                    )
                    description = (
                        await desc_locator.first.text_content()
                        if await desc_locator.count() > 0
                        else ""
                    )

                    self.items.append({
                        "company_site_id": company_site_id,
                        "company_site_country": country,
                        "company_site_role": role,
                        "article_source": site,
                        "article_section": section,
                        "article_date": str(date) if date else None,
                        "article_title": title,
                        "article_description": description.strip(),
                        "article_content": None,
                        "article_tags": [],
                        "article_slug": href.rstrip("/").split("/")[-1],
                        "article_url": BASE_URL + href if href.startswith("/") else href,
                    })

                if stop_pagination:
                    logger.info("Reached target date — stopping pagination")
                    break

                load_more = page.locator("button:has-text('Load more')")
                if await load_more.count() == 0:
                    logger.info("No Load More button")
                    break

                logger.info("Clicking Load More")
                await self.handle_attestation(page)
                await self.remove_attestation_dom(page)

                await load_more.first.scroll_into_view_if_needed()
                await asyncio.sleep(1)

                await load_more.first.click(force=True)
                await asyncio.sleep(self.sleep_time)

                page_index += 1

            await self.scrape_article_pages(context)
            await browser.close()

            return [
                i for i in self.items
                if i["article_date"]
                and parser.parse(i["article_date"]).date() >= self.target_date
            ]

    async def scrape_article_pages(self, context):
        for item in self.items:
            page = await context.new_page()
            try:
                await page.goto(item["article_url"], timeout=60000)
                await asyncio.sleep(self.sleep_time)

                paragraphs = await page.locator("p").all_text_contents()
                item["article_content"] = " ".join(
                    p.strip() for p in paragraphs if p.strip()
                )
            except Exception as e:
                logger.error(f"Article scrape error: {e}")
            finally:
                await page.close()


async def PGIMUSI(target_date):
    url = ("https://www.pgim.com/us/en/intermediary/insights/thought-leadership/latest-insights"
    )

    scraper = PGIMScraper(target_date)
    results = await scraper.scrape(url)

    output_path = f"/tmp/{company_site_id}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    logger.info(f"Scraped {len(results)} articles ≥ {target_date}")
    return 200


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2026-01-01"
    asyncio.run(PGIMUSI(target_date))
