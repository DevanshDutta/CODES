import asyncio
import json
import logging
import sys
import re
from dateutil import parser
from playwright.async_api import async_playwright

site = "Vanguard"
section = "Insights"
country = "United Kingdom"
role = "Professional Investor"
company_site_id = "am-206"

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
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
            page = await context.new_page()

            await page.set_extra_http_headers({
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1"
            })

            await page.goto(url, timeout=60000)
            await asyncio.sleep(self.sleep_time)

            seen_urls = set()
            stop = False
            page_count = 1

            # ------------------------------------------------------------
            # PAGINATED LISTING LOOP
            # ------------------------------------------------------------
            while not stop:

                # Wait for UK article cards
                await page.wait_for_selector("nds-base-card-article", timeout=10000)
                cards = await page.locator("nds-base-card-article").all()

                logger.info(f"Page {page_count}: Found {len(cards)} article cards")

                for card in cards:
                    try:
                        link_el = card.locator("a.nds-base-card-article-link-wrapper")
                        href = await link_el.get_attribute("href")
                        if not href:
                            continue

                        if href.startswith("/"):
                            article_url = "https://www.vanguard.co.uk" + href
                        else:
                            article_url = href

                        if article_url in seen_urls:
                            continue
                        seen_urls.add(article_url)

                        raw_date = None
                        date_node = card.locator("span.nds-card-content-header__date")

                        if await date_node.count():
                            raw_date = (await date_node.text_content()).strip()

                        parsed_date = None
                        if raw_date:
                            try:
                                parsed_date = parser.parse(raw_date, fuzzy=True).date()
                            except:
                                parsed_date = None

                        logger.info(f"Date {parsed_date} → {article_url}")

                        # TARGET-DATE STOP LOGIC
                        if parsed_date and parsed_date < self.target_date:
                            logger.info(f"Stopping: {parsed_date} < target_date {self.target_date}")
                            stop = True
                            break

                        title_el = card.locator("h3.nds-base-card__title")
                        title = (await title_el.text_content()).strip() if await title_el.count() else None

                        # DESCRIPTION
                        desc_el = card.locator("div.nds-base-card__body.with-tags")
                        description = (await desc_el.text_content()).strip() if await desc_el.count() else None

                        # TAGS
                        tags = await card.locator(
                            "div.nds-card-content-tags__tags span.nds-tag-text"
                        ).all_text_contents()

                        # ARTICLE CONTENT
                        content = await self.scrape_article(context, article_url)

                        self.items.append({
                            "company_site_id": company_site_id,
                            "company_site_country": country,
                            "company_site_role": role,
                            "article_source": site,
                            "article_section": section,
                            "article_date": str(parsed_date) if parsed_date else None,
                            "article_title": title,
                            "article_description": description,
                            "article_content": content,
                            "article_tags": tags,
                            "article_slug": article_url.rstrip("/").split("/")[-1],
                            "article_url": article_url,
                        })

                    except Exception as e:
                        logger.exception(f"Error processing card: {e}")
                        continue

                if stop:
                    break

                # PAGINATION BUTTON
                next_button = page.locator(
                    "button[aria-label='Next page'], button[aria-label='Next'], a[rel='next']"
                )

                if not await next_button.count():
                    logger.info("Next button not found → stopping pagination.")
                    break

                disabled = await next_button.first.get_attribute("disabled")
                if disabled is not None:
                    logger.info("Next button disabled → stopping pagination.")
                    break

                logger.info(f"Clicking next page button (page {page_count + 1})")
                await next_button.first.click()
                await asyncio.sleep(self.sleep_time)
                page_count += 1

            await browser.close()
            logger.info(f"Scraping complete — total {len(self.items)} articles collected")
            return self.items

    async def scrape_article(self, context, url):
        try:
            page = await context.new_page()
            await page.goto(url, timeout=60000)

            # Allow JS hydration
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(1)

            content_list = []

            # ----------------------------
            # 1) Extract heading section
            # ----------------------------
            try:
                heading_blocks = await page.locator("nds-base-article-heading div").all()
                for block in heading_blocks:
                    txt = await block.text_content()
                    if txt and txt.strip():
                        content_list.append(txt.strip())
            except Exception as e:
                logger.debug(f"Heading extraction failed for {url}: {e}")

            # ----------------------------
            # 2) Extract main blog content
            # ----------------------------
            try:
                body_blocks = await page.locator("nds-aem-blog-post-container div").all()
                for block in body_blocks:
                    txt = await block.text_content()
                    if txt and txt.strip():
                        content_list.append(txt.strip())
            except Exception as e:
                logger.debug(f"Body extraction failed for {url}: {e}")

            await page.close()

            if not content_list:
                logger.warning(f"No article content extracted for {url}")
                return None

            return "\n".join(content_list)

        except Exception as e:
            logger.error(f"Error scraping article {url}: {e}")
            return None


async def VanguardUKPI(target_date):
    results = []
    try:
        url = "https://www.vanguard.co.uk/professional/insights"
        scraper = VanguardScraper(target_date)
        results = await scraper.scrape(url)
        output_path = f"/tmp/{company_site_id}.json"

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)

        logger.info(f"Scraped {len(results)} articles → {output_path}")
        return 200

    except Exception as e:
        logger.error(f"Error: {e}")
        return 500


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-11-01"
    logger.info(f"Scraping UK Insights using target date: {target_date}")
    asyncio.run(VanguardUKPI(target_date))

