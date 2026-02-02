import asyncio
import json
import logging
import sys
import re
import os
from datetime import datetime
from dateutil import parser
from playwright.async_api import async_playwright

# --- Site metadata ---
site = "PIMCO"
section = "Insights"
company_site_id = "am-210"
country = "United States"
role = "Financial Advisor"
BASE_URL = "https://www.pimco.com"

# --- Logging setup ---
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)


# --- Scraper class ---
class PIMCOScraperUS:
    def __init__(self, target_date, sleep_time=3):
        # parse target_date same as template
        self.target_date = parser.parse(target_date, fuzzy=True).date()
        self.sleep_time = sleep_time
        self.items = []
        self.seen_urls = set()

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
                    "--window-size=1280,1696",
                ],
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

            # minimal content guard
            await page.set_content("<meta http-equiv='X-Content-Type-Options' content='nosniff'>")

            # Load initial page
            await page.goto(url, timeout=60000)
            await asyncio.sleep(self.sleep_time)

            # Accept cookies / overlays if present (best-effort)
            try:
                await page.locator("#onetrust-accept-btn-handler, .cc-btn.cc-allow, button[aria-label='accept']").first.click(timeout=5000)
                logger.debug("Cookie/consent accepted (if present)")
                await asyncio.sleep(1)
            except Exception:
                # ignore if not present
                pass

            # Attempt to detect the number of pages from pager
            total_pages = 1
            try:
                # Wait for pager; Coveo uses .coveo-pager-list-item or similar
                await page.wait_for_selector(".coveo-pager-list-item span, .coveo-pager-list-item-text", timeout=5000)
                pager_buttons = await page.locator(".coveo-pager-list-item span, .coveo-pager-list-item-text").all()
                page_nums = []
                for btn in pager_buttons:
                    try:
                        txt = (await btn.text_content() or "").strip()
                        if txt.isdigit():
                            page_nums.append(int(txt))
                    except Exception:
                        continue
                if page_nums:
                    total_pages = max(page_nums)
                logger.info(f"Detected {total_pages} pagination pages")
            except Exception as e:
                # fallback: try to infer from URL hash or default large number
                logger.warning(f"Pagination not detected via DOM, defaulting strategy: {e}")
                # We'll still paginate using `first` offsets until we stop due to date
                total_pages = 100  # safe upper bound; loop will break early on date condition

            # --- Loop through pages ---
            stop_scraping = False
            # Coveo seems to use first offset increments of 10 (example you gave). Use page_size 10.
            page_size = 10
            # If pager detected and reasonable, cap to that; otherwise rely on default and stop by date.
            if total_pages > 50:
                # keep reasonable cap to avoid endless runs if site huge
                max_pages = total_pages
            else:
                max_pages = total_pages

            for p_no in range(1, max_pages + 1):
                logger.info(f"Scraping page {p_no}...")

                # Construct page URL with fragment `#first=...&sort=...`
                # Page 1 uses no first param (or first=0). We will always use first=(p_no-1)*page_size
                first_offset = (p_no - 1) * page_size
                fragment = f"#first={first_offset}&sort=%40publishz32xdate%20descending"
                page_url = BASE_URL + "/us/en/insights" + fragment

                try:
                    # Navigate to the page (works for URL-changing pagination)
                    await page.goto(page_url, timeout=60000)
                    await asyncio.sleep(self.sleep_time)
                    # Wait until results are present
                    await page.wait_for_selector(".coveo-result-cell.coveoforsitecore-information-section, .coveo-result", timeout=15000)
                    # attempt to dismiss any small consent popups
                    try:
                        await page.locator(".cc-btn.cc-allow, #onetrust-accept-btn-handler").first.click(timeout=2000)
                    except Exception:
                        pass
                    # scroll to bottom to ensure lazy load run
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(1)
                except Exception as e:
                    logger.warning(f"Failed loading page {p_no} ({page_url}): {e}")
                    # If navigation fails, try next page (don't crash)
                    continue

                # Collect article cards
                try:
                    cards = await page.locator(".coveo-result-cell.coveoforsitecore-information-section, .coveo-result").all()
                    logger.info(f"Page {p_no}: Found {len(cards)} article cards")
                except Exception as e:
                    logger.warning(f"No article cards found on page {p_no}: {e}")
                    cards = []

                # Iterate cards and collect listing-level metadata
                for idx, card in enumerate(cards, start=1):
                    try:
                        # Title and relative URL
                        title_el = card.locator("a.CoveoResultLink").first
                        title = (await title_el.text_content()) or None
                        href = (await title_el.get_attribute("href")) or None
                        if not href:
                            # fallback: try anchor inside result
                            href = (await card.locator("a").first.get_attribute("href")) or None

                        url_full = href if (href and href.startswith("http")) else (BASE_URL + href if href else None)
                        if not url_full:
                            logger.debug(f"Card {idx} has no URL, skipping")
                            continue

                        # avoid duplicates
                        if url_full in self.seen_urls:
                            logger.debug(f"Duplicate URL skipped: {url_full}")
                            continue
                        self.seen_urls.add(url_full)

                        # slug
                        slug = url_full.rstrip("/").split("/")[-1]

                        # Date extraction — Coveo result date stored in span title attr (Selenium code had it)
                        date_text = None
                        try:
                            # try the element with title attribute
                            date_el = card.locator(".coveo-result-row.result-date span.CoveoFieldValue span").first
                            date_text = await date_el.get_attribute("title")
                        except Exception:
                            # fallback: try visible text
                            try:
                                date_text = (await card.locator(".coveo-result-row.result-date .CoveoFieldValue").text_content()) or None
                            except Exception:
                                date_text = None

                        parsed_date = None
                        if date_text:
                            try:
                                parsed_date = parser.parse(date_text, fuzzy=True).date()
                            except Exception:
                                parsed_date = None

                        # If date parsed and older than target -> set stop flag
                        if parsed_date and parsed_date < self.target_date:
                            stop_scraping = True
                            logger.info(f"STOP: Found older article {parsed_date} < {self.target_date} (page {p_no})")
                            break

                        # description (short)
                        try:
                            description = (await card.locator(".coveo-result-row.result-text span.CoveoFieldValue span").text_content()) or None
                        except Exception:
                            description = None

                        self.items.append({
                            "company_site_id": company_site_id,
                            "company_site_country": country,
                            "company_site_role": role,
                            "article_source": site,
                            "article_section": section,
                            "article_date": str(parsed_date) if parsed_date else None,
                            "article_title": title,
                            "article_description": description,
                            "article_content": None,
                            "article_tags": [],
                            "article_slug": slug,
                            "article_url": url_full
                        })
                        logger.debug(f"Added listing: {title} -> {url_full}")

                    except Exception as e:
                        logger.error(f"Error parsing card on page {p_no} #{idx}: {e}")
                        continue

                if stop_scraping:
                    break

            # After listing collection, scrape each article page for full content
            await self.scrape_article_pages(context)

            await browser.close()
            logger.info(f"Finished scraping {len(self.items)} total articles")
            # Keep behaviour similar to template: only keep items that have article_content
            self.items = [item for item in self.items if item.get("article_content")]
            return self.items

    async def scrape_article_pages(self, context):
        logger.debug("Starting to scrape individual article pages...")
        page = await context.new_page()

        for idx, item in enumerate(self.items, start=1):
            url = item.get("article_url")
            if not url:
                continue

            # Skip PDFs
            if url.lower().endswith(".pdf"):
                item["article_content"] = url
                item["article_date"] = item.get("article_date") or str(self.target_date)
                logger.debug(f"#{idx}: Skipped PDF -> {url}")
                continue

            try:
                await page.goto(url, timeout=60000)
                await asyncio.sleep(self.sleep_time)

                # Accept small consent on article pages if any
                try:
                    await page.locator(".cc-btn.cc-allow, #onetrust-accept-btn-handler").first.click(timeout=2000)
                except Exception:
                    pass

                # description (meta)
                try:
                    desc = await page.locator('meta[name="description"]').get_attribute("content")
                    item["article_description"] = desc
                except Exception:
                    item["article_description"] = item.get("article_description")

                # tags: try hero eyebrow or other possible selectors
                try:
                    tags = await page.locator(".hero__eyebrow-subtitle, .article-tags li, .tag, .coveo-field-json .tags").all_text_contents()
                    tags = [t.strip() for t in tags if t and t.strip()]
                    item["article_tags"] = tags
                except Exception:
                    item["article_tags"] = item.get("article_tags", []) or []

                # content: common PIMCO article container(s)
                try:
                    sel_candidates = [
                        ".page-text-area__text",
                        ".article__content",
                        "article",
                        "main",
                        ".rich-text"
                    ]
                    texts = []
                    for sel in sel_candidates:
                        try:
                            parts = await page.locator(sel).all_text_contents()
                            if parts:
                                texts.extend([p.strip() for p in parts if p and p.strip()])
                        except Exception:
                            continue
                    content_text = " ".join(texts).strip() if texts else None
                except Exception:
                    content_text = None

                item["article_content"] = content_text

                # date extraction from article page if present
                try:
                    time_el = page.locator("time").first
                    time_attr = await time_el.get_attribute("datetime")
                    if time_attr:
                        dt = parser.parse(time_attr, fuzzy=True)
                        item["article_date"] = str(dt.date())
                except Exception:
                    # keep existing article_date if any
                    pass

                logger.debug(f"Scraped article #{idx}: {item.get('article_title','')[:60]} ({item.get('article_date')})")

            except Exception as e:
                logger.error(f"Failed to scrape article #{idx} ({url}): {e}")
                # continue to next article

        await page.close()


async def PIMCOUSFA(target_date):
    results = []
    try:
        url = BASE_URL + "/us/en/insights#sort=%40publishz32xdate%20descending"
        scraper = PIMCOScraperUS(target_date)
        results = await scraper.scrape(url)
        output_path = f"/tmp/{company_site_id}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)
        logger.info(f"Scraped {len(results)} articles after {target_date}")
        logger.info(f"Saved JSON at: {output_path}")
        return 200
    except Exception as error:
        logger.error(f"Error: {error}")
        return 500


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2026-01-01"
    logger.info(f"Scraping PIMCO U.S. articles using target date: {target_date}")
    status = asyncio.run(PIMCOUSFA(target_date=target_date))
    logger.info(f"Exit status: {status}")
