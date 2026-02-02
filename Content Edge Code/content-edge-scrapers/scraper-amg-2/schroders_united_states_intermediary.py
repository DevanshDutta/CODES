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
site = "Schroders"
section = "Insights"
company_site_id = "am-229"
country = "United State"
role = "Intermediary"
BASE_URL = "https://www.schroders.com"

# --- Logging setup ---
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)

def _normalize_date_text(date_text: str):
    if not date_text:
        return None
    date_text = date_text.strip()
    date_text = re.sub(r"[^\w\s\/\-,]", " ", date_text).strip()
    try:
        if re.match(r"^\d{1,2}[-/]\d{1,2}[-/]\d{2,4}$", date_text):
            dt = parser.parse(date_text, dayfirst=False, fuzzy=True)
            return dt.date()
        if re.search(r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec|\w+ember)\b",
                     date_text, re.I):
            dt = parser.parse(date_text, fuzzy=True)
            return dt.date()
        if re.match(r"^\d{4}$", date_text):
            return datetime(int(date_text), 1, 1).date()
        dt = parser.parse(date_text, dayfirst=False, fuzzy=True)
        return dt.date()
    except Exception:
        return None



# --- Scraper class ---
class SchrodersScraperUS:
    def __init__(self, target_date, sleep_time=3):
        self.target_date = parser.parse(target_date, fuzzy=True).date()
        self.sleep_time = sleep_time
        self.items = []
        self.seen_urls = set()

    async def scrape(self, url):
        logger.info(f"Starting scraper for URL: {url} with target_date={self.target_date}")

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
                "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
                "Cache-Control": "max-age=0",
            })

            await page.set_content("<meta http-equiv='X-Content-Type-Options' content='nosniff'>")

            # Load initial page
            await page.goto(url, timeout=60000)
            await asyncio.sleep(self.sleep_time)

            # Accept cookies if present (best-effort)
            try:
                await page.locator("#onetrust-accept-btn-handler").click(timeout=5000)
                logger.debug("Cookie accept clicked")
                await asyncio.sleep(0.8)
            except Exception:
                pass
            current_page_index = 1
            keep_paginating = True

            while keep_paginating:
                logger.info(f"Processing listing page index: {current_page_index}")
                try:
                    await page.wait_for_selector("span.CardHeading__StyledHeading-sc-2flmla-0", state="attached", timeout=10000)
                except Exception:
                    logger.warning(f"No article headings found on page {current_page_index}")
                    break
                title_spans = await page.locator("span.CardHeading__StyledHeading-sc-2flmla-0").all()

                logger.info(f"Found {len(title_spans)} title nodes on page {current_page_index}")

                page_old_article_found = False

                for idx, span in enumerate(title_spans, start=1):
                    try:
                        try:
                            title = (await span.text_content()).strip()
                        except Exception:
                            title = None
                        try:
                            href = await span.evaluate("(el) => { const a = el.closest('a'); return a ? a.href : null; }")
                        except Exception:
                            href = None

                        if not href:
                            logger.debug(f"Skipping title #{idx} because no href found")
                            continue
                        if href.startswith("/"):
                            url_full = BASE_URL.rstrip("/") + href
                        else:
                            url_full = href

                        if url_full in self.seen_urls:
                            logger.debug(f"Duplicate URL skipped: {url_full}")
                            continue
                        self.seen_urls.add(url_full)

                        # slug
                        slug = url_full.rstrip("/").split("/")[-1] if url_full else None
                        date_text = await span.evaluate("""
                            (el) => {
                                // Search common nearby locations for the date element
                                const selectors = [
                                    ".CardFooter__FooterLabel-sc-t8rxlh-3",
                                    ".CardFooter__FooterLabel",
                                    ".Card__FooterLabel",
                                    "time",
                                    ".card-footer time"
                                ];
                                let rootA = el.closest('a') || el;
                                for (const sel of selectors) {
                                    // check inside the anchor
                                    let found = rootA.querySelector(sel);
                                    if (found && found.textContent) return found.textContent.trim();
                                    // check parent container
                                    if (rootA.parentElement) {
                                        found = rootA.parentElement.querySelector(sel);
                                        if (found && found.textContent) return found.textContent.trim();
                                    }
                                    // check next sibling
                                    if (rootA.nextElementSibling) {
                                        found = rootA.nextElementSibling.querySelector(sel);
                                        if (found && found.textContent) return found.textContent.trim();
                                    }
                                }
                                return null;
                            }
                        """)
                        if not date_text:
                            try:
                                date_text = await page.evaluate(f"""
                                    () => {{
                                        const a = document.querySelector("a[href='{href}']");
                                        if(!a) return null;
                                        const sel = a.closest('div') || a.parentElement;
                                        if(!sel) return null;
                                        const d = sel.querySelector(".CardFooter__FooterLabel-sc-t8rxlh-3, time, .CardFooter__FooterLabel");
                                        return d ? d.textContent.trim() : null;
                                    }}
                                """)
                            except Exception:
                                date_text = None
                        parsed_date = None
                        if date_text:
                            parsed_date = _normalize_date_text(date_text)
                        if not parsed_date:
                            try:
                                temp = await context.new_page()
                                await temp.goto(url_full, timeout=20000)
                                try:
                                    time_attr = await temp.locator("time").first.get_attribute("datetime")
                                    if time_attr:
                                        dt = parser.parse(time_attr, fuzzy=True)
                                        parsed_date = dt.date()
                                    else:
                                        # fallback to reading visible time text
                                        txt = await temp.locator("time").first.text_content()
                                        parsed_date = _normalize_date_text(txt) if txt else None
                                except Exception:
                                    parsed_date = None
                                await temp.close()
                            except Exception:
                                parsed_date = None
                        if not parsed_date:
                            parsed_date = self.target_date
                        if parsed_date < self.target_date:
                            logger.info(f"Found older article on page {current_page_index}: {parsed_date} < {self.target_date}. Stopping pagination.")
                            page_old_article_found = True
                            break
                        description = await span.evaluate("""
                            (el) => {
                                const a = el.closest('a') || el;
                                const d = a.querySelector('p[aria-label=\"article-subtitle\"]') ||
                                          a.parentElement && a.parentElement.querySelector('p[aria-label=\"article-subtitle\"]') ||
                                          a.nextElementSibling && a.nextElementSibling.querySelector('p[aria-label=\"article-subtitle\"]');
                                return d ? d.textContent.trim() : null;
                            }
                        """)
                        tags = [section] if section else []

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
                            "article_tags": tags,
                            "article_slug": slug,
                            "article_url": url_full
                        })
                        logger.debug(f"Added listing: {title} -> {url_full} ({parsed_date})")

                    except Exception as e:
                        logger.error(f"Error parsing title node #{idx} on page {current_page_index}: {e}")
                        continue
                if page_old_article_found:
                    logger.info("Stopping pagination due to older article found on current page.")
                    break
                current_page_index += 1
                try:
                    sel_btn = f'button[data-index="{current_page_index}"]'
                    btn = page.locator(sel_btn)
                    if await btn.count() > 0:
                        logger.info(f"Clicking pagination button for page {current_page_index}")
                        try:
                            await btn.first.click(timeout=8000)
                            await asyncio.sleep(self.sleep_time)
                            continue
                        except Exception as ex_click:
                            logger.warning(f"Failed to click page {current_page_index} button: {ex_click}")

                            break
                    else:
                        next_btn = page.locator('button[aria-label="Next"], button.PaginationNavButton__DefaultPaginationButton-sc-1lj9724-0[aria-label="Next"]')
                        if await next_btn.count() > 0:
                            try:
                                await next_btn.first.click(timeout=8000)
                                await asyncio.sleep(self.sleep_time)
                                continue
                            except Exception:
                                break
                        logger.info("No further pagination controls found. Ending pagination.")
                        break
                except Exception as e:
                    logger.warning(f"Pagination navigation error: {e}")
                    break
            await self.scrape_article_pages(context)

            await browser.close()
            logger.info(f"Finished scraping {len(self.items)} total articles")
            return self.items

    async def scrape_article_pages(self, context):
        logger.debug("Starting to scrape individual article pages...")
        page = await context.new_page()

        for idx, item in enumerate(self.items, start=1):
            url = item.get("article_url")
            if not url:
                continue
            if url.lower().endswith(".pdf"):
                item["article_content"] = url
                if not item.get("article_date"):
                    item["article_date"] = str(self.target_date)
                logger.debug(f"#{idx}: Skipped PDF -> {url}")
                continue

            try:
                await page.goto(url, timeout=60000)
                await asyncio.sleep(self.sleep_time)
                try:
                    desc = await page.locator('meta[name="description"]').get_attribute("content")
                    if desc:
                        item["article_description"] = desc
                except Exception:
                    pass
                item["article_tags"] = item.get("article_tags", [section])
                content_text = None
                try:
                    # Prefer data-testid article body
                    if await page.locator("div[data-testid='article-body']").count() > 0:
                        parts = await page.locator("div[data-testid='article-body'] p, div[data-testid='article-body'] h2, div[data-testid='article-body'] li").all_text_contents()
                        content_text = " ".join([p.strip() for p in parts if p and p.strip()]).strip()
                    else:

                        parts = await page.locator("div.ModularBody__ModularBodyWrapper-sc-1nacfb7-1 p, div.RTEFieldstyled__BodyWrapper-sc-1k6weum-0 p, div.RTEFieldstyled__BodyWrapper-sc-1k6weum-0 li, article p, article li").all_text_contents()
                        if parts:
                            content_text = " ".join([p.strip() for p in parts if p and p.strip()]).strip()
                        else:
                            # fallback to larger containers
                            parts = await page.locator("main p, main li, main h2").all_text_contents()
                            content_text = " ".join([p.strip() for p in parts if p and p.strip()]).strip()
                except Exception as e:
                    logger.warning(f"Content extraction warning for #{idx}: {e}")
                    content_text = None
                parsed_date = None
                try:
                    if await page.locator("time").count() > 0:
                        time_el = page.locator("time").first
                        try:
                            time_attr = await time_el.get_attribute("datetime")
                            if time_attr:
                                dt = parser.parse(time_attr, fuzzy=True)
                                parsed_date = dt.date()
                        except Exception:
                            try:
                                txt = await time_el.text_content()
                                parsed_date = _normalize_date_text(txt)
                            except Exception:
                                parsed_date = None

                    if not parsed_date:
                        try:
                            txt = await page.locator(".CardFooter__FooterLabel-sc-t8rxlh-3").first.text_content()
                            parsed_date = _normalize_date_text(txt)
                        except Exception:
                            parsed_date = None

                    if not parsed_date:
                        paras = await page.locator("p").all_text_contents()
                        mark_line = next((p.strip() for p in paras if re.search(r"\b(19|20)\d{2}\b", p)), None)
                        if mark_line:
                            parsed_date = _normalize_date_text(mark_line)

                    if not parsed_date:
                        parsed_date = self.target_date
                except Exception as e:
                    logger.warning(f"Date parse failed for article #{idx}: {e}")
                    parsed_date = self.target_date
                if parsed_date < self.target_date:
                    logger.info(f"Skipping article {item.get('article_title')} as {parsed_date} < {self.target_date}")
                    continue

                item["article_date"] = str(parsed_date)
                item["article_content"] = content_text

                logger.debug(f"Scraped article #{idx}: {item.get('article_title', '')[:60]} ({parsed_date})")

            except Exception as e:
                logger.error(f"Failed to scrape article #{idx} ({url}): {e}")

        await page.close()

async def SIMUSI(target_date):
    results = []
    try:
        url = "https://www.schroders.com/en-us/us/intermediary/insights/"
        scraper = SchrodersScraperUS(target_date)
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
    logger.info(f"Scraping Schroders insights using target date: {target_date}")
    status = asyncio.run(
        SIMUSI(
            target_date=target_date
        )
    )
    logger.info(f"Exit status: {status}")
