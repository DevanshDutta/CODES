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
site = "Robeco"
section = "Insights"
country = "United Kingdom"
role = "Corporate"
company_site_id = "am-233"
BASE_URL = "https://www.robeco.com"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(company_site_id)

class RobecoScraper:
    def __init__(self, target_date, sleep_time=2):
        self.target_date = parser.parse(target_date, fuzzy=True).date()
        self.sleep_time = sleep_time
        self.items = []

    async def scrape(self, url):
        logger.info(f"Starting scraper for: {url}")

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
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ""AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                )
            )

            async def route_handler(route, request):
                if request.resource_type in ("image", "font", "stylesheet", "media"):
                    await route.abort()
                else:
                    await route.continue_()
            try:
                await context.route("**/*", route_handler)
            except Exception:
                pass

            page = await context.new_page()

            await page.goto(url, timeout=120000, wait_until="domcontentloaded")
            await asyncio.sleep(self.sleep_time)
            try:
                await page.locator("#attestationAccept").click(timeout=3000)
            except:
                pass
            try:
                while True:
                    try:
                        await page.wait_for_selector("span.title", timeout=10000)
                    except:
                        pass
                    anchors = await page.locator("a:has(span.title)").element_handles()
                    logger.info(f"Pagination: found {len(anchors)} anchor titles")
                    if not anchors:
                        spans = await page.locator("span.title").element_handles()
                        if not spans:
                            logger.info("No titles detected — breaking pagination")
                            break
                    date_nodes = await page.locator("time[data-testid='app-date']").all_text_contents()

                    if date_nodes:
                        last_date_raw = date_nodes[-1].strip()
                        try:
                            last_date = datetime.strptime(last_date_raw, "%d-%m-%Y").date()
                        except:
                            last_date = parser.parse(last_date_raw, fuzzy=True).date()
                    else:
                        last_date_raw = None
                        last_date = None
                    logger.info(f"Pagination last_date_raw={last_date_raw}, parsed={last_date}")
                    if last_date and last_date < self.target_date:
                        logger.info(f"STOP pagination → last_date {last_date} < target {self.target_date}")
                        break
                    try:
                        load_more_btn = page.locator("button[data-testid='app-button']:has(span.title:text('Show more'))")
                        if await load_more_btn.count() == 0:
                            load_more_btn = page.locator("button[data-testid='app-button']:has-text('Show more')")

                        if await load_more_btn.count() > 0:
                            logger.info("Clicking 'Show more'")
                            await load_more_btn.first.click(timeout=15000)
                            await asyncio.sleep(self.sleep_time)
                            continue
                        else:
                            logger.info("Scrolling bottom — trying lazy load")
                            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            await asyncio.sleep(self.sleep_time + 0.5)

                            if await page.locator("button[data-testid='app-button']:has-text('Show more')").count() > 0:
                                await page.locator("button[data-testid='app-button']:has-text('Show more')").first.click()
                                await asyncio.sleep(self.sleep_time)
                                continue
                            else:
                                logger.info("No more content to load — ending pagination")
                                break
                    except Exception as e:
                        logger.warning(f"Load-more error: {e}")
                        break
            except Exception as e:
                logger.error(f"Pagination loop failed: {e}")

            try:
                await page.wait_for_selector("span.title", timeout=8000)
            except:
                pass

            anchors = await page.locator("a:has(span.title)").element_handles()
            logger.info(f"Final collection — anchor-based cards: {len(anchors)}")

            if not anchors:
                title_spans = await page.locator("span.title").element_handles()
                title_handles = title_spans
            else:
                title_handles = anchors
            for handle in title_handles:
                container_handle = None
                try:
                    try:
                        title_text = await handle.eval_on_selector("span.title", "el => el?.textContent?.trim()")
                    except:
                        title_text = await handle.evaluate("el => el?.textContent?.trim()")
                    try:
                        href = await handle.get_attribute("href")
                    except:
                        href = None
                    if not href:
                        continue
                    if href.startswith("/"):
                        article_url = BASE_URL.rstrip("/") + href
                    else:
                        article_url = href
                    try:
                        container_handle = await handle.evaluate_handle(
                            "el => el.closest('article') || el.closest('div[class]') || el.parentElement"
                        )
                        container = container_handle.as_element()
                    except:
                        container = None
                    card_date = None
                    card_description = None
                    if container:
                        try:
                            date_raw = await container.eval_on_selector(
                                "time[data-testid='app-date']",
                                "el => el?.textContent?.trim()"
                            )
                        except:
                            date_raw = None
                        if date_raw:
                            try:
                                card_date = datetime.strptime(date_raw.strip(), "%d-%m-%Y").date()
                            except:
                                card_date = parser.parse(date_raw.strip(), fuzzy=True).date()
                        try:
                            card_description = await container.eval_on_selector(
                                "p", "el => el?.textContent?.trim()"
                            )
                        except:
                            card_description = None
                    if not card_date or card_date < self.target_date:
                        logger.debug(f"Skipping outdated card: {title_text} ({card_date})")
                        continue

                    slug = article_url.rstrip("/").split("/")[-1]

                    item = {
                        "company_site_id": company_site_id,
                        "company_site_country": country,
                        "company_site_role": role,
                        "article_source": site,
                        "article_section": section,
                        "article_date": str(card_date),
                        "article_title": title_text,
                        "article_description": card_description or None,
                        "article_content": None,
                        "article_tags": [],
                        "article_slug": slug,
                        "article_url": article_url,
                    }

                    logger.info(f"Found Title: {title_text} | Date: {card_date} | URL: {article_url}")
                    self.items.append(item)
                except Exception as e:
                    logger.error(f"Error extracting a card: {e}")
                finally:
                    try:
                        if container_handle:
                            await container_handle.dispose()
                    except:
                        pass

            await self.scrape_article_pages(context)
            await browser.close()
            return [it for it in self.items if it.get("article_content")]
        
    async def scrape_article_pages(self, context):
        logger.debug("Scraping individual articles...")
        page = await context.new_page()
        async def route_handler(route, request):
            if request.resource_type in ("image", "font", "stylesheet", "media"):
                await route.abort()
            else:
                await route.continue_()
        try:
            await context.route("**/*", route_handler)
        except:
            pass
        for idx, item in enumerate(self.items, start=1):
            url = item.get("article_url")
            if not url:
                continue
            if url.lower().endswith(".pdf"):
                item["article_content"] = url
                continue
            try:
                await page.goto(url, timeout=120000, wait_until="domcontentloaded")
                await asyncio.sleep(self.sleep_time)
                content_texts = []
                try:
                    blocks = await page.locator(
                        "div.grid-100-center, div.content, div[data-testid='foldable-wrapper']"
                    ).all_text_contents()
                    content_texts.extend([b.strip() for b in blocks if b.strip()])
                except:
                    pass
                if not content_texts:
                    try:
                        paras = await page.locator(
                            "article p, .content p, .grid-100-center p"
                        ).all_text_contents()
                        content_texts.extend([p.strip() for p in paras if p.strip()])
                    except:
                        pass
                full_text = " ".join(content_texts).strip()
                item["article_content"] = full_text
                try:
                    desc_paras = await page.locator(
                        'div[data-testid="foldable-wrapper"] p'
                    ).all_text_contents()
                    desc_paras = [p.strip() for p in desc_paras if p.strip()]
                    if desc_paras:
                        item["article_description"] = desc_paras[0]
                except:
                    pass
                try:
                    tags = await page.locator(
                        'a[data-testid="pill"]'
                    ).all_text_contents()
                    item["article_tags"] = [t.strip() for t in tags if t.strip()]
                except:
                    pass
            except Exception as e:
                logger.error(f"Failed to scrape {url}: {e}")
                item["article_content"] = None
        try:
            await page.close()
        except:
            pass

async def RobecoUKC(target_date):
    url = "https://www.robeco.com/en-uk/insights/latest-insights"
    scraper = RobecoScraper(target_date)
    results = await scraper.scrape(url)
    output_path = f"/tmp/{company_site_id}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)
    logger.info(f"Saved JSON at: {output_path}")
    logger.info(f"Scraped {len(results)} articles after {target_date}")
    return 200

if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2026-01-01"
    logger.info(f"Scraping articles using target date: {target_date}")
    asyncio.run(RobecoUKC(target_date=target_date))
