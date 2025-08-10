import asyncio
import json
import re
import os
import csv
import random
import datetime
import pandas as pd
from playwright.async_api import async_playwright
from tkinter import Tk, Frame, Label, Entry, Button, scrolledtext, filedialog, StringVar
import threading
from collections import Counter

# -------------------
# Configurable Selectors
# -------------------
SELECTOR_CONFIG = {
    "name": [
        'h1 span[aria-level="1"]',
        'h1 span',
        'h1[class*="section-hero-header-title"] span'
    ],
    "address": [
        'button[data-item-id="address"] span',
        'button[aria-label^="Address"] span'
    ],
    "phone": [
        'button[data-item-id="phone"] span',
        'button[aria-label^="Phone"] span'
    ],
    "website": [
        'a[data-item-id="authority"]',
        'a[aria-label^="Website"]'
    ]
}

# -------------------
# User-Agent Pool
# -------------------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
]

EMAIL_REGEX = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"

# -------------------
# Helper Functions
# -------------------
def extract_emails(text):
    return list(set(re.findall(EMAIL_REGEX, text)))

def timestamped_filename(base, ext):
    ts = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return f"{base}_{ts}.{ext}"

def save_data(data, filepath, formats):
    if not data:
        return
    # Remove duplicates based on Name + Website
    unique_data = list({ (d["Name"], d["Website"]): d for d in data }.values())

    base_path, _ = os.path.splitext(filepath)
    if "csv" in formats:
        with open(f"{base_path}.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=unique_data[0].keys())
            writer.writeheader()
            writer.writerows(unique_data)
    if "json" in formats:
        with open(f"{base_path}.json", "w", encoding="utf-8") as f:
            json.dump(unique_data, f, indent=2, ensure_ascii=False)
    if "excel" in formats:
        pd.DataFrame(unique_data).to_excel(f"{base_path}.xlsx", index=False)

async def try_selectors(page, selectors):
    for sel in selectors:
        try:
            el = await page.query_selector(sel)
            if el:
                return el
        except:
            continue
    return None

async def try_selectors_text(page, selectors):
    for sel in selectors:
        try:
            el = await page.query_selector(sel)
            if el:
                txt = (await el.inner_text()).strip()
                if txt:
                    return txt
        except:
            continue
    return "N/A"

# -------------------
# Improved Auto Scroll Function
# -------------------
async def auto_scroll_page(page, listing_selector, max_attempts=50, pause_min=1.0, pause_max=2.0, log=None):
    previous_height = await page.evaluate("() => document.body.scrollHeight")
    seen_count = 0
    attempts = 0

    while attempts < max_attempts:
        # Scroll down by window height
        await page.evaluate("() => window.scrollBy(0, window.innerHeight)")
        await asyncio.sleep(random.uniform(pause_min, pause_max))

        # Click "Show more" button if present
        try:
            show_more = await page.query_selector('button[jsaction*="pane.paginationSection.showMore"]')
            if show_more:
                await show_more.click()
                if log:
                    log("Clicked 'Show more' button.")
                await asyncio.sleep(random.uniform(pause_min, pause_max))
        except:
            pass

        listings = await page.query_selector_all(listing_selector)
        count = len(listings)

        current_height = await page.evaluate("() => document.body.scrollHeight")

        if count == seen_count and current_height == previous_height:
            attempts += 1
        else:
            attempts = 0
            seen_count = count
            previous_height = current_height

        if log:
            log(f"Scrolling... Listings found: {count}, Attempts: {attempts}/{max_attempts}")

    if log:
        log(f"Finished scrolling. Total listings found: {seen_count}")

    return seen_count

# -------------------
# Main Scraper with Auto-detect Listing Selector & Improved Scroll
# -------------------
async def scrape_google_maps(country, state, company_type, log, pause_event, proxy=None):
    query = f"{company_type} {state} {country}"
    search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}?hl=en"
    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, proxy={"server": proxy} if proxy else None)
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            locale="en-US",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"}
        )
        page = await context.new_page()
        log(f"Opening: {search_url}")
        await page.goto(search_url)
        await asyncio.sleep(5)

        # Auto-detect listing selector
        all_divs = await page.query_selector_all("div")
        class_list = []
        for div in all_divs:
            cls = await div.get_attribute("class")
            if cls:
                class_list.append(cls)
        class_counter = Counter(class_list)
        most_common_class, count = class_counter.most_common(1)[0]
        listing_selector = "div." + ".".join(most_common_class.split())
        log(f"Auto-detected listing selector: {listing_selector}")

        # Auto scroll page to load listings
        seen_count = await auto_scroll_page(
            page,
            listing_selector,
            max_attempts=50,
            pause_min=1.0,
            pause_max=2.0,
            log=log
        )

        for idx in range(seen_count):
            while pause_event.is_set():
                await asyncio.sleep(1)

            try:
                listings = await page.query_selector_all(listing_selector)
                if idx >= len(listings):
                    break

                await listings[idx].scroll_into_view_if_needed()
                await asyncio.sleep(0.5)  # wait scroll complete

                # Try clicking safely
                try:
                    await listings[idx].click()
                except Exception:
                    # Normal click fail হলে force click চেষ্টা করো
                    try:
                        await listings[idx].click(force=True)
                    except Exception:
                        # যদি তাও না হয়, মাউস দিয়ে ক্লিক করো (backup)
                        box = await listings[idx].bounding_box()
                        if box:
                            await page.mouse.click(box["x"] + box["width"]/2, box["y"] + box["height"]/2)
                        else:
                            raise

                await asyncio.sleep(random.uniform(3, 5))

                name = await try_selectors_text(page, SELECTOR_CONFIG["name"])
                address = await try_selectors_text(page, SELECTOR_CONFIG["address"])
                phone = await try_selectors_text(page, SELECTOR_CONFIG["phone"])
                website_el = await try_selectors(page, SELECTOR_CONFIG["website"])
                website = await website_el.get_attribute("href") if website_el else "N/A"

                log(f"[{idx+1}] {name} | {phone} | {website}")

                emails = []
                if website != "N/A":
                    try:
                        await page.goto(website, timeout=15000)
                        content = await page.content()
                        emails = extract_emails(content)
                        await page.go_back()
                        await asyncio.sleep(2)
                    except Exception as e:
                        log(f"Error fetching website emails: {e}")

                results.append({
                    "Name": name,
                    "Address": address,
                    "Phone": phone,
                    "Website": website,
                    "Emails": ", ".join(emails)
                })

                await page.goto(search_url)
                await asyncio.sleep(3)
            except Exception as e:
                log(f"Error processing listing {idx+1}: {e}")
                continue

        await browser.close()
    return results

# -------------------
# GUI App
# -------------------
class App:
    def __init__(self, root):
        self.root = root
        self.pause_event = threading.Event()
        self.root.title("Async Google Maps Scraper with Auto-detect Selector")
        self.save_dir = None

        f = Frame(root)
        f.pack(padx=10, pady=10)
        self.country_var = StringVar(value="USA")
        self.state_var = StringVar(value="Texas")
        self.company_var = StringVar(value="Salon Beauty Shop")
        self.filename_var = StringVar(value="output")
        self.proxy_var = StringVar(value="")

        for i, (label, var) in enumerate([
            ("Country", self.country_var),
            ("State", self.state_var),
            ("Company Type", self.company_var),
            ("Base Filename", self.filename_var),
            ("Proxy (Optional)", self.proxy_var)
        ]):
            Label(f, text=label).grid(row=i, column=0, sticky="e", pady=2)
            Entry(f, textvariable=var, width=40).grid(row=i, column=1, pady=2)

        Button(f, text="Select Save Folder", command=self.select_folder).grid(row=5, column=0, columnspan=2, pady=5)

        Button(f, text="Start", command=self.start_scraping).grid(row=6, column=0, pady=5)
        Button(f, text="Pause/Resume", command=self.toggle_pause).grid(row=6, column=1, pady=5)

        self.log_area = scrolledtext.ScrolledText(root, width=100, height=25)
        self.log_area.pack(padx=10, pady=10)

    def log(self, msg):
        self.log_area.insert("end", msg + "\n")
        self.log_area.see("end")
        self.log_area.update()

    def select_folder(self):
        self.save_dir = filedialog.askdirectory()
        if self.save_dir:
            self.log(f"Save folder selected: {self.save_dir}")

    def toggle_pause(self):
        if self.pause_event.is_set():
            self.pause_event.clear()
            self.log("Resumed scraping.")
        else:
            self.pause_event.set()
            self.log("Paused scraping.")

    def start_scraping(self):
        if not self.save_dir:
            self.log("Select a folder first.")
            return
        filename = timestamped_filename(self.filename_var.get(), "csv")
        filepath = os.path.join(self.save_dir, filename)

        threading.Thread(
            target=lambda: asyncio.run(self.scrape_and_save(filepath)),
            daemon=True
        ).start()

    async def scrape_and_save(self, filepath):
        results = await scrape_google_maps(
            self.country_var.get(),
            self.state_var.get(),
            self.company_var.get(),
            self.log,
            self.pause_event,
            proxy=self.proxy_var.get().strip() or None
        )
        save_data(results, filepath, formats=["csv", "json", "excel"])
        self.log(f"Saved data to: {filepath}")

if __name__ == "__main__":
    root = Tk()
    app = App(root)
    root.geometry("900x700")
    root.mainloop()
