import asyncio
import json
import re
import os
import csv
import random
import datetime
import pandas as pd
import phonenumbers
import threading
import schedule
import time

from collections import Counter
from playwright.async_api import async_playwright
from tkinter import Tk, Frame, Label, Entry, Button, scrolledtext, filedialog, StringVar, ttk, messagebox, Canvas

# Google Sheets imports
import gspread
from google.oauth2.service_account import Credentials

# -------------------
# Scratch Background Canvas with Dark Grey + Scratch Lines
# -------------------
class ScratchBackground(Canvas):
    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self.configure(bg="#2e2e2e", highlightthickness=0)  # Dark grey background
        self.width = int(self["width"])
        self.height = int(self["height"])
        self.draw_scratch_lines()

    def draw_scratch_lines(self):
        import random
        line_count = 80
        for _ in range(line_count):
            x1 = random.randint(0, self.width)
            y1 = random.randint(0, self.height)
            length = random.randint(50, 200)
            angle = random.uniform(-0.5, 0.5)
            x2 = x1 + length
            y2 = y1 + int(length * angle)
            width = random.choice([1, 2, 3])
            color = "#555555"
            self.create_line(x1, y1, x2, y2, fill=color, width=width, smooth=True)

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

def is_valid_phone(phone):
    try:
        parsed = phonenumbers.parse(phone, "US")
        return phonenumbers.is_valid_number(parsed)
    except:
        return False

def is_valid_website(url):
    if url and url.startswith("http"):
        return True
    return False

def filter_data(data, name_contains=None, phone_starts=None):
    filtered = data
    if name_contains:
        filtered = [d for d in filtered if name_contains.lower() in d["Name"].lower()]
    if phone_starts:
        filtered = [d for d in filtered if d["Phone"].startswith(phone_starts)]
    return filtered

def save_data(data, filepath, formats):
    if not data:
        return
    unique_data = list({ (d["Name"], d["Phone"], d["Website"]): d for d in data }.values())
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

async def auto_scroll_page(page, listing_selector, max_attempts=50, pause_min=1.0, pause_max=2.0, log=None):
    previous_height = await page.evaluate("() => document.body.scrollHeight")
    seen_count = 0
    attempts = 0

    while attempts < max_attempts:
        await page.evaluate("() => window.scrollBy(0, window.innerHeight)")
        await asyncio.sleep(random.uniform(pause_min, pause_max))

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

def generate_email(name, company):
    return f"Dear {name},\n\nWe at {company} are excited to connect with you regarding your business. Looking forward to your response.\n\nBest regards,\nYour Team"

def generate_message(name, company):
    return f"Hi {name}, this is {company}. We would love to discuss how we can collaborate."

def upload_to_google_sheets(sheet_url, data, log, creds_path="credentials.json"):
    try:
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(sheet_url).sheet1

        headers = list(data[0].keys())
        rows = [headers] + [[d[h] for h in headers] for d in data]

        sheet.clear()
        sheet.update(rows)
        log(f"Uploaded {len(data)} rows to Google Sheets.")
    except Exception as e:
        log(f"Google Sheets upload failed: {e}")

async def scrape_google_maps(country, state, company_type, log, pause_event, progress_callback=None, proxy=None):
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

        all_divs = await page.query_selector_all("div")
        class_list = []
        for div in all_divs:
            cls = await div.get_attribute("class")
            if cls:
                class_list.append(cls)
        from collections import Counter
        class_counter = Counter(class_list)
        most_common_class, count = class_counter.most_common(1)[0]
        listing_selector = "div." + ".".join(most_common_class.split())
        log(f"Auto-detected listing selector: {listing_selector}")

        seen_count = await auto_scroll_page(page, listing_selector, max_attempts=50, pause_min=1.0, pause_max=2.0, log=log)

        for idx in range(seen_count):
            while pause_event.is_set():
                await asyncio.sleep(1)

            try:
                listings = await page.query_selector_all(listing_selector)
                if idx >= len(listings):
                    break

                await listings[idx].scroll_into_view_if_needed()
                await asyncio.sleep(0.5)

                try:
                    await listings[idx].click()
                except Exception:
                    try:
                        await listings[idx].click(force=True)
                    except Exception:
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

                phone_valid = is_valid_phone(phone)
                website_valid = is_valid_website(website)

                log(f"[{idx+1}] {name} | {phone} (valid: {phone_valid}) | {website} (valid: {website_valid})")

                emails = []
                if website_valid:
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
                    "Phone Valid": phone_valid,
                    "Website": website,
                    "Website Valid": website_valid,
                    "Emails": ", ".join(emails)
                })

                if progress_callback:
                    progress_callback(idx + 1, seen_count)

                await page.goto(search_url)
                await asyncio.sleep(3)
            except Exception as e:
                log(f"Error processing listing {idx+1}: {e}")

        await browser.close()
    return results

# -------------------
# GUI App
# -------------------
class App:
    def __init__(self, root):
        self.root = root
        self.pause_event = threading.Event()
        self.root.title("Async Google Maps Scraper")

        # Full window size
        self.root.geometry("900x700")
        self.root.resizable(False, False)

        # Scratch background canvas
        self.bg_canvas = ScratchBackground(root, width=900, height=700)
        self.bg_canvas.place(x=0, y=0)

        # Main frame over canvas with dark background
        self.main_frame = Frame(root, bg="#2e2e2e")
        self.main_frame.place(x=0, y=0, relwidth=1, relheight=1)

        self.save_dir = None

        # Input fields
        self.country_var = StringVar(value="USA")
        self.state_var = StringVar(value="Texas")
        self.company_var = StringVar(value="Salon Beauty Shop")
        self.filename_var = StringVar(value="output")
        self.proxy_var = StringVar(value="")
        self.filter_name_var = StringVar()
        self.filter_phone_var = StringVar()
        self.google_sheet_url_var = StringVar()
        self.schedule_interval_var = StringVar(value="0")  # in minutes, 0 means no schedule

        # Layout input labels and entries
        labels = [
            "Country", "State", "Company Type",
            "Base Filename", "Proxy (Optional)",
            "Filter Name Contains", "Filter Phone Starts With",
            "Google Sheets URL (Optional)",
            "Schedule Interval (min, 0=off)"
        ]
        vars_ = [
            self.country_var, self.state_var, self.company_var,
            self.filename_var, self.proxy_var,
            self.filter_name_var, self.filter_phone_var,
            self.google_sheet_url_var,
            self.schedule_interval_var
        ]

        for i, (label_text, var) in enumerate(zip(labels, vars_)):
            Label(self.main_frame, text=label_text, fg="white", bg="#2e2e2e").grid(row=i, column=0, sticky="e", padx=5, pady=2)
            Entry(self.main_frame, textvariable=var, width=40).grid(row=i, column=1, padx=5, pady=2)

        Button(self.main_frame, text="Select Save Folder", command=self.select_folder).grid(row=len(labels), column=0, columnspan=2, pady=5)

        # Buttons: Start, Pause/Resume, Export Filtered, Clear Log
        Button(self.main_frame, text="Start Scraping", command=self.start_scraping).grid(row=len(labels)+1, column=0, pady=5)
        Button(self.main_frame, text="Pause/Resume", command=self.toggle_pause).grid(row=len(labels)+1, column=1, pady=5)
        Button(self.main_frame, text="Export Filtered Data", command=self.export_filtered).grid(row=len(labels)+2, column=0, pady=5)
        Button(self.main_frame, text="Clear Log", command=self.clear_log).grid(row=len(labels)+2, column=1, pady=5)

        # Progress bar
        self.progress_var = ttk.DoubleVar()
        self.progress_bar = ttk.Progressbar(self.main_frame, maximum=100, variable=self.progress_var, length=400)
        self.progress_bar.grid(row=len(labels)+3, column=0, columnspan=2, pady=5)

        # Log area
        self.log_area = scrolledtext.ScrolledText(self.main_frame, width=110, height=20, bg="#1e1e1e", fg="white")
        self.log_area.grid(row=len(labels)+4, column=0, columnspan=2, pady=10)

        # Data storage
        self.scraped_data = []

        # Scheduler thread
        self.scheduler_thread = threading.Thread(target=self.scheduler_loop, daemon=True)
        self.scheduler_thread.start()

    def log(self, msg):
        self.log_area.insert("end", msg + "\n")
        self.log_area.see("end")
        self.log_area.update()

    def clear_log(self):
        self.log_area.delete("1.0", "end")

    def select_folder(self):
        self.save_dir = filedialog.askdirectory()
        if self.save_dir:
            self.log(f"Selected folder: {self.save_dir}")

    def toggle_pause(self):
        if self.pause_event.is_set():
            self.pause_event.clear()
            self.log("Resumed scraping.")
        else:
            self.pause_event.set()
            self.log("Paused scraping.")

    def update_progress(self, current, total):
        percent = (current / total) * 100 if total > 0 else 0
        self.progress_var.set(percent)

    def export_filtered(self):
        if not self.scraped_data:
            messagebox.showwarning("Warning", "No data to export.")
            return

        filtered = filter_data(
            self.scraped_data,
            name_contains=self.filter_name_var.get().strip() or None,
            phone_starts=self.filter_phone_var.get().strip() or None
        )

        if not filtered:
            messagebox.showinfo("Info", "No data matched the filter criteria.")
            return

        if not self.save_dir:
            messagebox.showwarning("Warning", "Select a save folder first.")
            return

        filename = timestamped_filename(self.filename_var.get() + "_filtered", "csv")
        filepath = os.path.join(self.save_dir, filename)
        save_data(filtered, filepath, formats=["csv", "json", "excel"])
        self.log(f"Filtered data saved to: {filepath}")

        # Google Sheets upload if URL provided
        gs_url = self.google_sheet_url_var.get().strip()
        if gs_url:
            upload_to_google_sheets(gs_url, filtered, self.log)

    def start_scraping(self):
        if not self.save_dir:
            self.log("Select a folder first.")
            return
        self.scraped_data.clear()
        self.progress_var.set(0)
        filename = timestamped_filename(self.filename_var.get(), "csv")
        filepath = os.path.join(self.save_dir, filename)

        def run_scraper():
            asyncio.run(self.scrape_and_save(filepath))

        threading.Thread(target=run_scraper, daemon=True).start()

    async def scrape_and_save(self, filepath):
        data = await scrape_google_maps(
            self.country_var.get(),
            self.state_var.get(),
            self.company_var.get(),
            self.log,
            self.pause_event,
            progress_callback=self.update_progress,
            proxy=self.proxy_var.get().strip() or None
        )
        self.scraped_data.extend(data)
        save_data(self.scraped_data, filepath, formats=["csv", "json", "excel"])
        self.log(f"Saved data to: {filepath}")

        # Google Sheets upload if URL provided
        gs_url = self.google_sheet_url_var.get().strip()
        if gs_url:
            upload_to_google_sheets(gs_url, self.scraped_data, self.log)

    def scheduler_loop(self):
        while True:
            try:
                interval = int(self.schedule_interval_var.get())
            except:
                interval = 0
            if interval > 0:
                self.log(f"Scheduled scraping every {interval} minutes started.")
                schedule.every(interval).minutes.do(self.scheduled_scrape)
                while True:
                    schedule.run_pending()
                    time.sleep(1)
            else:
                time.sleep(5)

    def scheduled_scrape(self):
        if not self.save_dir:
            self.log("Select a folder first before scheduled scraping.")
            return
        self.log("Starting scheduled scraping...")
        filename = timestamped_filename(self.filename_var.get(), "scheduled.csv")
        filepath = os.path.join(self.save_dir, filename)
        asyncio.run(self.scrape_and_save(filepath))


if __name__ == "__main__":
    root = Tk()
    app = App(root)
    root.mainloop()
