import asyncio
import json
import re
import os
import csv
import time
import random
import datetime
import pandas as pd
from playwright.async_api import async_playwright
from tkinter import Tk, Frame, Label, Entry, Button, scrolledtext, filedialog, StringVar, messagebox
import threading
import phonenumbers
import schedule

# -------------------
# Configurable Selectors
# -------------------
SELECTOR_CONFIG = {
    "listing": [
        'div[role="article"]',
        '.Nv2PK',
        'div[jsaction="pane.wfvdle23"]',
        'div[aria-label][role="listitem"]'
    ],
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

def save_data(data, filepath, formats):
    if not data:
        return
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

def filter_data(data, name_contains=None, phone_starts=None):
    filtered = data
    if name_contains:
        filtered = [d for d in filtered if name_contains.lower() in d["Name"].lower()]
    if phone_starts:
        filtered = [d for d in filtered if d["Phone"].startswith(phone_starts)]
    return filtered

def is_valid_phone(phone):
    try:
        x = phonenumbers.parse(phone, None)
        return phonenumbers.is_possible_number(x)
    except:
        return False

# -------------------
# Google Sheets Upload (Optional)
# -------------------
try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials

    def upload_to_google_sheets(sheet_url, data, log_func):
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        client = gspread.authorize(creds)
        try:
            sheet = client.open_by_url(sheet_url).sheet1
        except Exception as e:
            log_func(f"Google Sheets Error: {e}")
            return

        headers = list(data[0].keys())
        rows = [headers] + [[d.get(h, "") for h in headers] for d in data]
        sheet.clear()
        sheet.update('A1', rows)
        log_func(f"Uploaded {len(data)} rows to Google Sheets.")
except ImportError:
    def upload_to_google_sheets(sheet_url, data, log_func):
        log_func("gspread package not installed, skipping Google Sheets upload.")

# -------------------
# Scroll Helper with Show More click & delay
# -------------------
async def auto_scroll_and_load(page, max_attempts=50, scroll_delay=1.0, scroll_height=1000):
    last_height = await page.evaluate("() => document.body.scrollHeight")
    attempts = 0
    while attempts < max_attempts:
        # Scroll down by scroll_height
        await page.mouse.wheel(0, scroll_height)
        await asyncio.sleep(scroll_delay)

        # Click "Show more" button if exists
        try:
            show_more = await page.query_selector('button[jsaction*="pane.paginationSection.showMore"]')
            if show_more:
                await show_more.click()
                await asyncio.sleep(scroll_delay)
        except Exception:
            pass

        new_height = await page.evaluate("() => document.body.scrollHeight")
        if new_height == last_height:
            attempts += 1
        else:
            attempts = 0
            last_height = new_height

# -------------------
# Main Scraper with User Profile logic
# -------------------
async def scrape_google_maps(country, state, company_type, log, pause_event, progress_callback=None, proxy=None):
    query = f"{company_type} {state} {country}"
    search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}?hl=en"
    results = []

    user_data_dir = get_chrome_user_data_dir()
    async with async_playwright() as p:
        browser_context = None
        if user_data_dir:
            log(f"Using Chrome user profile at: {user_data_dir}")
            try:
                browser_context = await p.chromium.launch_persistent_context(
                    user_data_dir=user_data_dir,
                    headless=False,
                    proxy={"server": proxy} if proxy else None,
                    user_agent=random.choice(USER_AGENTS),
                    locale="en-US",
                    extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
                    args=["--start-maximized"]
                )
            except Exception as e:
                log(f"Error launching with user profile: {e}")
                browser_context = None

        if browser_context is None:
            log("Launching browser without user profile.")
            browser = await p.chromium.launch(headless=False, proxy={"server": proxy} if proxy else None)
            browser_context = await browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                locale="en-US",
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"}
            )

        page = await browser_context.new_page()
        log(f"Opening: {search_url}")
        await page.goto(search_url)
        await asyncio.sleep(5)

        # Auto scroll to load listings
        await auto_scroll_and_load(page)

        listings = []
        for sel in SELECTOR_CONFIG["listing"]:
            l = await page.query_selector_all(sel)
            if l:
                listings = l
                break

        log(f"Total listings found: {len(listings)}")
        total = len(listings)

        for idx in range(total):
            while pause_event.is_set():
                await asyncio.sleep(1)

            if progress_callback:
                progress_callback(idx, total)

            try:
                # Refresh listings each loop to avoid stale handles
                listings = []
                for sel in SELECTOR_CONFIG["listing"]:
                    l = await page.query_selector_all(sel)
                    if l:
                        listings = l
                        break

                if idx >= len(listings):
                    break

                listing = listings[idx]
                await listing.scroll_into_view_if_needed()

                # Try click with retries to avoid timeout issues
                click_success = False
                for _ in range(3):
                    try:
                        await listing.click(timeout=5000)
                        click_success = True
                        break
                    except Exception:
                        await asyncio.sleep(2)

                if not click_success:
                    log(f"Skipping listing {idx+1} due to click timeout.")
                    continue

                await asyncio.sleep(random.uniform(3, 5))

                name = await try_selectors_text(page, SELECTOR_CONFIG["name"])
                address = await try_selectors_text(page, SELECTOR_CONFIG["address"])
                phone = await try_selectors_text(page, SELECTOR_CONFIG["phone"])
                website_el = await try_selectors(page, SELECTOR_CONFIG["website"])
                website = await website_el.get_attribute("href") if website_el else "N/A"

                # Validate phone
                if phone != "N/A" and not is_valid_phone(phone):
                    phone = "Invalid"

                log(f"[{idx+1}] {name} | {phone} | {website}")

                emails = []
                if website != "N/A":
                    try:
                        await page.goto(website, timeout=15000)
                        content = await page.content()
                        emails = extract_emails(content)
                        await page.go_back()
                        await asyncio.sleep(2)
                    except Exception:
                        pass

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

        if browser_context:
            await browser_context.close()
        else:
            await browser.close()

    return results

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

def get_chrome_user_data_dir():
    # Windows
    path = os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\User Data")
    if os.path.exists(path):
        return path
    # macOS
    mac_path = os.path.expanduser("~/Library/Application Support/Google/Chrome")
    if os.path.exists(mac_path):
        return mac_path
    # Linux
    linux_path = os.path.expanduser("~/.config/google-chrome")
    if os.path.exists(linux_path):
        return linux_path
    return None

# -------------------
# GUI App
# -------------------
class App:
    def __init__(self, root):
        self.root = root
        self.pause_event = threading.Event()
        self.root.title("Async Google Maps Scraper with User Profile Support")

        self.save_dir = None

        self.main_frame = Frame(root, bg="#2e2e2e")
        self.main_frame.pack(padx=10, pady=10)

        labels = ["Country", "State", "Company Type", "Base Filename", "Proxy (Optional)", "Google Sheet URL", "Schedule Interval (min)"]
        vars_ = [StringVar(value=v) for v in ["USA", "Texas", "Salon Beauty Shop", "output", "", "", "0"]]
        self.country_var, self.state_var, self.company_var, self.filename_var, self.proxy_var, self.google_sheet_url_var, self.schedule_interval_var = vars_

        for i, (label_text, var) in enumerate(zip(labels, vars_)):
            Label(self.main_frame, text=label_text, fg="white", bg="#2e2e2e").grid(row=i, column=0, sticky="e", padx=5, pady=2)
            Entry(self.main_frame, textvariable=var, width=40).grid(row=i, column=1, padx=5, pady=2)

        Button(self.main_frame, text="Select Save Folder", command=self.select_folder).grid(row=len(labels), column=0, columnspan=2, pady=5)

        Button(self.main_frame, text="Start Scraping", command=self.start_scraping).grid(row=len(labels)+1, column=0, pady=5)
        Button(self.main_frame, text="Pause/Resume", command=self.toggle_pause).grid(row=len(labels)+1, column=1, pady=5)
        Button(self.main_frame, text="Export Filtered Data", command=self.export_filtered).grid(row=len(labels)+2, column=0, pady=5)
        Button(self.main_frame, text="Clear Log", command=self.clear_log).grid(row=len(labels)+2, column=1, pady=5)

        self.progress_var = StringVar(value="0.0")
        from tkinter import ttk
        self.progress_bar = ttk.Progressbar(self.main_frame, maximum=100, variable=self.progress_var, length=400)
        self.progress_bar.grid(row=len(labels)+3, column=0, columnspan=2, pady=5)

        self.log_area = scrolledtext.ScrolledText(self.main_frame, width=110, height=20, bg="#1e1e1e", fg="white")
        self.log_area.grid(row=len(labels)+4, column=0, columnspan=2, pady=10)

        self.scraped_data = []

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
        try:
            self.progress_var.set(percent)
        except Exception:
            pass

    def export_filtered(self):
        if not self.scraped_data:
            messagebox.showwarning("Warning", "No data to export.")
            return

        filtered = filter_data(
            self.scraped_data,
            name_contains=self.country_var.get().strip() or None,
            phone_starts=self.state_var.get().strip() or None
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

        gs_url = self.google_sheet_url_var.get().strip()
        if gs_url:
            upload_to_google_sheets(gs_url, filtered, self.log)

    def start_scraping(self):
        if not self.save_dir:
            self.log("Select a folder first.")
            return
        self.scraped_data.clear()
        self.update_progress(0,1)

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

        gs_url = self.google_sheet_url_var.get().strip()
        if gs_url:
            upload_to_google_sheets(gs_url, self.scraped_data, self.log)

    def scheduler_loop(self):
        while True:
            try:
                interval = int(self.schedule_interval_var.get())
                if interval > 0:
                    schedule.every(interval).minutes.do(self.scheduled_job)
                    while True:
                        schedule.run_pending()
                        time.sleep(1)
                else:
                    time.sleep(10)
            except Exception:
                time.sleep(10)

    def scheduled_job(self):
        self.log("Scheduled scraping started.")
        if not self.save_dir:
            self.log("Select save folder before scheduled scraping.")
            return

        filename = timestamped_filename(self.filename_var.get() + "_scheduled", "csv")
        filepath = os.path.join(self.save_dir, filename)
        asyncio.run(self.scrape_and_save(filepath))

if __name__ == "__main__":
    root = Tk()
    root.geometry("1000x800")
    root.configure(bg="#2e2e2e")
    app = App(root)
    root.mainloop()
