import threading
import tkinter as tk
from tkinter import scrolledtext
from playwright.sync_api import sync_playwright
import time
import re
import csv
import urllib.parse

EMAIL_REGEX = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"

def extract_emails_from_text(text):
    return re.findall(EMAIL_REGEX, text)

def scrape_emails_from_page(page):
    content = page.content()
    emails = extract_emails_from_text(content)
    return set(emails)

def try_multiple_selectors(page, selectors, log_func):
    for sel in selectors:
        elems = page.query_selector_all(sel)
        if elems and len(elems) > 0:
            log_func(f"Using selector: {sel} (found {len(elems)} elements)")
            return elems
    log_func("No listings found with any tested selectors.")
    return []

def scrape_google_maps(log_func, country, state, company_type):
    query = f"{company_type} {state} {country}"
    search_url = f"https://www.google.com/maps/search/{urllib.parse.quote(query)}"

    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        log_func(f"Loading Google Maps: {search_url}")
        page.goto(search_url)
        time.sleep(8)

        log_func("Scrolling to load listings...")
        for _ in range(15):
            page.mouse.wheel(0, 1000)
            time.sleep(3)

        possible_selectors = [
            'div[role="article"]',
            'div[jsaction="pane.wfvdle23"]',
            'div[aria-label][role="listitem"]',
            '.Nv2PK',
            '.section-result',
            'div.section-result-content',
        ]

        listings = try_multiple_selectors(page, possible_selectors, log_func)
        log_func(f"Total listings found: {len(listings)}")

        if not listings:
            log_func("No listings found. Exiting scraper.")
            page.screenshot(path="debug_no_listings.png")
            browser.close()
            return

        for idx in range(len(listings)):
            try:
                # fresh listings নাও প্রতি লুপে
                listings = try_multiple_selectors(page, possible_selectors, log_func)
                listing = listings[idx]

                log_func(f"\nProcessing listing {idx + 1} of {len(listings)}")
                listing.click()
                time.sleep(6)

                def get_inner_text(selector_list):
                    for sel in selector_list:
                        el = page.query_selector(sel)
                        if el:
                            text = el.inner_text().strip()
                            if text:
                                return text
                    return "N/A"

                name = get_inner_text(['h1.section-hero-header-title-title span', 'h1 span'])
                address = get_inner_text(['button[data-item-id="address"] div:nth-child(2)', 
                                          'button[data-item-id="address"] span', 
                                          'span.section-info-text'])
                phone = get_inner_text(['button[data-item-id="phone"] div:nth-child(2)', 
                                        'button[data-item-id="phone"] span'])
                website_el = page.query_selector('a[data-item-id="authority"]')
                website = website_el.get_attribute("href") if website_el else ""

                log_func(f"Name: {name}")
                log_func(f"Address: {address}")
                log_func(f"Phone: {phone}")
                log_func(f"Website: {website if website else 'N/A'}")

                emails = set()
                if website:
                    emails.update(scrape_emails_from_page(page))

                results.append({
                    "Name": name,
                    "Address": address,
                    "Phone": phone,
                    "Website": website,
                    "Emails": ", ".join(emails) if emails else "",
                })

                log_func("Entry saved.")

                # go_back বাদ, মূল সার্চ পেজে পুনরায় যাও
                log_func("Returning to main search page...")
                page.goto(search_url)
                time.sleep(8)

                log_func("Scrolling again to reload listings...")
                for _ in range(10):
                    page.mouse.wheel(0, 1000)
                    time.sleep(2)

            except Exception as e:
                log_func(f"Error processing listing {idx + 1}: {e}")
                page.goto(search_url)
                time.sleep(8)
                continue

        browser.close()

    if results:
        keys = results[0].keys()
        filename = f"{company_type}_{state}_{country}_salons.csv".replace(" ", "_")
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, keys)
            writer.writeheader()
            writer.writerows(results)
        log_func(f"Saved {len(results)} records to {filename}")
    else:
        log_func("No data scraped.")

class App:
    def __init__(self, root):
        self.root = root
        root.title("Google Maps Scraper with Reliable Navigation")

        input_frame = tk.Frame(root)
        input_frame.pack(padx=10, pady=5)

        tk.Label(input_frame, text="Country:").grid(row=0, column=0, sticky="e")
        self.country_entry = tk.Entry(input_frame, width=30)
        self.country_entry.grid(row=0, column=1, padx=5)
        self.country_entry.insert(0, "USA")

        tk.Label(input_frame, text="State:").grid(row=1, column=0, sticky="e")
        self.state_entry = tk.Entry(input_frame, width=30)
        self.state_entry.grid(row=1, column=1, padx=5)
        self.state_entry.insert(0, "Texas")

        tk.Label(input_frame, text="Company Type:").grid(row=2, column=0, sticky="e")
        self.company_entry = tk.Entry(input_frame, width=30)
        self.company_entry.grid(row=2, column=1, padx=5)
        self.company_entry.insert(0, "salon beauty shop")

        self.start_button = tk.Button(root, text="Start Scraping", command=self.start_scraping)
        self.start_button.pack(pady=5)

        self.log_area = scrolledtext.ScrolledText(root, wrap=tk.WORD, width=100, height=25)
        self.log_area.pack(padx=10, pady=10)

    def log(self, message):
        self.log_area.insert(tk.END, message + "\n")
        self.log_area.see(tk.END)
        self.log_area.update()

    def start_scraping(self):
        country = self.country_entry.get().strip()
        state = self.state_entry.get().strip()
        company_type = self.company_entry.get().strip()

        if not country or not state or not company_type:
            self.log("Please fill all input fields.")
            return

        self.start_button.config(state=tk.DISABLED)
        self.log_area.delete(1.0, tk.END)
        self.log(f"Scraping started for: {company_type}, {state}, {country}")

        threading.Thread(target=self.scrape_thread, args=(country, state, company_type), daemon=True).start()

    def scrape_thread(self, country, state, company_type):
        try:
            scrape_google_maps(self.log, country, state, company_type)
        except Exception as e:
            self.log(f"Error: {e}")
        finally:
            self.start_button.config(state=tk.NORMAL)
            self.log("\nScraping finished.")

if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()
