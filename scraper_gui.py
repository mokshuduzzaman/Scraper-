import threading
import tkinter as tk
from tkinter import scrolledtext
from playwright.sync_api import sync_playwright, TimeoutError
import time
import re
import csv
import urllib.parse
import sys
import io

EMAIL_REGEX = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"

def extract_emails_from_text(text):
    return re.findall(EMAIL_REGEX, text)

def scrape_emails_from_page(page):
    content = page.content()
    emails = extract_emails_from_text(content)
    return set(emails)

def try_visit_and_scrape(page, url, log_func):
    emails = set()
    try:
        log_func(f"Visiting website: {url}\n")
        page.goto(url, timeout=30000)
        time.sleep(6)
        emails.update(scrape_emails_from_page(page))
    except Exception as e:
        log_func(f"Error visiting {url}: {e}\n")
    return emails

def find_additional_pages_and_scrape_emails(page, base_url, log_func):
    emails = set()
    possible_paths = ["/contact", "/contact-us", "/about", "/about-us", "/contactus"]
    for path in possible_paths:
        url = urllib.parse.urljoin(base_url, path)
        log_func(f"Trying additional page: {url}\n")
        emails.update(try_visit_and_scrape(page, url, log_func))
        time.sleep(4)
    return emails

def run_scraper(log_func):
    query = "salon beauty shop Texas USA"
    base_url = f"https://www.google.com/maps/search/{urllib.parse.quote(query)}"

    results = []

   with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    context = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
    )
    page = context.new_page()


        log_func(f"Loading page: {base_url}\n")
        page.goto(base_url)
        time.sleep(8)

        for _ in range(20):
            page.mouse.wheel(0, 1000)
            time.sleep(4)

        listings = page.query_selector_all('div[role="article"]')
        log_func(f"Found {len(listings)} listings\n")

        if len(listings) == 0:
            log_func("No listings found! Selector may be outdated.\n")
            page.screenshot(path="debug_no_listings.png")
            return

        for idx, listing in enumerate(listings):
            try:
                log_func(f"Processing listing {idx+1}\n")
                listing.click()
                time.sleep(6)

                name_el = page.query_selector('h1.section-hero-header-title-title span')
                name = name_el.inner_text().strip() if name_el else ""

                address_el = page.query_selector('button[data-item-id="address"] div:nth-child(2)')
                address = address_el.inner_text().strip() if address_el else ""

                phone_el = page.query_selector('button[data-item-id="phone"] div:nth-child(2)')
                phone = phone_el.inner_text().strip() if phone_el else ""

                website = ""
                website_el = page.query_selector('a[data-item-id="authority"]')
                if website_el:
                    website = website_el.get_attribute("href")

                emails = set()
                if website:
                    emails.update(scrape_emails_from_page(page))
                    emails.update(find_additional_pages_and_scrape_emails(page, website, log_func))
                    log_func(f"Emails found: {emails}\n")

                results.append({
                    "Name": name,
                    "Address": address,
                    "Phone": phone,
                    "Website": website,
                    "Emails": ", ".join(emails) if emails else "",
                })

                log_func(f"Saved: {name} | {phone} | {website}\n")
                page.go_back()
                time.sleep(6)

            except TimeoutError:
                log_func(f"Timeout at listing {idx+1}, skipping...\n")
                page.go_back()
                time.sleep(6)
                continue
            except Exception as e:
                log_func(f"Error at listing {idx+1}: {e}\n")
                page.go_back()
                time.sleep(6)
                continue

        browser.close()

    if results:
        keys = results[0].keys()
        with open("texas_salons_enhanced_emails.csv", "w", newline="", encoding="utf-8") as f:
            dict_writer = csv.DictWriter(f, keys)
            dict_writer.writeheader()
            dict_writer.writerows(results)
        log_func(f"Saved {len(results)} records with enhanced emails to texas_salons_enhanced_emails.csv\n")
    else:
        log_func("No data scraped.\n")

class App:
    def __init__(self, root):
        self.root = root
        root.title("Google Maps Salon Scraper")

        self.text_area = scrolledtext.ScrolledText(root, wrap=tk.WORD, width=100, height=30)
        self.text_area.pack(padx=10, pady=10)

        self.start_button = tk.Button(root, text="Start Scraping", command=self.start_scraping)
        self.start_button.pack(pady=5)

    def log(self, message):
        self.text_area.insert(tk.END, message)
        self.text_area.see(tk.END)
        self.text_area.update()

    def start_scraping(self):
        self.start_button.config(state=tk.DISABLED)
        self.log("Scraping started...\n")

        thread = threading.Thread(target=self.run_scraper_thread)
        thread.start()

    def run_scraper_thread(self):
        try:
            run_scraper(self.log)
        except Exception as e:
            self.log(f"Error: {e}\n")
        finally:
            self.start_button.config(state=tk.NORMAL)
            self.log("Scraping finished.\n")

if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()
