from playwright.sync_api import sync_playwright, TimeoutError
import csv
import time
import re
import urllib.parse

EMAIL_REGEX = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"

def extract_emails_from_text(text):
    return re.findall(EMAIL_REGEX, text)

def scrape_emails_from_page(page):
    content = page.content()
    emails = extract_emails_from_text(content)
    return set(emails)

def try_visit_and_scrape(page, url):
    emails = set()
    try:
        page.goto(url, timeout=30000)
        time.sleep(6)  # ওয়েবসাইট লোডের জন্য অপেক্ষা
        emails.update(scrape_emails_from_page(page))
    except Exception as e:
        print(f"Error visiting {url}: {e}")
    return emails

def find_additional_pages_and_scrape_emails(page, base_url):
    emails = set()
    possible_paths = ["/contact", "/contact-us", "/about", "/about-us", "/contactus"]
    
    for path in possible_paths:
        url = urllib.parse.urljoin(base_url, path)
        print(f"Trying to scrape additional page: {url}")
        emails.update(try_visit_and_scrape(page, url))
        time.sleep(4)
    return emails

def scrape_texas_salons_with_enhanced_emails():
    query = "salon beauty shop Texas USA"
    base_url = f"https://www.google.com/maps/search/{urllib.parse.quote(query)}"

    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.set_user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36")

        page.goto(base_url)
        time.sleep(8)

        for _ in range(20):
            page.mouse.wheel(0, 1000)
            time.sleep(4)

        listings = page.query_selector_all('div[role="article"]')
        print(f"Found {len(listings)} listings")

        for idx, listing in enumerate(listings):
            try:
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
                    emails.update(find_additional_pages_and_scrape_emails(page, website))
                    print(f"Emails found on {website} and subpages: {emails}")

                results.append({
                    "Name": name,
                    "Address": address,
                    "Phone": phone,
                    "Website": website,
                    "Emails": ", ".join(emails) if emails else "",
                })

                print(f"{idx+1}. {name} | {phone} | {website} | Emails: {emails}")

                page.go_back()
                time.sleep(6)

            except TimeoutError:
                print(f"Timeout at listing {idx+1}, skipping...")
                page.go_back()
                time.sleep(6)
                continue
            except Exception as e:
                print(f"Error at listing {idx+1}: {e}")
                page.go_back()
                time.sleep(6)
                continue

        browser.close()

    keys = results[0].keys() if results else []
    with open("texas_salons_enhanced_emails.csv", "w", newline="", encoding="utf-8") as f:
        dict_writer = csv.DictWriter(f, keys)
        dict_writer.writeheader()
        dict_writer.writerows(results)

    print(f"Saved {len(results)} records with enhanced emails to texas_salons_enhanced_emails.csv")

if __name__ == "__main__":
    scrape_texas_salons_with_enhanced_emails()
