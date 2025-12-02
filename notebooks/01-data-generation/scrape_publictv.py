"""
Public TV News Scraper (Karnataka)

This script scrapes article URLs from Public TV Karnataka news pages.
Category URL format: https://publictv.in/category/states/karnataka/page/{page_number}/
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import json
import os
import re
from datetime import datetime
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Base URL for Public TV
BASE_URL = "https://publictv.in"
CATEGORY_URL = "https://publictv.in/category/states/karnataka/"

# Progress tracking
CACHE_DIR = "cache_publictv"
PROGRESS_FILE = os.path.join(CACHE_DIR, "scraping_progress.json")

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "test"
MONGO_COLLECTION = "articles"


# Initialize MongoDB connection
def get_mongo_collection():
    """Get MongoDB collection instance."""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    # Create index on URL to speed up duplicate checks
    collection.create_index("url", unique=True)
    return collection


def normalize_published_date(raw_date):
    if not raw_date:
        return raw_date

    text = str(raw_date).strip()

    # Remove leading label like 'Last updated:' if present
    text = re.sub(r"(?i)^last\s+updated:\s*", "", text)

    # Try ISO or ISO-like formats first
    try:
        iso_candidate = text
        if (
            "T" not in iso_candidate
            and " " in iso_candidate
            and "-" in iso_candidate[:10]
        ):
            iso_candidate = iso_candidate.replace(" ", "T", 1)
        if iso_candidate.endswith("Z"):
            iso_candidate = iso_candidate[:-1] + "+00:00"
        dt = datetime.fromisoformat(iso_candidate)
        return dt.date().isoformat()
    except Exception:
        pass

    # Known textual patterns like 'November 30, 2025 8:30 pm'
    patterns = [
        "%B %d, %Y %I:%M %p",
        "%b %d, %Y %I:%M %p",
        "%B %d, %Y",
        "%b %d, %Y",
    ]

    for pattern in patterns:
        try:
            dt = datetime.strptime(text, pattern)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Fallback: strip timezone tokens and try again
    cleaned = re.sub(r"\b(IST|GMT|UTC)\b", "", text).strip().rstrip(",")
    for pattern in ["%B %d %Y %I:%M %p", "%b %d %Y %I:%M %p"]:
        try:
            dt = datetime.strptime(cleaned, pattern)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return raw_date


def extract_article_content(url):
    """
    Extract the full content of an article from Public TV.

    Args:
        url (str): Article URL

    Returns:
        dict: Dictionary containing article content and metadata
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5,kn;q=0.3",
        "Connection": "keep-alive",
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code != 200:
            return {"success": False, "error": f"HTTP {response.status_code}"}

        soup = BeautifulSoup(response.content, "html.parser")

        # Initialize result dictionary
        article_data = {
            "success": True,
            "url": url,
            "title": None,
            "author": None,
            "published_date": None,
            "modified_date": None,
            "section": None,
            "tags": None,
            "article_text": None,
            "word_count": 0,
            "error": None,
        }

        # Extract title from h1 tag
        title_tag = soup.find("h1", class_="entry-title")
        if not title_tag:
            title_tag = soup.find("h1")
        if title_tag:
            article_data["title"] = title_tag.get_text(strip=True)

        # Extract author - Public TV typically uses "Public TV" as author
        author_link = soup.find("a", href=re.compile(r"/author/"))
        if author_link:
            article_data["author"] = author_link.get_text(strip=True)
        else:
            # Default author
            article_data["author"] = "Public TV"

        # Extract published date from meta tags or page content
        date_meta = soup.find("meta", property="article:published_time")
        if date_meta:
            article_data["published_date"] = normalize_published_date(
                date_meta.get("content")
            )
        else:
            # Try to find date in the page content
            time_tag = soup.find("time", class_="entry-date")
            if time_tag:
                raw_time = time_tag.get("datetime") or time_tag.get_text(strip=True)
                article_data["published_date"] = normalize_published_date(raw_time)
            else:
                # Look for "Last updated:" text
                date_text = soup.find(string=re.compile(r"Last updated:", re.I))
                if date_text:
                    article_data["published_date"] = normalize_published_date(
                        date_text.strip()
                    )

        # Try modified date
        modified_meta = soup.find("meta", property="article:modified_time")
        if modified_meta:
            article_data["modified_date"] = modified_meta.get("content")

        # Extract section/category from breadcrumb or meta
        section_meta = soup.find("meta", property="article:section")
        if section_meta:
            article_data["section"] = section_meta.get("content")
        else:
            # Try to extract from category links
            category_links = soup.find_all("a", href=re.compile(r"/category/"))
            if category_links:
                categories = [
                    cat.get_text(strip=True)
                    for cat in category_links
                    if cat.get_text(strip=True)
                ]
                article_data["section"] = ", ".join(
                    categories[:3]
                )  # Limit to first 3 categories

        # Extract tags
        tag_links = soup.find_all("a", href=re.compile(r"/tag/"))
        if tag_links:
            tags = [
                tag.get_text(strip=True)
                for tag in tag_links
                if tag.get_text(strip=True)
            ]
            article_data["tags"] = ", ".join(tags)
        else:
            # Try keywords meta
            keywords_meta = soup.find("meta", attrs={"name": "keywords"})
            if keywords_meta:
                article_data["tags"] = keywords_meta.get("content")

        # Extract article text/body
        article_body = None

        # Try different selectors for Public TV article content
        article_body = soup.find("div", class_="entry-content")
        if not article_body:
            article_body = soup.find("div", class_="post-content")
        if not article_body:
            article_body = soup.find("article")
        if not article_body:
            article_body = soup.find(
                "div", class_=re.compile(r"content|article|story", re.I)
            )

        if article_body:
            # Remove unwanted elements
            for element in article_body.find_all(
                [
                    "script",
                    "style",
                    "nav",
                    "header",
                    "footer",
                    "aside",
                    "iframe",
                    "noscript",
                ]
            ):
                element.decompose()

            # Remove ads, social sharing, and related articles
            for element in article_body.find_all(
                class_=lambda x: x
                and any(
                    keyword in str(x).lower()
                    for keyword in [
                        "ad",
                        "advertisement",
                        "promo",
                        "social",
                        "share",
                        "related",
                        "also-read",
                    ]
                )
            ):
                element.decompose()

            # Extract paragraphs
            paragraphs = article_body.find_all("p")
            article_text_parts = []
            for p in paragraphs:
                text = p.get_text(strip=True)
                if len(text) > 10:
                    # Skip common unwanted phrases
                    if not any(
                        skip_word in text.lower()
                        for skip_word in [
                            "advertisement",
                            "also read",
                            "read more",
                            "subscribe",
                            "follow us",
                            "download app",
                            "ಇದನ್ನೂ ಓದಿ",  # Kannada for "Also read"
                        ]
                    ):
                        article_text_parts.append(text)

            article_data["article_text"] = "\n\n".join(article_text_parts)
            article_data["word_count"] = len(article_data["article_text"].split())

        # If no article text found, try JSON-LD data
        if not article_data["article_text"] or article_data["word_count"] < 20:
            json_ld_scripts = soup.find_all("script", type="application/ld+json")
            for json_ld in json_ld_scripts:
                try:
                    data = json.loads(json_ld.string)
                    if isinstance(data, list):
                        data = data[0]
                    if isinstance(data, dict):
                        if "articleBody" in data:
                            article_data["article_text"] = data["articleBody"]
                            article_data["word_count"] = len(
                                article_data["article_text"].split()
                            )

                        if not article_data["title"] and "headline" in data:
                            article_data["title"] = data["headline"]
                        if not article_data["author"] and "author" in data:
                            if isinstance(data["author"], dict):
                                article_data["author"] = data["author"].get("name")
                            elif isinstance(data["author"], list):
                                article_data["author"] = ", ".join(
                                    [
                                        a.get("name", "")
                                        for a in data["author"]
                                        if isinstance(a, dict)
                                    ]
                                )
                        if (
                            not article_data["published_date"]
                            and "datePublished" in data
                        ):
                            article_data["published_date"] = normalize_published_date(
                                data["datePublished"]
                            )
                        if not article_data["modified_date"] and "dateModified" in data:
                            article_data["modified_date"] = data["dateModified"]

                        if article_data["article_text"]:
                            break
                except (json.JSONDecodeError, TypeError):
                    pass

        return article_data

    except requests.exceptions.Timeout:
        return {"success": False, "url": url, "error": "Timeout"}
    except requests.exceptions.RequestException as e:
        return {"success": False, "url": url, "error": str(e)}
    except Exception as e:
        return {"success": False, "url": url, "error": str(e)}


def scrape_publictv_articles_for_page(page_number):
    """
    Scrape article links for a specific page from Public TV Karnataka category.

    Args:
        page_number (int): Page number to scrape

    Returns:
        list: List of dictionaries containing article information
    """
    # Construct the URL for the specific page
    if page_number == 1:
        url = CATEGORY_URL
    else:
        url = f"{CATEGORY_URL}page/{page_number}/"

    print(f"Scraping URL: {url}")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5,kn;q=0.3",
        "Referer": BASE_URL,
        "Connection": "keep-alive",
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code == 404:
            print(f"Page {page_number} not found (404) - likely reached end of archive")
            return None  # Signal end of pagination

        if response.status_code != 200:
            print(
                f"Failed to retrieve data from {url} - Status code: {response.status_code}"
            )
            return []

        soup = BeautifulSoup(response.content, "html.parser")

        # Find all article links on the page
        article_links = []
        seen_urls = set()

        # Public TV article URLs follow pattern: https://publictv.in/{article-slug}/
        # They are typically in article cards or list items

        # Find all article links - they point directly to articles
        for link in soup.find_all("a", href=True):
            href = link["href"]

            # Skip if not a publictv.in link
            if not href.startswith(BASE_URL) and not href.startswith("/"):
                continue

            # Make full URL if relative
            if href.startswith("/"):
                full_link = BASE_URL + href
            else:
                full_link = href

            # Skip category, tag, author, and page links
            if "/category/" in full_link:
                continue
            if "/tag/" in full_link:
                continue
            if "/author/" in full_link:
                continue
            if "/page/" in full_link:
                continue
            if full_link == BASE_URL or full_link == BASE_URL + "/":
                continue

            # Must be a publictv.in article link (ends with / and has slug)
            # Pattern: https://publictv.in/{slug}/
            if re.match(r"https://publictv\.in/[a-z0-9-]+/$", full_link, re.I):
                if full_link not in seen_urls:
                    seen_urls.add(full_link)
                    article_links.append(
                        {
                            "Media Name": "PUBLIC TV",
                            "Article Link": full_link,
                            "Page": page_number,
                        }
                    )

        print(f"Found {len(article_links)} articles on page {page_number}")
        return article_links

    except Exception as e:
        print(f"Error scraping {url}: {str(e)}")
        return []


def load_progress():
    """Load scraping progress from cache."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"completed_pages": [], "last_page": 0, "total_articles": 0}


def save_progress(completed_pages, last_page, total_articles):
    """Save scraping progress to cache."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    progress = {
        "completed_pages": completed_pages,
        "last_page": last_page,
        "total_articles": total_articles,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def batch_check_existing_urls(collection, urls):
    """
    Check which URLs already exist in MongoDB using a single batch query.

    Args:
        collection: MongoDB collection instance
        urls (list): List of URLs to check

    Returns:
        set: Set of URLs that already exist in the database
    """
    if not urls:
        return set()

    existing_docs = collection.find({"url": {"$in": urls}}, {"url": 1})
    existing_urls = {doc["url"] for doc in existing_docs}

    return existing_urls


def process_single_article(article_info, collection, stats_lock, stats):
    """
    Process a single article: extract content and save to MongoDB.
    Thread-safe function for parallel processing.

    Args:
        article_info (dict): Article metadata (URL, page, etc.)
        collection: MongoDB collection instance
        stats_lock: Threading lock for updating stats
        stats (dict): Statistics dictionary

    Returns:
        tuple: (success, message)
    """
    article_url = article_info["Article Link"]

    try:
        print(f"  Extracting content from: {article_url}")

        # Extract article content
        content = extract_article_content(article_url)

        if content["success"]:
            # Skip if word count is zero
            if content.get("word_count", 0) == 0:
                with stats_lock:
                    stats["zero_word_count_skipped"] += 1
                return (False, f"    ⚠ Skipping - Zero word count")

            # Add metadata
            content["media_name"] = article_info["Media Name"]
            content["scrape_page"] = article_info["Page"]
            content["scraped_at"] = datetime.now().isoformat()

            # Insert into MongoDB
            try:
                collection.insert_one(content)
                with stats_lock:
                    stats["new_articles_added"] += 1

                title_preview = content.get("title", "N/A")[:60]
                word_count = content.get("word_count", 0)
                return (
                    True,
                    f"    ✓ Added - Title: {title_preview}... ({word_count} words)",
                )
            except Exception as e:
                if "duplicate key" in str(e).lower():
                    with stats_lock:
                        stats["duplicates_skipped"] += 1
                    return (False, f"    ⚠ Duplicate URL skipped")
                with stats_lock:
                    stats["extraction_failures"] += 1
                return (False, f"    ✗ MongoDB error: {str(e)}")
        else:
            with stats_lock:
                stats["extraction_failures"] += 1
            return (
                False,
                f"    ✗ Extraction failed: {content.get('error', 'Unknown')}",
            )

    except Exception as e:
        with stats_lock:
            stats["extraction_failures"] += 1
        return (False, f"    ✗ Exception: {str(e)}")


def scrape_publictv_articles(
    start_page=1, end_page=None, use_cache=True, max_workers=5
):
    """
    Scrape articles from Public TV Karnataka category pages.
    Uses parallel processing for article extraction.

    Args:
        start_page (int): Starting page number
        end_page (int): Ending page number (None for auto-detect end)
        use_cache (bool): Whether to use cache and resume from last position
        max_workers (int): Maximum number of parallel workers

    Returns:
        dict: Statistics of scraping operation
    """
    # Load progress if using cache
    progress = (
        load_progress()
        if use_cache
        else {"completed_pages": [], "last_page": 0, "total_articles": 0}
    )
    completed_pages = set(progress["completed_pages"])

    # Resume from last page if using cache
    if use_cache and progress["last_page"] > start_page:
        start_page = progress["last_page"] + 1

    print(f"\nCache status: {len(completed_pages)} pages already scraped")
    if progress["last_page"]:
        print(f"Last scraped page: {progress['last_page']}")
    print(f"Using {max_workers} parallel workers for article extraction\n")

    # Get MongoDB collection
    collection = get_mongo_collection()

    # Thread lock for stats updates
    stats_lock = threading.Lock()

    stats = {
        "total_urls_found": 0,
        "new_articles_added": 0,
        "duplicates_skipped": 0,
        "zero_word_count_skipped": 0,
        "extraction_failures": 0,
    }

    current_page = start_page
    consecutive_empty = 0
    max_consecutive_empty = 3  # Stop after 3 consecutive empty pages

    while True:
        # Check end condition
        if end_page and current_page > end_page:
            print(f"Reached end page {end_page}")
            break

        # Skip if already scraped
        if use_cache and current_page in completed_pages:
            print(f"Skipping page {current_page} (already scraped)")
            current_page += 1
            continue

        try:
            article_urls = scrape_publictv_articles_for_page(current_page)

            # Check for end of pagination
            if article_urls is None:
                print(f"Reached end of archive at page {current_page}")
                break

            if not article_urls:
                consecutive_empty += 1
                if consecutive_empty >= max_consecutive_empty:
                    print(
                        f"Stopping after {max_consecutive_empty} consecutive empty pages"
                    )
                    break
                current_page += 1
                continue

            consecutive_empty = 0
            stats["total_urls_found"] += len(article_urls)

            # Batch check for existing URLs
            print(f"  Checking for duplicates in batch...")
            all_urls = [article["Article Link"] for article in article_urls]
            existing_urls = batch_check_existing_urls(collection, all_urls)

            # Filter out articles that already exist
            new_articles = [
                article
                for article in article_urls
                if article["Article Link"] not in existing_urls
            ]

            duplicates_found = len(article_urls) - len(new_articles)
            if duplicates_found > 0:
                with stats_lock:
                    stats["duplicates_skipped"] += duplicates_found
                print(
                    f"  Found {duplicates_found} duplicates, processing {len(new_articles)} new articles"
                )

            if not new_articles:
                print(
                    f"  All articles on page {current_page} already exist in database"
                )
                if use_cache:
                    completed_pages.add(current_page)
                    save_progress(
                        list(completed_pages), current_page, stats["new_articles_added"]
                    )
                current_page += 1
                continue

            # Process articles in parallel
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_article = {
                    executor.submit(
                        process_single_article,
                        article_info,
                        collection,
                        stats_lock,
                        stats,
                    ): article_info
                    for article_info in new_articles
                }

                for future in as_completed(future_to_article):
                    try:
                        success, message = future.result()
                        print(message)
                        time.sleep(random.uniform(0.5, 1.5))
                    except Exception as e:
                        print(f"    ✗ Task exception: {str(e)}")
                        with stats_lock:
                            stats["extraction_failures"] += 1

            # Mark page as completed
            if use_cache:
                completed_pages.add(current_page)
                save_progress(
                    list(completed_pages), current_page, stats["new_articles_added"]
                )

            print(
                f"  Page {current_page} completed - Total added: {stats['new_articles_added']}"
            )

        except Exception as e:
            print(f"Error on page {current_page}: {e}")

        current_page += 1
        time.sleep(random.uniform(1, 2))

    return stats


def main():
    """Main execution function"""
    print("=" * 80)
    print("PUBLIC TV NEWS SCRAPER (Karnataka)")
    print("=" * 80)

    # Configure scraping parameters
    START_PAGE = 1
    END_PAGE = None  # Set to None for auto-detect, or a number like 5614
    MAX_WORKERS = 5

    print(f"\nScraping Karnataka news from Public TV")
    print(f"Starting from page: {START_PAGE}")
    print(f"End page: {'Auto-detect' if END_PAGE is None else END_PAGE}")
    print(f"Parallel workers: {MAX_WORKERS}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Scrape articles
    stats = scrape_publictv_articles(
        start_page=START_PAGE,
        end_page=END_PAGE,
        use_cache=True,
        max_workers=MAX_WORKERS,
    )

    # Display results
    print("\n" + "=" * 80)
    print("SCRAPING COMPLETED")
    print("=" * 80)
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    print("\nStatistics:")
    print(f"- Total URLs found: {stats['total_urls_found']}")
    print(f"- New articles added to MongoDB: {stats['new_articles_added']}")
    print(f"- Duplicates skipped: {stats['duplicates_skipped']}")
    print(f"- Zero word count skipped: {stats['zero_word_count_skipped']}")
    print(f"- Extraction failures: {stats['extraction_failures']}")
    print(f"\nProgress tracking saved in: {CACHE_DIR}/")
    print(f"MongoDB: {MONGO_URI}{MONGO_DB}/{MONGO_COLLECTION}")


if __name__ == "__main__":
    main()
