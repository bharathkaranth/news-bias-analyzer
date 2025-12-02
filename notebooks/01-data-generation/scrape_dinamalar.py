"""
Dinamalar Archive Article Scraper

This script scrapes article URLs from Dinamalar archive pages.
Archive URL format: https://www.dinamalar.com/archive/YYYY-Mon/DD
Example: https://www.dinamalar.com/archive/2025-Jan/02
"""

import requests
from bs4 import BeautifulSoup
import time
import random
import json
import os
import re
from datetime import datetime, timedelta
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Base URL for Dinamalar
BASE_URL = "https://www.dinamalar.com"

# Progress tracking
CACHE_DIR = "cache_dinamalar"
PROGRESS_FILE = os.path.join(CACHE_DIR, "scraping_progress.json")

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "test"
MONGO_COLLECTION = "articles"

# Parallelization Configuration
MAX_WORKERS = 5

# Month name mapping for URL construction
MONTH_NAMES = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dec",
}


def get_mongo_collection():
    """Get MongoDB collection instance."""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    # Create index on URL to speed up duplicate checks
    collection.create_index("url", unique=True)
    return collection


def build_archive_url(date):
    """
    Build archive URL for a specific date.

    Args:
        date (datetime): Date to build URL for

    Returns:
        str: Archive URL
    """
    month_name = MONTH_NAMES[date.month]
    return f"{BASE_URL}/archive/{date.year}-{month_name}/{date.day:02d}"


def extract_article_content(url):
    """
    Extract the full content of an article from Dinamalar.

    Args:
        url (str): Article URL

    Returns:
        dict: Dictionary containing article content and metadata
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5,ta;q=0.3",
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

        # Extract title from h1 tag or og:title
        title_tag = soup.find("h1")
        if title_tag:
            article_data["title"] = title_tag.get_text(strip=True)
        else:
            og_title = soup.find("meta", property="og:title")
            if og_title:
                article_data["title"] = og_title.get("content")

        # Extract author - Dinamalar typically doesn't show individual authors
        article_data["author"] = "Dinamalar"

        # Extract published date from meta tags
        date_meta = soup.find("meta", property="article:published_time")
        if date_meta:
            article_data["published_date"] = date_meta.get("content")
        else:
            # Try to find date in page content
            time_tag = soup.find("time")
            if time_tag:
                article_data["published_date"] = time_tag.get(
                    "datetime"
                ) or time_tag.get_text(strip=True)

        # If still no published date, try to infer from 'ADDED :' label in page text
        if not article_data["published_date"]:
            page_text = soup.get_text("\n", strip=True)
            for raw_line in page_text.split("\n"):
                line = raw_line.strip()
                if line.upper().startswith("ADDED :"):
                    # Store the date/time portion after 'ADDED :'
                    article_data["published_date"] = line.replace("ADDED :", "").strip()
                    break

        # Try modified date
        modified_meta = soup.find("meta", property="article:modified_time")
        if modified_meta:
            article_data["modified_date"] = modified_meta.get("content")

        # Extract section/category from URL or breadcrumb
        section_meta = soup.find("meta", property="article:section")
        if section_meta:
            article_data["section"] = section_meta.get("content")
        else:
            # Extract from URL path
            url_parts = url.split("/")
            if len(url_parts) > 4:
                # URL pattern: /news/{category}/{slug}/{id} or /videos/{category}/{slug}/{id}
                article_data["section"] = (
                    url_parts[4] if len(url_parts) > 4 else url_parts[3]
                )

        # Extract tags/keywords
        keywords_meta = soup.find("meta", attrs={"name": "keywords"})
        if keywords_meta:
            article_data["tags"] = keywords_meta.get("content")
        else:
            tag_meta = soup.find("meta", property="article:tag")
            if tag_meta:
                article_data["tags"] = tag_meta.get("content")

        # Extract article text/body
        article_text_parts = []

        # Try to find article body - Dinamalar uses various containers
        article_body = None

        # Try different selectors
        for selector in [
            ("div", {"class": "article-content"}),
            ("div", {"class": "story-content"}),
            ("div", {"class": "news-content"}),
            ("div", {"class": "video-content"}),
            ("article", {}),
            ("div", {"class": re.compile(r"content|article|story", re.I)}),
            ("div", {"id": re.compile(r"content|article|story", re.I)}),
        ]:
            article_body = soup.find(selector[0], selector[1])
            if article_body:
                break

        # As a last resort, fall back to <main> or the whole <body>
        if article_body is None:
            main_tag = soup.find("main")
            article_body = main_tag if main_tag is not None else soup.body

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

            # Remove ads and social elements
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
                        "comment",
                    ]
                )
            ):
                element.decompose()

            # First try standard paragraph tags
            paragraphs = article_body.find_all("p")
            for p in paragraphs:
                text = p.get_text(strip=True)
                if len(text) > 15:
                    # Skip unwanted phrases (Tamil and English)
                    if not any(
                        skip_word in text.lower()
                        for skip_word in [
                            "advertisement",
                            "also read",
                            "read more",
                            "subscribe",
                            "follow us",
                            "download app",
                            "மேலும் படிக்க",  # Tamil: Read more
                            "இதையும் படிக்கவும்",  # Tamil: Also read this
                        ]
                    ):
                        article_text_parts.append(text)

            def _add_lines_from_text(raw_text):
                for raw_line in raw_text.split("\n"):
                    line = raw_line.strip()
                    if not line:
                        continue
                    # Skip lines that are just the title or metadata
                    if article_data["title"] and line == article_data["title"]:
                        continue
                    if line.upper().startswith("ADDED :"):
                        continue
                    if "our apps available on" in line.lower():
                        continue
                    if any(
                        skip_word in line.lower()
                        for skip_word in [
                            "advertisement",
                            "also read",
                            "read more",
                            "subscribe",
                            "follow us",
                            "download app",
                            "மேலும் படிக்க",
                            "இதையும் படிக்கவும்",
                        ]
                    ):
                        continue
                    if len(line.split()) < 3:
                        continue
                    article_text_parts.append(line)

            # If paragraphs are missing or too short, fall back to line-based extraction
            if not article_text_parts or len(" ".join(article_text_parts).split()) < 40:
                # 1) From the detected article_body container
                full_text = article_body.get_text("\n", strip=True)
                _add_lines_from_text(full_text)

                # 2) If still short, use the page text around the 'ADDED :' marker
                if len(" ".join(article_text_parts).split()) < 40:
                    page_text = soup.get_text("\n", strip=True)
                    segment = page_text
                    if "ADDED :" in segment:
                        segment = segment.split("ADDED :", 1)[1]
                    # Cut off trailing app/promo section if present
                    marker = "Our Apps Available On"
                    if marker in segment:
                        segment = segment.split(marker, 1)[0]
                    _add_lines_from_text(segment)

        # Also try to get description/summary from og:description as an extra hint
        og_desc = soup.find("meta", property="og:description")
        if og_desc:
            desc_text = og_desc.get("content")
            if (
                desc_text
                and len(desc_text.split()) > 3
                and desc_text not in article_text_parts
                and (
                    not article_text_parts
                    or len(" ".join(article_text_parts).split()) < 40
                )
            ):
                article_text_parts.insert(0, desc_text)

        # Final fallback: extract from all paragraphs on page
        if not article_text_parts:
            all_paragraphs = soup.find_all("p")
            for p in all_paragraphs:
                text = p.get_text(strip=True)
                if len(text) > 50:  # Higher threshold for fallback
                    article_text_parts.append(text)

        article_data["article_text"] = "\n\n".join(article_text_parts)
        article_data["word_count"] = len(article_data["article_text"].split())

        # If still no content, try JSON-LD data (only when article_text is empty)
        if not article_data["article_text"]:
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
                        if (
                            not article_data["published_date"]
                            and "datePublished" in data
                        ):
                            article_data["published_date"] = data["datePublished"]
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


def scrape_dinamalar_articles_for_date(date):
    """
    Scrape article links from Dinamalar archive for a specific date.

    Args:
        date (datetime): Date to scrape

    Returns:
        list: List of dictionaries containing article information
    """
    archive_url = build_archive_url(date)
    print(f"Scraping archive: {archive_url}")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5,ta;q=0.3",
        "Referer": BASE_URL,
        "Connection": "keep-alive",
    }

    try:
        response = requests.get(archive_url, headers=headers, timeout=30)

        if response.status_code == 404:
            print(f"Archive not found for {date.strftime('%Y-%m-%d')}")
            return []

        if response.status_code != 200:
            print(f"Failed to retrieve archive - Status code: {response.status_code}")
            return []

        soup = BeautifulSoup(response.content, "html.parser")

        article_links = []
        seen_urls = set()

        # Find all article links on the archive page
        # Dinamalar news article URLs follow pattern:
        # - /news/{category}/{slug}/{id}

        for link in soup.find_all("a", href=True):
            href = link["href"]

            # Make full URL if relative
            if href.startswith("/"):
                full_link = BASE_URL + href
            elif href.startswith("http"):
                full_link = href
            else:
                continue

            # Skip non-dinamalar links
            if "dinamalar.com" not in full_link:
                continue

            # Skip archive, category, and non-article links
            if "/archive/" in full_link:
                continue
            if full_link.endswith("/"):
                # Check if it's a category page (no article ID)
                if not re.search(r"/\d+$", full_link.rstrip("/")):
                    continue

            # Match article URL patterns with numeric ID at end
            # Pattern: /news/.../{id} (news section only)
            if re.search(r"/news/[^/]+/[^/]+/\d+", full_link):
                if full_link not in seen_urls:
                    seen_urls.add(full_link)
                    article_links.append(
                        {
                            "Media Name": "DINAMALAR",
                            "Article Link": full_link,
                            "Archive Date": date.strftime("%Y-%m-%d"),
                        }
                    )

        print(f"Found {len(article_links)} articles for {date.strftime('%Y-%m-%d')}")
        return article_links

    except Exception as e:
        print(f"Error scraping archive for {date.strftime('%Y-%m-%d')}: {str(e)}")
        return []


def load_progress():
    """Load scraping progress from cache."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"completed_dates": [], "total_articles": 0}


def save_progress(completed_dates, total_articles):
    """Save scraping progress to cache."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    progress = {
        "completed_dates": completed_dates,
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
        article_info (dict): Article metadata
        collection: MongoDB collection instance
        stats_lock (Lock): Thread lock for statistics
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

            # Ensure published_date is always set, falling back to archive date
            if not content.get("published_date") and article_info.get("Archive Date"):
                content["published_date"] = article_info["Archive Date"]

            # Add metadata
            content["media_name"] = article_info["Media Name"]
            content["archive_date"] = article_info.get("Archive Date")
            content["archived_date"] = content.get("published_date") or content.get(
                "archive_date"
            )
            content["scraped_at"] = datetime.now().isoformat()

            # Insert into MongoDB
            try:
                collection.insert_one(content)
                with stats_lock:
                    stats["new_articles_added"] += 1

                title_preview = content.get("title", "N/A")[:50]
                word_count = content.get("word_count", 0)
                return (True, f"    ✓ Added - {title_preview}... ({word_count} words)")
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


def generate_date_range(start_date, end_date):
    """
    Generate a list of dates between start_date and end_date (inclusive).

    Args:
        start_date (datetime): Start date
        end_date (datetime): End date

    Returns:
        list: List of datetime objects
    """
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date)
        current_date += timedelta(days=1)
    return dates


def scrape_dinamalar_articles(
    start_date, end_date, use_cache=True, max_workers=MAX_WORKERS
):
    """
    Scrape articles from Dinamalar archive for a date range.
    Uses parallel processing for article extraction.

    Args:
        start_date (datetime): Start date
        end_date (datetime): End date
        use_cache (bool): Whether to use cache and resume from last position
        max_workers (int): Maximum number of parallel workers

    Returns:
        dict: Statistics of scraping operation
    """
    # Load progress if using cache
    progress = (
        load_progress() if use_cache else {"completed_dates": [], "total_articles": 0}
    )
    completed_dates = set(progress["completed_dates"])

    print(f"\nCache status: {len(completed_dates)} dates already scraped")
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

    # Generate date range
    dates = generate_date_range(start_date, end_date)
    print(
        f"Processing {len(dates)} dates from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
    )

    for date in dates:
        date_str = date.strftime("%Y-%m-%d")

        # Skip if already scraped
        if use_cache and date_str in completed_dates:
            print(f"Skipping {date_str} (already scraped)")
            continue

        try:
            # Get article URLs for this date
            article_urls = scrape_dinamalar_articles_for_date(date)

            if not article_urls:
                print(f"  No articles found for {date_str}")
                if use_cache:
                    completed_dates.add(date_str)
                    save_progress(list(completed_dates), stats["new_articles_added"])
                continue

            stats["total_urls_found"] += len(article_urls)

            # Batch check for existing URLs
            print(f"  Checking for duplicates...")
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
                print(f"  All articles for {date_str} already exist in database")
                if use_cache:
                    completed_dates.add(date_str)
                    save_progress(list(completed_dates), stats["new_articles_added"])
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

            # Mark date as completed
            if use_cache:
                completed_dates.add(date_str)
                save_progress(list(completed_dates), stats["new_articles_added"])

            print(
                f"  Date {date_str} completed - Total added: {stats['new_articles_added']}"
            )

        except Exception as e:
            print(f"Error on {date_str}: {e}")

        time.sleep(random.uniform(1, 2))

    return stats


def main():
    """Main execution function"""
    print("=" * 80)
    print("DINAMALAR NEWS SCRAPER")
    print("=" * 80)

    # Configure scraping parameters - date range
    # Format: Year, Month, Day
    START_DATE = datetime(2024, 1, 1)
    END_DATE = datetime(2025, 1, 31)  # Adjust as needed
    MAX_WORKERS = 5

    print(f"\nScraping Dinamalar archive")
    print(
        f"Date range: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}"
    )
    print(f"Parallel workers: {MAX_WORKERS}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Scrape articles
    stats = scrape_dinamalar_articles(
        start_date=START_DATE,
        end_date=END_DATE,
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
