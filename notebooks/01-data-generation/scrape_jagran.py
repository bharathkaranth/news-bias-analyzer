"""
Jagran National News Scraper

This script scrapes article URLs from Jagran national news using their API.
API URL format: https://api.jagran.com/api/jagran/articlesbycatsubcat/news/national/{page}/{count}
Article URL format: https://www.jagran.com/news/national-{webTitleUrl}-{id}.html
"""

import requests
from bs4 import BeautifulSoup
import time
import random
import json
import os
import re
from datetime import datetime
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Base URLs
BASE_URL = "https://www.jagran.com"
API_BASE_URL = "https://api.jagran.com/api/jagran/articlesbycatsubcat"

# API Authorization Token (from user's curl)
API_TOKEN = ""

# Progress tracking
CACHE_DIR = "cache_jagran"
PROGRESS_FILE = os.path.join(CACHE_DIR, "scraping_progress.json")

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "test"
MONGO_COLLECTION = "articles"

# Parallelization Configuration
MAX_WORKERS = 5  # Number of concurrent threads for article extraction


# Initialize MongoDB connection
def get_mongo_collection():
    """Get MongoDB collection instance."""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    # Create index on URL to speed up duplicate checks
    collection.create_index("url", unique=True)
    return collection


def get_api_headers():
    """Get headers for API requests."""
    return {
        "Accept": "*/*",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Authorization": f"Bearer {API_TOKEN}",
        "Connection": "keep-alive",
        "Origin": "https://www.jagran.com",
        "Referer": "https://www.jagran.com/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
    }


def get_web_headers():
    """Get headers for web page requests."""
    return {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5,hi;q=0.3",
        "Connection": "keep-alive",
    }


def build_article_url(web_title_url, article_id):
    """
    Build article URL from API response data.

    Args:
        web_title_url (str): The webTitleUrl from API response
        article_id (str): The article id from API response

    Returns:
        str: Full article URL
    """
    return f"{BASE_URL}/news/national-{web_title_url}-{article_id}.html"


def normalize_published_date(raw_date):
    """Normalize various Jagran published_date formats to 'YYYY-MM-DD'.

    Handles examples like:
    - 'Sun, 30 Nov 2025 07:25 PM (IST)'
    - 'Sun, 30 Nov 2025 07:25 PM IST'
    - '30 Nov 2025 07:25 PM IST'
    - ISO-like strings: '2025-11-30T19:25:00+05:30'
    """

    if not raw_date:
        return raw_date

    text = str(raw_date).strip()

    # Try ISO / ISO-like formats first
    try:
        iso_candidate = text
        # Handle '2025-11-30 19:25:00+05:30' -> '2025-11-30T19:25:00+05:30'
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

    # Known Jagran textual patterns
    patterns = [
        "%a, %d %b %Y %I:%M %p (%Z)",
        "%a, %d %b %Y %I:%M %p %Z",
        "%d %b %Y %I:%M %p (%Z)",
        "%d %b %Y %I:%M %p %Z",
    ]

    for pattern in patterns:
        try:
            dt = datetime.strptime(text, pattern)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Fallback: strip timezone like '(IST)' or 'IST' and try simpler patterns
    cleaned = text.replace("(IST)", "").replace("IST", "").strip().rstrip(",")
    for pattern in ["%a, %d %b %Y %I:%M %p", "%d %b %Y %I:%M %p"]:
        try:
            dt = datetime.strptime(cleaned, pattern)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    # If nothing matched, return the original string
    return text


def extract_article_content(url):
    """
    Extract the full content of an article from Jagran.

    Args:
        url (str): Article URL

    Returns:
        dict: Dictionary containing article content and metadata
    """
    headers = get_web_headers()

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
        title_tag = soup.find("h1")
        if title_tag:
            article_data["title"] = title_tag.get_text(strip=True)

        # Extract author from author link
        author_link = soup.find("a", href=re.compile(r"/author/"))
        if author_link:
            article_data["author"] = author_link.get_text(strip=True)

        # Extract published date from meta tags
        date_meta = soup.find("meta", property="article:published_time")
        if date_meta:
            article_data["published_date"] = normalize_published_date(
                date_meta.get("content")
            )
        else:
            # Try to find date in page content
            time_tag = soup.find("time")
            if time_tag:
                raw_time = time_tag.get("datetime") or time_tag.get_text(strip=True)
                article_data["published_date"] = normalize_published_date(raw_time)

        # Try modified date
        modified_meta = soup.find("meta", property="article:modified_time")
        if modified_meta:
            article_data["modified_date"] = modified_meta.get("content")

        # Extract section/category
        section_meta = soup.find("meta", property="article:section")
        if section_meta:
            article_data["section"] = section_meta.get("content")
        else:
            # Default to national news
            article_data["section"] = "National"

        # Extract tags/keywords
        keywords_meta = soup.find("meta", attrs={"name": "keywords"})
        if keywords_meta:
            article_data["tags"] = keywords_meta.get("content")
        else:
            tag_meta = soup.find("meta", property="article:tag")
            if tag_meta:
                article_data["tags"] = tag_meta.get("content")

        # Extract article text/body
        article_body = None

        # Try different selectors for Jagran article content
        article_body = soup.find("div", class_="articleBody")
        if not article_body:
            article_body = soup.find("div", class_="article-content")
        if not article_body:
            article_body = soup.find("div", class_="story-content")
        if not article_body:
            article_body = soup.find("article")
        if not article_body:
            # Fallback: find main content area
            article_body = soup.find(
                "div", class_=re.compile(r"content|article|story", re.I)
            )

        # As a last resort, fall back to <main> or <body>
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
                    ]
                )
            ):
                element.decompose()

            # Extract paragraphs and list items first
            article_text_parts = []
            for tag_name in ["p", "li"]:
                for node in article_body.find_all(tag_name):
                    text = node.get_text(strip=True)
                    if len(text) <= 15:
                        continue
                    # Skip common unwanted phrases
                    if any(
                        skip_word in text.lower()
                        for skip_word in [
                            "advertisement",
                            "also read",
                            "read more",
                            "subscribe",
                            "follow us",
                            "download app",
                            "ये भी पढ़ें",
                            "यह भी पढ़ें",
                            "इसे भी पढ़ें",
                        ]
                    ):
                        continue
                    article_text_parts.append(text)

            # If word count still looks too small, fall back to line-based extraction
            base_word_count = len(" ".join(article_text_parts).split())
            if base_word_count < 80:
                full_text = article_body.get_text("\n", strip=True)
                for line in full_text.split("\n"):
                    line = line.strip()
                    if not line:
                        continue
                    # Skip title and very short/meta lines
                    if article_data["title"] and line == article_data["title"]:
                        continue
                    if any(
                        prefix in line
                        for prefix in [
                            "Updated:",
                            "Published:",
                            "Written by",
                            "Edited by",
                        ]
                    ):
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
                            "ये भी पढ़ें",
                            "यह भी पढ़ें",
                            "इसे भी पढ़ें",
                            "खबरें और भी",
                        ]
                    ):
                        continue
                    if len(line.split()) < 4:
                        continue
                    article_text_parts.append(line)

            # De-duplicate while preserving order
            seen_lines = set()
            deduped_parts = []
            for line in article_text_parts:
                if line not in seen_lines:
                    seen_lines.add(line)
                    deduped_parts.append(line)

            article_data["article_text"] = "\n\n".join(deduped_parts)
            article_data["word_count"] = len(article_data["article_text"].split())

        # If no article text found, try JSON-LD data
        if not article_data["article_text"] or article_data["word_count"] < 30:
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


def fetch_articles_from_api(
    page_number, count=10, category="news", subcategory="national"
):
    """
    Fetch article list from Jagran API for a specific page.

    Args:
        page_number (int): Page number to fetch
        count (int): Number of articles per page
        category (str): Category (default: news)
        subcategory (str): Subcategory (default: national)

    Returns:
        list: List of article dictionaries from API, or None if error
    """
    url = f"{API_BASE_URL}/{category}/{subcategory}/{page_number}/{count}"
    print(f"Fetching API: {url}")

    headers = get_api_headers()

    try:
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code != 200:
            print(f"API request failed - Status code: {response.status_code}")
            return None

        data = response.json()

        if not data or not isinstance(data, list):
            print(f"Empty or invalid API response on page {page_number}")
            return []

        # Process API response to build article info
        articles = []
        for item in data:
            article_id = item.get("id")
            web_title_url = item.get("webTitleUrl")

            if article_id and web_title_url:
                article_url = build_article_url(web_title_url, article_id)
                articles.append(
                    {
                        "Media Name": "DAINIK JAGRAN",
                        "Article Link": article_url,
                        "Article ID": article_id,
                        "Headline": item.get("headline"),
                        "Summary": item.get("summary"),
                        "Category": item.get("category"),
                        "Subcategory": item.get("subcategory"),
                        "State": item.get("state"),
                        "City": item.get("city"),
                        "ModDate": item.get("modDate"),
                        "Page": page_number,
                    }
                )

        print(f"Found {len(articles)} articles on page {page_number}")
        return articles

    except requests.exceptions.Timeout:
        print(f"API request timeout on page {page_number}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"API request error on page {page_number}: {str(e)}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON decode error on page {page_number}: {str(e)}")
        return None
    except Exception as e:
        print(f"Error fetching page {page_number}: {str(e)}")
        return None


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
        article_info (dict): Article metadata from API
        collection: MongoDB collection instance
        stats_lock (Lock): Thread lock for updating statistics
        stats (dict): Statistics dictionary

    Returns:
        tuple: (success, message)
    """
    article_url = article_info["Article Link"]

    try:
        print(f"  Extracting content from: {article_url}")

        # Extract article content from web page
        content = extract_article_content(article_url)

        if content["success"]:
            # Skip if word count is zero
            if content.get("word_count", 0) == 0:
                with stats_lock:
                    stats["zero_word_count_skipped"] += 1
                return (False, f"    ⚠ Skipping - Zero word count")

            # Add metadata from API response
            content["media_name"] = article_info["Media Name"]
            content["article_id"] = article_info.get("Article ID")
            content["api_headline"] = article_info.get("Headline")
            content["api_summary"] = article_info.get("Summary")
            content["state"] = article_info.get("State")
            content["city"] = article_info.get("City")
            content["scrape_page"] = article_info.get("Page")
            content["scraped_at"] = datetime.now().isoformat()

            # Use API date if web extraction failed
            if not content.get("published_date") and article_info.get("ModDate"):
                content["published_date"] = normalize_published_date(
                    article_info["ModDate"]
                )

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


def scrape_jagran_articles(
    start_page=1,
    end_page=None,
    articles_per_page=10,
    use_cache=True,
    max_workers=MAX_WORKERS,
):
    """
    Scrape articles from Jagran national news using their API.
    Uses parallel processing for article extraction.

    Args:
        start_page (int): Starting page number
        end_page (int): Ending page number (None for auto-detect)
        articles_per_page (int): Number of articles per API call
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
    if use_cache and progress["last_page"] >= start_page:
        start_page = progress["last_page"] + 1

    print(f"\nCache status: {len(completed_pages)} pages already scraped")
    if progress["last_page"]:
        print(f"Last scraped page: {progress['last_page']}")
    print(f"Using {max_workers} parallel workers for article extraction\n")

    # Get MongoDB collection
    collection = get_mongo_collection()

    # Thread lock for stats updates
    stats_lock = Lock()

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
            # Fetch articles from API
            articles = fetch_articles_from_api(
                page_number=current_page, count=articles_per_page
            )

            # Check for end of pagination or API error
            if articles is None:
                print(f"API error on page {current_page}, retrying...")
                time.sleep(5)
                articles = fetch_articles_from_api(
                    page_number=current_page, count=articles_per_page
                )
                if articles is None:
                    print(f"API still failing, skipping page {current_page}")
                    current_page += 1
                    continue

            if not articles:
                consecutive_empty += 1
                if consecutive_empty >= max_consecutive_empty:
                    print(
                        f"Stopping after {max_consecutive_empty} consecutive empty pages"
                    )
                    break
                current_page += 1
                continue

            consecutive_empty = 0
            stats["total_urls_found"] += len(articles)

            # Batch check for existing URLs
            print(f"  Checking for duplicates...")
            all_urls = [article["Article Link"] for article in articles]
            existing_urls = batch_check_existing_urls(collection, all_urls)

            # Filter out articles that already exist
            new_articles = [
                article
                for article in articles
                if article["Article Link"] not in existing_urls
            ]

            duplicates_found = len(articles) - len(new_articles)
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
    print("DAINIK JAGRAN NATIONAL NEWS SCRAPER")
    print("=" * 80)

    # Configure scraping parameters
    START_PAGE = 1
    END_PAGE = None  # Set to None for auto-detect, or a number to limit
    ARTICLES_PER_PAGE = 10
    MAX_WORKERS = 5

    print(f"\nScraping National News from Dainik Jagran")
    print(f"Starting from page: {START_PAGE}")
    print(f"End page: {'Auto-detect' if END_PAGE is None else END_PAGE}")
    print(f"Articles per page: {ARTICLES_PER_PAGE}")
    print(f"Parallel workers: {MAX_WORKERS}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Scrape articles
    stats = scrape_jagran_articles(
        start_page=START_PAGE,
        end_page=END_PAGE,
        articles_per_page=ARTICLES_PER_PAGE,
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
