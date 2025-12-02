import pandas as pd
import holidays
import requests
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Define year range
start_year = 1995
end_year = 2025

# Initialize list to collect dates info
data = []

# Get India holidays (default is observed holidays)
india_holidays = holidays.India(years=range(start_year, end_year + 1))

session = requests.Session()
headers = {"User-Agent": "bias-detection-events-script/1.0"}
cache = {}
cache_lock = Lock()

print(f"Starting run: {start_year}-01-01 to {end_year}-12-31", flush=True)
print("Initializing cache and HTTP session", flush=True)

def classify_holiday(name: str) -> str:
    if not name:
        return ''
    n = name.lower()
    hindu_kw = [
        'diwali','deepavali','holi','pongal','navratri','dussehra','vijaya dashami','dashami',
        'ram navami','ramnavami','krishna janmashtami','janmashtami','ganesh','makar sankranti','sankranti',
        'ugadi','gudi padwa','onam','thaipusam','mahashivratri','maha shivratri','raksha bandhan','bhaidooj','bhai dooj',
        'vishu','akshaya tritiya','karva chauth','lohri'
    ]
    muslim_kw = [
        'eid','ramadan','ramzan','bakrid','eid al-adha','eid al fitr','eid-ul-fitr','eid-ul-adha',
        'muharram','milad','mawlid','id-e-milad','shab-e-barat'
    ]
    christian_kw = [
        'christmas','good friday','easter','palm sunday','ash wednesday','holy saturday','boxing day'
    ]
    sikh_kw = [
        'guru nanak','gurpurab','baisakhi','vaisakhi','guru gobind singh','guru tegh bahadur','guru arjan'
    ]
    if any(k in n for k in hindu_kw):
        return 'hindu'
    if any(k in n for k in muslim_kw):
        return 'muslim'
    if any(k in n for k in christian_kw):
        return 'christian'
    if any(k in n for k in sikh_kw):
        return 'sikh'
    general_kw = [
        'republic day','independence day','gandhi jayanti','labour day','may day','new year','teachers day','children',
        'ambedkar jayanti','maharashtra day','bihu','vesak','buddha purnima','onam','chhath'
    ]
    if any(k in n for k in general_kw):
        return 'general'
    return 'general'


def classify_event_text(text: str) -> str:
    if not text:
        return ''
    t = text.lower()
    bad_kw = [
        'war','bomb','attack','terror','killed','dead','died','massacre','disaster','earthquake','flood','tsunami',
        'hurricane','pandemic','outbreak','assassinated','assassination','murder','riot','riots','violence','explosion',
        'crash','shooting','genocide','famine','collapse','defeat','invasion','conflict','hostage','kidnapping'
    ]
    good_kw = [
        'peace','treaty','agreement','ceasefire','launch','founded','discovered','won','victory','award','nobel',
        'opens','opening','inaugurated','independence','liberation','rescue','recovered','record','milestone','landed',
        'success','first','achieved','approved','breakthrough'
    ]
    if any(k in t for k in bad_kw):
        return 'bad'
    if any(k in t for k in good_kw):
        return 'good'
    return 'neutral'

def fetch_month_day(mm: int, dd: int):
    key = (mm, dd)
    with cache_lock:
        if key in cache:
            print(f"Cache hit for {mm:02d}-{dd:02d}", flush=True)
            return cache[key]
    url = f"https://api.wikimedia.org/feed/v1/wikipedia/en/onthisday/all/{mm:02d}/{dd:02d}"
    attempts = 0
    while attempts < 5:
        try:
            print(f"Fetching events for {mm:02d}-{dd:02d}, attempt {attempts+1}", flush=True)
            resp = session.get(url, headers=headers, timeout=20)
            if resp.status_code == 429:
                sleep_s = 2 ** attempts
                print(f"Rate limited (429) for {mm:02d}-{dd:02d}, sleeping {sleep_s}s", flush=True)
                time.sleep(sleep_s)
                attempts += 1
                continue
            resp.raise_for_status()
            payload = resp.json()
            events_primary = payload.get("events", []) or []
            events_selected = payload.get("selected", []) or []
            # Merge selected into events to avoid missing curated items (e.g., multi-day attacks)
            events = events_primary + [e for e in events_selected if e not in events_primary]
            print(f"Received {len(events)} total items for {mm:02d}-{dd:02d} (events={len(events_primary)}, selected={len(events_selected)})", flush=True)
            with cache_lock:
                cache[key] = events
            time.sleep(0.05)
            return events
        except Exception as e:
            sleep_s = 2 ** attempts
            print(f"Error fetching {mm:02d}-{dd:02d} on attempt {attempts+1}: {e}. Sleeping {sleep_s}s", flush=True)
            time.sleep(sleep_s)
            attempts += 1
    print(f"Giving up on {mm:02d}-{dd:02d} after {attempts} attempts", flush=True)
    with cache_lock:
        cache[key] = []
    return []

def prefetch_events(all_dates, batch_size: int = 10, max_workers: int = 10):
    print(f"Prefetching events by month-day with batch_size={batch_size}, max_workers={max_workers}", flush=True)
    unique_md = sorted({(d.month, d.day) for d in all_dates})
    print(f"Unique month-day pairs to fetch: {len(unique_md)}", flush=True)
    for i in range(0, len(unique_md), batch_size):
        batch = unique_md[i:i+batch_size]
        print(f"Fetching batch {i//batch_size + 1}: {[f'{mm:02d}-{dd:02d}' for mm, dd in batch]}", flush=True)
        with ThreadPoolExecutor(max_workers=min(max_workers, len(batch))) as executor:
            futures = {executor.submit(fetch_month_day, mm, dd): (mm, dd) for mm, dd in batch}
            for fut in as_completed(futures):
                mm, dd = futures[fut]
                try:
                    events = fut.result()
                    print(f"Completed {mm:02d}-{dd:02d} with {len(events)} events", flush=True)
                except Exception as e:
                    print(f"Batch fetch error for {mm:02d}-{dd:02d}: {e}", flush=True)
        time.sleep(0.2)

all_dates = pd.date_range(start=f"{start_year}-01-01", end=f"{end_year}-12-31")

# Prefetch in parallel batches of 10
prefetch_events(all_dates, batch_size=20, max_workers=10)

prev_month = None
processed = 0
for date in all_dates:
    if prev_month != date.month:
        print(f"Processing month {date.strftime('%Y-%m')}...", flush=True)
        prev_month = date.month
    processed += 1
    if processed % 100 == 0:
        print(f"Processed {processed} dates so far (up to {date.date()})", flush=True)

    day_name = date.day_name()
    holiday_name = india_holidays.get(date.date())
    is_holiday = bool(holiday_name)
    mm = date.month
    dd = date.day
    yyyy = date.year

    # Read from cache (should already be populated by prefetch). If empty, retry once.
    with cache_lock:
        month_day_events = list(cache.get((mm, dd), []))
    if not month_day_events:
        print(f"Cache empty for {mm:02d}-{dd:02d}, retrying fetch once from main loop", flush=True)
        month_day_events = fetch_month_day(mm, dd)

    todays_events = []
    links = []
    sentiments = []
    for ev in month_day_events:
        try:
            if int(ev.get("year", 0)) == yyyy:
                text = ev.get("text") or ""
                todays_events.append(text)
                sentiments.append(classify_event_text(text))
                pages = ev.get("pages") or []
                if pages:
                    page = pages[0]
                    content_urls = page.get("content_urls") or {}
                    desktop = content_urls.get("desktop") or {}
                    url = desktop.get("page")
                    if url:
                        links.append(url)
        except Exception as e:
            print(f"Error parsing event for {date.date()}: {e}", flush=True)

    holiday_category = classify_holiday(holiday_name if holiday_name else '')
    good_count = sum(1 for s in sentiments if s == 'good')
    bad_count = sum(1 for s in sentiments if s == 'bad')
    overall_sent = 'bad' if bad_count > 0 else ('good' if good_count > 0 else 'neutral')

    data.append({
        'Date': date.date(),
        'Day': day_name,
        'Is_Holiday': is_holiday,
        'Holiday_Name': holiday_name if holiday_name else '',
        'Holiday_Category': holiday_category,
        'Events_Count': len(todays_events),
        'Event_Good_Count': good_count,
        'Event_Bad_Count': bad_count,
        'Event_Sentiment': overall_sent,
        'Event_Texts': ' || '.join(todays_events),
        'Event_Links': ' || '.join(links)
    })

# Create DataFrame
df = pd.DataFrame(data)
print(f"Created DataFrame with {len(df)} rows", flush=True)

# Save to CSV
csv_filename = f"india_calendar_events_{start_year}_{end_year}.csv"
df.to_csv(csv_filename, index=False)
print(f"Calendar with holidays and events saved to {csv_filename}", flush=True)