import os
import re

from pymongo import MongoClient


PHRASES_TO_REMOVE = [
    "1  2  3  View  all  Stories",
    "trusted news source add economic times whatsapp channel",
    "(What 's  moving  Sensex  and  Nifty  Track  latest  market  news  , stock  tips  , Budget  2025  , Share  Market  on  Budget  2025  and  expert  advice  , on  ETMarkets  . Also , ETMarkets .com  is  now  on  Telegram . For  fastest  news  alerts  on  financial  markets , investment  strategies  and  stocks  alerts , to  our  Telegram  feeds  .)",
    " Results  HPCL  Total  income  currency  refining  margin  company  hpcl  (What 's  moving  Sensex  and  Nifty  Track  latest  market  news  , stock  tips  , Budget  2025  , Share  Market  on  Budget  2025  and  expert  advice  , on  ETMarkets  . Also , ETMarkets .com  is  now  on  Telegram . For  fastest  news  alerts  on  financial  markets , investment  strategies  and  stocks  alerts , to  our  Telegram  feeds  .) to  and  read  the  Economic  Times  ePaper  Online .and  Sensex  Today  . Top  Trending  Stocks : SBI  Share  Price  , Axis  Bank  Share  Price  , HDFC  Bank  Share  Price  , Infosys  Share  Price  , Wipro  Share  Price  , NTPC  Share  Price  ... more  less  HPCL  Q  1  Results  HPCL  Total  income  currency  refining  margin  company  hpcl  (What 's  moving  Sensex  and  Nifty  Track  latest  market  news  , stock  tips  , Budget  2025  , Share  Market  on  Budget  2025  and  expert  advice  , on  ETMarkets  . Also , ETMarkets .com  is  now  on  Telegram . For  fastest  news  alerts  on  financial  markets , investment  strategies  and  stocks  alerts , to  our  Telegram  feeds  .) to  and  read  the  Economic  Times  ePaper  Online .and  Sensex  Today  . Top  Trending  Stocks : SBI  Share  Price  , Axis  Bank  Share  Price  , HDFC  Bank  Share  Price  , Infosys  Share  Price  , Wipro  Share  Price  , NTPC  Share  Price  ... more  less  Prime  Exclusives  Investment  Ideas  Stock  Report  Plus  ePaper  Wealth  Edition  GAIL  built  nation 's  gas  pipelines  for  4  decades . Now  it  is  battling  to  retain  the  edge  Nadella , Ellison , Pichai  have  all  jumped  on  the  AI  bandwagon . But  why  is  Buffett  staying  away ? Investors ' 4 -year  roller -coaster  ride  on  Paytm : How  secure  is  the  future ? As  Indian  IT  chases  the  hottest  AI  role , cost  becomes  a  question . Stock  Radar : GMR  Airports  stock  breaks  out  from  rectangular  pattern  to  hit  fresh  record  highs ; time  to  buy  or  book  profits ? Multibagger  or  IBC  - Part  33 : An  auto  ancillary  caught  between  ICE  engine  & EV  battery , will  management  be  able  to  sail  through ? 1  2  3  View  all  Stories",
    " (What 's  moving  Sensex  and  Nifty  Track  latest  market  news  , stock  tips  , Budget  2025  , Share  Market  on  Budget  2025  and  expert  advice  , on  ETMarkets  . Also , ETMarkets .com  is  now  on  Telegram . For  fastest  news  alerts  on  financial  markets , investment  strategies  and  stocks  alerts , to  our  Telegram  feeds  .) to  and  read  the  Economic  Times  ePaper  Online .and  Sensex  Today  .",
    "Top  Trending  Stocks : SBI  Share  Price  , Axis  Bank  Share  Price  , HDFC  Bank  Share  Price  , Infosys  Share  Price  , Wipro  Share  Price  , NTPC  Share  Price  ... more  less  ICICI  Pru  Life  Share  Price  ICICI  Pru  Life  BSE  NSE  Nifty  Sensex  Stocks  in  News  icici  prudential  life  insurance  company  ltd ",
    "(What 's  moving  Sensex  and  Nifty  Track  latest  market  news  , stock  tips  , Budget  2025  , Share  Market  on  Budget  2025  and  expert  advice  , on  ETMarkets  . Also , ETMarkets .com  is  now  on  Telegram . For  fastest  news  alerts  on  financial  markets , investment  strategies  and  stocks  alerts , to  our  Telegram  feeds  .)",
    "to  and  read  the  Economic  Times  ePaper  Online .and  Sensex  Today  .",
    "Top  Trending  Stocks : SBI  Share  Price  , Axis  Bank  Share  Price  , HDFC  Bank  Share  Price  , Infosys  Share  Price  , Wipro  Share  Price  , NTPC  Share  Price  ... more  less  Prime  Exclusives  Investment  Ideas  Stock  Report  Plus  ePaper  Wealth  Edition",
]


def get_collection():
    """Return the MongoDB collection containing scraped articles.

    Connection details can be overridden via environment variables:
    - MONGO_URI (default: mongodb://localhost:27017)
    - MONGO_DB_NAME (default: test) 
    - MONGO_COLLECTION_NAME (default: articles)
    """

    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    db_name = os.environ.get("MONGO_DB_NAME", "test")
    collection_name = os.environ.get("MONGO_COLLECTION_NAME", "articles")

    client = MongoClient(mongo_uri)
    db = client[db_name]
    return db[collection_name]


def build_query():
    """Build a MongoDB query that finds any article containing the phrases."""

    or_clauses = [
        {"article_text": {"$regex": re.escape(phrase)}} for phrase in PHRASES_TO_REMOVE
    ]
    return {"$or": or_clauses}


def extract_context(text: str, phrase: str, context_chars: int = 120) -> str | None:
    """Return a short context window around the first occurrence of phrase.

    The result is flattened to a single line to make terminal inspection easier.
    """

    if not text:
        return None

    idx = text.find(phrase)
    if idx == -1:
        return None

    start = max(0, idx - context_chars)
    end = min(len(text), idx + len(phrase) + context_chars)
    snippet = text[start:end]
    # Collapse all whitespace so the context is readable in logs.
    return " ".join(snippet.split())


def preview_matches(limit: int | None = 100) -> None:
    """Print all articles that currently contain the noisy phrases.

    The function only **reads** from MongoDB and does not perform any updates.
    """

    collection = get_collection()
    query = build_query()

    print("Querying for articles containing any of the configured phrases...")
    print("Number of phrases:", len(PHRASES_TO_REMOVE))
    print("Query:", query)
    print("-" * 80)

    cursor = collection.find(
        query,
        {"title": 1, "media_name": 1, "article_text": 1},
    )

    if limit is not None:
        cursor = cursor.limit(limit)

    count = 0
    for doc in cursor:
        count += 1
        print(f"Match #{count} _id={doc.get('_id')}")
        print("Title:", doc.get("title"))
        print("Media name:", doc.get("media_name"))

        article_text = doc.get("article_text", "")
        for idx, phrase in enumerate(PHRASES_TO_REMOVE, start=1):
            context = extract_context(article_text, phrase)
            if context is not None:
                print(f"  Phrase {idx} context:")
                print("  ...", context, "...")

        print("-" * 80)

    print("Total matches found:", count)


def count_matches() -> int:
    """Return the total number of articles that contain any of the phrases."""

    collection = get_collection()
    query = build_query()
    return collection.count_documents(query)


def clean_text(text: str) -> str:
    """Return article_text with the noisy boilerplate phrases removed."""

    if not text:
        return text

    cleaned = text
    for phrase in PHRASES_TO_REMOVE:
        cleaned = cleaned.replace(phrase, "")
    return cleaned.strip()


def main() -> None:
    # Step 1: Only preview the matches so you can manually verify first.
    total = count_matches()
    print("Total matches found:", total)

    collection = get_collection()
    query = build_query()
    updated = 0

    cursor = collection.find(query, {"article_text": 1})
    for doc in cursor:
        original = doc.get("article_text", "")
        cleaned = clean_text(original)
        if cleaned != original:
            collection.update_one(
                {"_id": doc["_id"]}, {"$set": {"article_text": cleaned}}
            )
            updated += 1
            if updated % 1000 == 0:
                print(f"Updated {updated} documents so far...")

    print("Total documents updated:", updated)


if __name__ == "__main__":
    main()
