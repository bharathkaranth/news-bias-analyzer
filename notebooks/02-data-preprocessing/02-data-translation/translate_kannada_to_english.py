import torch
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
from IndicTransToolkit.processor import IndicProcessor
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime
import os

# Load environment
load_dotenv()
HF_TOKEN = os.getenv("HUGGING_FACE_HUB_TOKEN")

# IndicTrans2 uses specific language tags
src_lang, tgt_lang = "kan_Knda", "eng_Latn"
model_name = "ai4bharat/indictrans2-indic-en-1B"

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "test"
SOURCE_COLLECTION = "articles"
TARGET_COLLECTION = "translate_kannada"

# Device selection
if torch.cuda.is_available():
    DEVICE = "cuda"
elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
    DEVICE = "mps"
else:
    DEVICE = "cpu"
print(f"Device: {DEVICE}")


def get_mongo_collections():
    """Get MongoDB source and target collection instances."""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    source = db[SOURCE_COLLECTION]
    target = db[TARGET_COLLECTION]
    # Create index on original_id to avoid duplicates
    target.create_index("original_id", unique=True)
    return source, target


def load_translation_model():
    """
    Load IndicTrans2 model, tokenizer, and processor.
    """
    print("Loading IndicTrans2 model...")
    tokenizer = AutoTokenizer.from_pretrained(
        model_name, trust_remote_code=True, token=HF_TOKEN
    )
    model = AutoModelForSeq2SeqLM.from_pretrained(
        model_name,
        trust_remote_code=True,
        token=HF_TOKEN,
        torch_dtype=torch.float16 if DEVICE == "cuda" else torch.float32,
    )
    model.to(DEVICE)

    # Disable cache to avoid past_key_values bug
    if hasattr(model.config, "use_cache"):
        model.config.use_cache = False

    ip = IndicProcessor(inference=True)
    print("Model loaded successfully!")
    return tokenizer, model, ip


def translate_batch(texts, tokenizer, model, ip):
    """
    Translate a batch of Kannada texts to English.

    Args:
        texts (list): List of Kannada text strings
        tokenizer: HuggingFace tokenizer
        model: IndicTrans2 model
        ip: IndicProcessor instance

    Returns:
        list: List of English translations
    """
    if not texts:
        return []

    # Preprocess with IndicProcessor
    batch = ip.preprocess_batch(texts, src_lang=src_lang, tgt_lang=tgt_lang)

    # Tokenize
    inputs = tokenizer(
        batch,
        truncation=True,
        padding="longest",
        return_tensors="pt",
        return_attention_mask=True,
    ).to(DEVICE)

    # Generate translation
    with torch.inference_mode():
        generated_tokens = model.generate(
            **inputs,
            use_cache=False,
            min_length=0,
            max_length=384,
            num_beams=1,
            num_return_sequences=1,
        )

    # Decode tokens
    decoded = tokenizer.batch_decode(
        generated_tokens, skip_special_tokens=True, clean_up_tokenization_spaces=True
    )

    # Postprocess with IndicProcessor
    translations = ip.postprocess_batch(decoded, lang=tgt_lang)

    return translations


def translate_articles_from_db(batch_size=5):
    """
    Pull articles from MongoDB, translate Kannada to English,
    and save to translate_kannada collection.

    Args:
        batch_size (int): Number of articles to process in each batch
    """
    source_collection, target_collection = get_mongo_collections()
    tokenizer, model, ip = load_translation_model()

    # Get already translated article IDs
    translated_ids = {
        str(doc["original_id"])
        for doc in target_collection.find(
            {"original_id": {"$exists": True}}, {"original_id": 1}
        )
    }
    print(f"Already translated: {len(translated_ids)} articles")

    # Query PUBLIC TV articles that haven't been translated yet
    query = {"media_name": "PUBLIC TV"}
    total_articles = source_collection.count_documents(query)
    print(f"Total PUBLIC TV articles in source: {total_articles}")

    # Stats
    stats = {
        "processed": 0,
        "translated": 0,
        "skipped_already_done": 0,
        "skipped_no_text": 0,
        "errors": 0,
    }

    # Process articles in batches
    cursor = source_collection.find(query, no_cursor_timeout=True)
    batch_articles = []

    try:
        for article in cursor:
            article_id = str(article["_id"])

            # Skip if already translated
            if article_id in translated_ids:
                stats["skipped_already_done"] += 1
                continue

            # Skip if no article text
            article_text = article.get("article_text", "") or ""
            if not article_text.strip():
                stats["skipped_no_text"] += 1
                continue

            batch_articles.append(article)

            # Process batch when full
            if len(batch_articles) >= batch_size:
                process_batch(
                    batch_articles, tokenizer, model, ip, target_collection, stats
                )
                batch_articles = []
    finally:
        cursor.close()

    # Process remaining articles
    if batch_articles:
        process_batch(batch_articles, tokenizer, model, ip, target_collection, stats)

    # Print final stats
    print("\n" + "=" * 60)
    print("TRANSLATION COMPLETED")
    print("=" * 60)
    print(f"Processed: {stats['processed']}")
    print(f"Translated: {stats['translated']}")
    print(f"Skipped (already done): {stats['skipped_already_done']}")
    print(f"Skipped (no text): {stats['skipped_no_text']}")
    print(f"Errors: {stats['errors']}")

    return stats


def process_batch(articles, tokenizer, model, ip, target_collection, stats):
    """
    Process a batch of articles: translate and save to target collection.
    """
    batch_ids = [str(a["_id"]) for a in articles]
    existing_ids = {
        str(doc["original_id"])
        for doc in target_collection.find(
            {"original_id": {"$in": batch_ids}}, {"original_id": 1}
        )
    }
    duplicates_in_batch = len(existing_ids)
    articles = [a for a in articles if str(a["_id"]) not in existing_ids]
    if not articles:
        stats["skipped_already_done"] += duplicates_in_batch
        return
    if duplicates_in_batch:
        stats["skipped_already_done"] += duplicates_in_batch

    # Extract texts to translate
    texts_to_translate = []
    titles_to_translate = []

    for article in articles:
        texts_to_translate.append(article.get("article_text", "") or "")
        titles_to_translate.append(article.get("title", "") or "")

    try:
        # Translate article texts
        print(f"\nTranslating batch of {len(articles)} articles...")
        translated_texts = translate_batch(texts_to_translate, tokenizer, model, ip)
        translated_titles = translate_batch(titles_to_translate, tokenizer, model, ip)

        # Save each translated article
        for i, article in enumerate(articles):
            try:
                translated_doc = {
                    "original_id": str(article["_id"]),
                    "url": article.get("url"),
                    "original_title": article.get("title"),
                    "translated_title": translated_titles[i] if i < len(translated_titles) else None,
                    "original_text": article.get("article_text"),
                    "translated_text": translated_texts[i] if i < len(translated_texts) else None,
                    "author": article.get("author"),
                    "published_date": article.get("published_date"),
                    "section": article.get("section"),
                    "tags": article.get("tags"),
                    "media_name": article.get("media_name"),
                    "original_word_count": article.get("word_count"),
                    "translated_word_count": len(translated_texts[i].split()) if i < len(translated_texts) else 0,
                    "translated_at": datetime.now().isoformat(),
                }

                target_collection.insert_one(translated_doc)
                stats["translated"] += 1

                # Print progress
                title_preview = (translated_titles[i] or "N/A")[:50]
                print(f"  ✓ Translated: {title_preview}...")

            except Exception as e:
                if "duplicate key" in str(e).lower():
                    stats["skipped_already_done"] += 1
                else:
                    stats["errors"] += 1
                    print(f"  ✗ Error saving article: {e}")

        stats["processed"] += len(articles)

    except Exception as e:
        stats["errors"] += len(articles)
        print(f"  ✗ Batch translation error: {e}")


if __name__ == "__main__":
    print("=" * 60)
    print("KANNADA TO ENGLISH TRANSLATOR")
    print("=" * 60)
    print(f"Source: {MONGO_DB}.{SOURCE_COLLECTION} (media_name: PUBLIC TV)")
    print(f"Target: {MONGO_DB}.{TARGET_COLLECTION}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    translate_articles_from_db(batch_size=2)
