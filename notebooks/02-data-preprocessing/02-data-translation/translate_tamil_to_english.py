import torch
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
from IndicTransToolkit.processor import IndicProcessor
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime
import os
import time

load_dotenv()
HF_TOKEN = os.getenv("HUGGING_FACE_HUB_TOKEN")

src_lang, tgt_lang = "tam_Taml", "eng_Latn"
model_name = "ai4bharat/indictrans2-indic-en-1B"

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "test"
SOURCE_COLLECTION = "articles"
TARGET_COLLECTION = "translate_tamil"
MEDIA_NAME = os.getenv("TAMIL_MEDIA_NAME", "CHANGE_ME_TAMIL_MEDIA")

if torch.cuda.is_available():
    DEVICE = "cuda"
elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
    DEVICE = "mps"
else:
    DEVICE = "cpu"
print(f"Device: {DEVICE}")


def get_mongo_collections():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    source = db[SOURCE_COLLECTION]
    target = db[TARGET_COLLECTION]
    target.create_index("original_id", unique=True)
    return source, target


def load_translation_model():
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
    model.eval()

    if hasattr(model.config, "use_cache"):
        model.config.use_cache = False

    ip = IndicProcessor(inference=True)
    return tokenizer, model, ip


def translate_batch(texts, tokenizer, model, ip):
    if not texts:
        return []

    batch = ip.preprocess_batch(texts, src_lang=src_lang, tgt_lang=tgt_lang)

    inputs = tokenizer(
        batch,
        truncation=True,
        padding="longest",
        return_tensors="pt",
        return_attention_mask=True,
    ).to(DEVICE)

    with torch.inference_mode():
        generated_tokens = model.generate(
            **inputs,
            use_cache=False,
            min_length=0,
            max_length=384,
            num_beams=1,
            num_return_sequences=1,
        )

    decoded = tokenizer.batch_decode(
        generated_tokens, skip_special_tokens=True, clean_up_tokenization_spaces=True
    )

    translations = ip.postprocess_batch(decoded, lang=tgt_lang)

    return translations


def benchmark_batch_sizes(batch_sizes=(2, 4, 8, 16), sample_limit=50):
    source_collection, target_collection = get_mongo_collections()
    tokenizer, model, ip = load_translation_model()

    print(
        f"Benchmarking batch sizes {batch_sizes} (up to {sample_limit} new translations each)"
    )

    for batch_size in batch_sizes:
        print("\n" + "-" * 60)
        print(f"Benchmarking batch_size={batch_size}")

        stats = {
            "processed": 0,
            "translated": 0,
            "skipped_already_done": 0,
            "skipped_no_text": 0,
            "errors": 0,
        }

        translated_ids = set(
            str(doc["original_id"])
            for doc in target_collection.find({}, {"original_id": 1})
        )

        query = {"media_name": MEDIA_NAME}
        cursor = source_collection.find(query, no_cursor_timeout=True)
        batch_articles = []

        start = time.perf_counter()
        try:
            for article in cursor:
                if stats["translated"] >= sample_limit:
                    break

                article_id = str(article["_id"])

                if article_id in translated_ids:
                    stats["skipped_already_done"] += 1
                    continue

                article_text = article.get("article_text", "") or ""
                if not article_text.strip():
                    stats["skipped_no_text"] += 1
                    continue

                batch_articles.append(article)

                if len(batch_articles) >= batch_size:
                    process_batch(
                        batch_articles, tokenizer, model, ip, target_collection, stats
                    )
                    batch_articles = []

                    if stats["translated"] >= sample_limit:
                        break
        finally:
            cursor.close()

        if batch_articles and stats["translated"] < sample_limit:
            process_batch(
                batch_articles, tokenizer, model, ip, target_collection, stats
            )

        elapsed = time.perf_counter() - start
        docs_per_sec = stats["translated"] / elapsed if elapsed > 0 else 0.0
        print(
            f"batch_size={batch_size}: {elapsed:.1f}s total, {stats['translated']} translated, {docs_per_sec:.2f} docs/s"
        )


def translate_articles_from_db(batch_size=5):
    source_collection, target_collection = get_mongo_collections()
    tokenizer, model, ip = load_translation_model()

    translated_ids = set(
        str(doc["original_id"])
        for doc in target_collection.find({}, {"original_id": 1})
    )
    print(f"Already translated: {len(translated_ids)} articles")

    query = {"media_name": MEDIA_NAME}
    total_articles = source_collection.count_documents(query)
    print(f"Total {MEDIA_NAME} articles in source: {total_articles}")

    stats = {
        "processed": 0,
        "translated": 0,
        "skipped_already_done": 0,
        "skipped_no_text": 0,
        "errors": 0,
    }

    cursor = source_collection.find(query, no_cursor_timeout=True)
    batch_articles = []

    try:
        for article in cursor:
            article_id = str(article["_id"])

            if article_id in translated_ids:
                stats["skipped_already_done"] += 1
                continue

            article_text = article.get("article_text", "") or ""
            if not article_text.strip():
                stats["skipped_no_text"] += 1
                continue

            batch_articles.append(article)

            if len(batch_articles) >= batch_size:
                process_batch(
                    batch_articles, tokenizer, model, ip, target_collection, stats
                )
                batch_articles = []
    finally:
        cursor.close()

    if batch_articles:
        process_batch(batch_articles, tokenizer, model, ip, target_collection, stats)

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
    texts_to_translate = []
    titles_to_translate = []

    for article in articles:
        texts_to_translate.append(article.get("article_text", "") or "")
        titles_to_translate.append(article.get("title", "") or "")

    try:
        print(f"\nTranslating batch of {len(articles)} articles...")
        translated_texts = translate_batch(texts_to_translate, tokenizer, model, ip)
        translated_titles = translate_batch(titles_to_translate, tokenizer, model, ip)

        for i, article in enumerate(articles):
            try:
                translated_doc = {
                    "original_id": str(article["_id"]),
                    "url": article.get("url"),
                    "original_title": article.get("title"),
                    "translated_title": (
                        translated_titles[i] if i < len(translated_titles) else None
                    ),
                    "original_text": article.get("article_text"),
                    "translated_text": (
                        translated_texts[i] if i < len(translated_texts) else None
                    ),
                    "author": article.get("author"),
                    "published_date": article.get("published_date"),
                    "section": article.get("section"),
                    "tags": article.get("tags"),
                    "media_name": article.get("media_name"),
                    "original_word_count": article.get("word_count"),
                    "translated_word_count": (
                        len(translated_texts[i].split())
                        if i < len(translated_texts)
                        else 0
                    ),
                    "translated_at": datetime.now().isoformat(),
                }

                target_collection.insert_one(translated_doc)
                stats["translated"] += 1

                title_preview = (translated_titles[i] or "N/A")[:50]
                print(f"  Translated: {title_preview}...")

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
    print("TAMIL TO ENGLISH TRANSLATOR")
    print("=" * 60)
    print(f"Source: {MONGO_DB}.{SOURCE_COLLECTION} (media_name: {MEDIA_NAME})")
    print(f"Target: {MONGO_DB}.{TARGET_COLLECTION}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    if os.getenv("RUN_TRANSLATION_BENCHMARK") == "1":
        benchmark_batch_sizes()
    else:
        translate_articles_from_db(batch_size=16)
