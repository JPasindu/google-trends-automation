import time
import logging
import random
import sys
import pandas as pd
from datetime import datetime
from pytrends.request import TrendReq
from pathlib import Path
import os

# ---------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(DATA_DIR / "script_log.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------
def load_terms_queue():
    queue_file = DATA_DIR / "terms_queue.csv"
    if queue_file.exists():
        df = pd.read_csv(queue_file)
        terms = df['term'].dropna().tolist()
        logger.info(f"📥 Loaded {len(terms)} terms from queue")
    else:
        logger.error("❌ Queue file not found.")
        terms = []
    return terms

def update_queue(remaining_terms):
    df = pd.DataFrame({'term': remaining_terms})
    df.to_csv(DATA_DIR / "terms_queue.csv", index=False)
    logger.info(f"🗂️ Queue updated. Remaining: {len(remaining_terms)} terms")

def append_processed(term, status):
    df = pd.DataFrame([[term, status, datetime.now()]],
                      columns=['term', 'status', 'timestamp'])
    processed_file = DATA_DIR / "processed_terms.csv"
    header = not processed_file.exists()
    df.to_csv(processed_file, mode='a', header=header, index=False)
    logger.info(f"✅ Logged {term} as {status}")

# ---------------------------------------------------------------------
# Trend Fetching Logic
# ---------------------------------------------------------------------
def fetch_trend_data(term, pytrends):
    try:
        pytrends.build_payload([term], timeframe='now 7-d')
        data = pytrends.interest_over_time()
        if data.empty:
            logger.warning(f"⚠️ No data found for {term}")
            return None
        data.reset_index(inplace=True)
        data['term'] = term
        data['fetch_date'] = datetime.now()
        return data
    except Exception as e:
        logger.error(f"❌ Error fetching data for {term}: {e}")
        return None

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    batch_size = int(os.getenv("BATCH_SIZE", 10))
    pytrends = TrendReq(hl='en-US', tz=330, timeout=(30, 60))

    terms = load_terms_queue()
    if not terms:
        logger.info("🎉 No terms to process. Exiting.")
        return

    all_results = []
    successful_terms, failed_terms = [], []
    rate_limit_hit = False

    for i, term in enumerate(terms[:batch_size]):
        logger.info(f"🔹 Fetching {i+1}/{batch_size}: {term}")
        df = fetch_trend_data(term, pytrends)

        if df is not None:
            all_results.append(df)
            append_processed(term, "success")
            successful_terms.append(term)
        else:
            append_processed(term, "failed")
            failed_terms.append(term)

        # Random delay between requests to avoid rate limits
        delay = random.randint(20, 60)
        logger.info(f"⏳ Sleeping {delay}s to avoid rate limit...")
        time.sleep(delay)

        # Detect rate limit
        if "RATE_LIMIT" in str(df):
            logger.warning("⚠️ Rate limit hit, stopping further requests.")
            rate_limit_hit = True
            break

    # -----------------------------------------------------------------
    # Save results safely
    # -----------------------------------------------------------------
    if all_results:
        combined_df = pd.concat(all_results, ignore_index=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_prefix = "partial" if rate_limit_hit else "batch"
        output_file = DATA_DIR / f"trends_{file_prefix}_{timestamp}.csv"

        try:
            combined_df.to_csv(output_file, index=False, encoding='utf-8')
            logger.info(f"💾 Saved {len(combined_df)} rows to {output_file}")
        except Exception as e:
            logger.error(f"❌ Failed to save CSV: {e}")

    # -----------------------------------------------------------------
    # Update queue only if rate limit not hit
    # -----------------------------------------------------------------
    remaining_terms = terms[len(successful_terms) + len(failed_terms):]
    if not rate_limit_hit:
        update_queue(remaining_terms)
    else:
        logger.info("⚠️ Skipping queue update due to rate limit.")

    # -----------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------
    summary_line = (
        f"✅ Run complete — Success: {len(successful_terms)}, "
        f"Failed: {len(failed_terms)}, Remaining: {len(remaining_terms)}"
    )
    logger.info(summary_line)
    print(summary_line)  # visible in GitHub Action logs

# ---------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------
if __name__ == "__main__":
    main()
