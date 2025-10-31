import os
import time
import random
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
from pytrends.request import TrendReq

# Configuration
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 75))
DELAY_MIN = 3
DELAY_MAX = 6
MAX_RETRIES = 3
TERMS_QUEUE_FILE = 'data/terms_queue.txt'
PROCESSED_FILE = 'processed_terms.txt'
DATA_DIR = Path('data/trends_results')
LOG_DIR = Path('logs')

# Setup directories
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Setup logging
log_file = LOG_DIR / f'trends_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def load_terms_queue():
    """Load terms from queue file."""
    if not Path(TERMS_QUEUE_FILE).exists():
        logger.error(f"Terms queue file not found: {TERMS_QUEUE_FILE}")
        return []
    
    with open(TERMS_QUEUE_FILE, 'r', encoding='utf-8') as f:
        terms = [line.strip() for line in f if line.strip()]
    
    logger.info(f"Loaded {len(terms)} terms from queue")
    return terms


def load_processed_terms():
    """Load already processed terms."""
    if not Path(PROCESSED_FILE).exists():
        return set()
    
    with open(PROCESSED_FILE, 'r', encoding='utf-8') as f:
        processed = {line.strip() for line in f if line.strip()}
    
    logger.info(f"Found {len(processed)} already processed terms")
    return processed


def save_processed_term(term):
    """Append term to processed list."""
    with open(PROCESSED_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{term}\n")


def update_queue(remaining_terms):
    """Update queue file with remaining terms."""
    with open(TERMS_QUEUE_FILE, 'w', encoding='utf-8') as f:
        for term in remaining_terms:
            f.write(f"{term}\n")
    logger.info(f"Updated queue with {len(remaining_terms)} remaining terms")


def fetch_trend_data(pytrends, term, retries=0):
    """Fetch Google Trends data for a single term."""
    try:
        # Build payload - Data from 2012, Sri Lanka
        pytrends.build_payload(
            [term],
            cat=0,
            timeframe='2012-01-01 2025-10-31',  # From 2012 to today
            geo='LK',  # Sri Lanka
            gprop=''
        )
        
        # Get interest over time
        interest_df = pytrends.interest_over_time()
        
        if interest_df.empty:
            logger.warning(f"No data returned for term: {term}")
            return None
        
        # Remove the 'isPartial' column if it exists
        if 'isPartial' in interest_df.columns:
            interest_df = interest_df.drop(columns=['isPartial'])
        
        # Add metadata
        interest_df['term'] = term
        interest_df['fetch_date'] = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"✓ Successfully fetched data for: {term}")
        return interest_df
    
    except Exception as e:
        error_msg = str(e)
        
        # Check for rate limit error
        if '429' in error_msg or 'Too Many Requests' in error_msg:
            logger.warning(f"⚠️ Rate limit hit (429) on term: {term} - will continue on next run")
            raise Exception("RATE_LIMIT")
        
        # Check for timeout errors
        if 'ReadTimeout' in error_msg or 'ConnectTimeout' in error_msg or 'timeout' in error_msg.lower():
            logger.warning(f"⏱️  Timeout on {term}, retry {retries+1}/{MAX_RETRIES}")
            if retries < MAX_RETRIES:
                time.sleep(random.uniform(10, 15))  # Longer wait for timeouts
                return fetch_trend_data(pytrends, term, retries + 1)
            else:
                logger.error(f"✗ Failed {term} after {MAX_RETRIES} timeout retries")
                return None
        
        # Retry on other errors
        if retries < MAX_RETRIES:
            logger.warning(f"Error fetching {term}, retry {retries+1}/{MAX_RETRIES}: {error_msg}")
            time.sleep(random.uniform(5, 10))
            return fetch_trend_data(pytrends, term, retries + 1)
        
        logger.error(f"✗ Failed to fetch {term} after {MAX_RETRIES} retries: {error_msg}")
        return None


def main():
    logger.info("=" * 60)
    logger.info("Starting Google Trends fetch job")
    logger.info(f"Batch size: {BATCH_SIZE}")
    logger.info("=" * 60)
    
    # Load terms
    all_terms = load_terms_queue()
    processed_terms = load_processed_terms()
    
    # Filter out already processed terms
    pending_terms = [t for t in all_terms if t not in processed_terms]
    
    if not pending_terms:
        logger.info("✅ No pending terms to process. Queue is empty or all terms processed.")
        return
    
    # Select batch
    batch = pending_terms[:BATCH_SIZE]
    logger.info(f"Processing batch of {len(batch)} terms")
    
    # Initialize pytrends with longer timeout
    pytrends = TrendReq(hl='en-US', tz=360, timeout=(30, 60), retries=3, backoff_factor=1.0)
    
    # Process terms
    all_results = []
    successful_terms = []
    failed_terms = []
    rate_limit_hit = False
    
    for idx, term in enumerate(batch, 1):
        logger.info(f"[{idx}/{len(batch)}] Processing: {term}")
        
        try:
            # Fetch data
            df = fetch_trend_data(pytrends, term)
            
            if df is not None:
                all_results.append(df)
                successful_terms.append(term)
                save_processed_term(term)
            else:
                failed_terms.append(term)
            
            # Random delay between requests
            if idx < len(batch):  # Don't delay after last term
                delay = random.uniform(DELAY_MIN, DELAY_MAX)
                logger.info(f"Waiting {delay:.1f}s before next request...")
                time.sleep(delay)
        
        except Exception as e:
            if "RATE_LIMIT" in str(e):
                logger.warning("⚠️ Rate limit encountered. Stopping this run gracefully.")
                rate_limit_hit = True
                break
            else:
                logger.error(f"Unexpected error processing {term}: {e}")
                failed_terms.append(term)
    
    # Save results
    if all_results:
        combined_df = pd.concat(all_results, ignore_index=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = DATA_DIR / f'trends_{timestamp}.csv'
        combined_df.to_csv(output_file, index=True)
        logger.info(f"💾 Saved {len(all_results)} term results to {output_file}")
    
    # Update queue (remove successfully processed terms)
    remaining_terms = [t for t in pending_terms if t not in successful_terms]
    update_queue(remaining_terms)
    
    # Summary
    logger.info("=" * 60)
    logger.info("Job Summary")
    logger.info(f"✓ Successful: {len(successful_terms)}")
    logger.info(f"✗ Failed: {len(failed_terms)}")
    logger.info(f"📊 Remaining in queue: {len(remaining_terms)}")
    if rate_limit_hit:
        logger.info("⚠️  Rate limit hit - will resume on next scheduled run")
    logger.info("=" * 60)
    


if __name__ == '__main__':
    main()
