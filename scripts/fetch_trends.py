import os
import time
import random
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
from pytrends.request import TrendReq

# Configuration
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 5))  # Reduced for demo
DELAY_MIN = 3
DELAY_MAX = 6
MAX_RETRIES = 3
TERMS_QUEUE_FILE = 'data/terms_queue.txt'
PROCESSED_FILE = 'processed_terms.txt'
DATA_DIR = Path('data/trends_results')
LOG_DIR = Path('logs')
MASTER_FILE = DATA_DIR / 'google_trends_master.csv'

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

def load_or_create_master_df():
    """Load existing master file or create new one."""
    if MASTER_FILE.exists():
        df = pd.read_csv(MASTER_FILE, parse_dates=['date'])
        logger.info(f"Loaded master file with {len(df)} rows and {len(df.columns)-1} terms")
        return df
    else:
        # Create empty dataframe with date column
        empty_df = pd.DataFrame(columns=['date'])
        return empty_df

def save_master_df(df):
    """Save master dataframe to CSV."""
    df.to_csv(MASTER_FILE, index=False)
    logger.info(f"Saved master file with {len(df)} rows and {len(df.columns)-1} terms")

def fetch_trend_data(pytrends, term, retries=0):
    """Fetch Google Trends data for a single term."""
    try:
        # Build payload
        pytrends.build_payload(
            [term],
            cat=0,
            timeframe='today 12-m',
            geo='',
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
        
        # Reset index to make date a column
        interest_df = interest_df.reset_index()
        interest_df = interest_df.rename(columns={term: f'{term}'})
        
        # Keep only date and the term column
        interest_df = interest_df[['date', term]]
        
        logger.info(f"✓ Successfully fetched data for: {term}")
        return interest_df
    
    except Exception as e:
        error_msg = str(e)
        
        if '429' in error_msg or 'Too Many Requests' in error_msg:
            logger.error(f"🚫 Rate limit hit (429) on term: {term}")
            raise Exception("RATE_LIMIT")
        
        if 'ReadTimeout' in error_msg or 'ConnectTimeout' in error_msg or 'timeout' in error_msg.lower():
            logger.warning(f"⏱️  Timeout on {term}, retry {retries+1}/{MAX_RETRIES}")
            if retries < MAX_RETRIES:
                time.sleep(random.uniform(10, 15))
                return fetch_trend_data(pytrends, term, retries + 1)
            else:
                logger.error(f"✗ Failed {term} after {MAX_RETRIES} timeout retries")
                return None
        
        if retries < MAX_RETRIES:
            logger.warning(f"Error fetching {term}, retry {retries+1}/{MAX_RETRIES}: {error_msg}")
            time.sleep(random.uniform(5, 10))
            return fetch_trend_data(pytrends, term, retries + 1)
        
        logger.error(f"✗ Failed to fetch {term} after {MAX_RETRIES} retries: {error_msg}")
        return None

def update_master_with_new_terms(master_df, new_data_list):
    """Merge new terms data into master dataframe."""
    if not new_data_list:
        return master_df
    
    # Start with first dataframe
    merged_df = new_data_list[0]
    
    # Merge all new data first
    for i in range(1, len(new_data_list)):
        merged_df = pd.merge(merged_df, new_data_list[i], on='date', how='outer')
    
    # If master exists, merge with master
    if not master_df.empty:
        # Merge on date column
        final_df = pd.merge(master_df, merged_df, on='date', how='outer')
    else:
        final_df = merged_df
    
    # Sort by date
    final_df = final_df.sort_values('date').reset_index(drop=True)
    
    return final_df

def main():
    logger.info("=" * 60)
    logger.info("Starting Google Trends fetch job - MASTER FILE MODE")
    logger.info(f"Batch size: {BATCH_SIZE}")
    logger.info("=" * 60)
    
    # Load master dataframe
    master_df = load_or_create_master_df()
    
    # Load terms
    all_terms = load_terms_queue()
    processed_terms = load_processed_terms()
    
    # Filter out already processed terms
    pending_terms = [t for t in all_terms if t not in processed_terms]
    
    if not pending_terms:
        logger.info("✅ No pending terms to process. Queue is empty or all terms processed.")
        return
    
    # Check which pending terms are already in master (in case of partial processing)
    existing_columns = set(master_df.columns) if not master_df.empty else set()
    truly_new_terms = [t for t in pending_terms if t not in existing_columns]
    
    if not truly_new_terms:
        logger.info("✅ All pending terms already exist in master file.")
        return
    
    # Select batch from truly new terms
    batch = truly_new_terms[:BATCH_SIZE]
    logger.info(f"Processing batch of {len(batch)} new terms")
    
    # Initialize pytrends
    pytrends = TrendReq(hl='en-US', tz=360, timeout=(30, 60), retries=3, backoff_factor=1.0)
    
    # Process terms
    new_data_list = []
    successful_terms = []
    failed_terms = []
    rate_limit_hit = False
    
    for idx, term in enumerate(batch, 1):
        logger.info(f"[{idx}/{len(batch)}] Processing: {term}")
        
        try:
            # Fetch data
            df = fetch_trend_data(pytrends, term)
            
            if df is not None:
                new_data_list.append(df)
                successful_terms.append(term)
                save_processed_term(term)
            else:
                failed_terms.append(term)
            
            # Random delay between requests
            if idx < len(batch):
                delay = random.uniform(DELAY_MIN, DELAY_MAX)
                logger.info(f"Waiting {delay:.1f}s before next request...")
                time.sleep(delay)
        
        except Exception as e:
            if "RATE_LIMIT" in str(e):
                logger.error("🚫 Rate limit encountered. Stopping this run.")
                rate_limit_hit = True
                break
            else:
                logger.error(f"Unexpected error processing {term}: {e}")
                failed_terms.append(term)
    
    # Update master dataframe
    if new_data_list:
        updated_master = update_master_with_new_terms(master_df, new_data_list)
        save_master_df(updated_master)
        logger.info(f"💾 Updated master file with {len(new_data_list)} new terms")
        
        # Also save a backup with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = DATA_DIR / f'trends_backup_{timestamp}.csv'
        updated_master.to_csv(backup_file, index=False)
        logger.info(f"💾 Created backup: {backup_file}")
    
    # Update queue
    remaining_terms = [t for t in pending_terms if t not in successful_terms]
    update_queue(remaining_terms)
    
    # Summary
    logger.info("=" * 60)
    logger.info("Job Summary")
    logger.info(f"✓ Successful: {len(successful_terms)}")
    logger.info(f"✗ Failed: {len(failed_terms)}")
    logger.info(f"📊 Total terms in master: {len(updated_master.columns)-1}")
    logger.info(f"📊 Remaining in queue: {len(remaining_terms)}")
    if rate_limit_hit:
        logger.info("⚠️  Rate limit hit - will resume on next scheduled run")
    logger.info("=" * 60)

# Keep the existing helper functions (load_terms_queue, load_processed_terms, etc.)
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

if __name__ == '__main__':
    main()
