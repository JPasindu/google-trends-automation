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
COUNTRIES_FILE = 'data/countries.txt'
PROGRESS_FILE = 'progress_state.txt'
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


def load_countries():
    """Load country codes from file."""
    if not Path(COUNTRIES_FILE).exists():
        logger.error(f"Countries file not found: {COUNTRIES_FILE}")
        logger.info("Creating sample countries.txt file with format: COUNTRY_CODE|Country Name")
        # Create sample file
        with open(COUNTRIES_FILE, 'w', encoding='utf-8') as f:
            f.write("LK|Sri Lanka\n")
            f.write("US|United States\n")
            f.write("IN|India\n")
        return []
    
    countries = []
    with open(COUNTRIES_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                parts = line.split('|')
                if len(parts) >= 2:
                    countries.append({'code': parts[0].strip(), 'name': parts[1].strip()})
                else:
                    countries.append({'code': parts[0].strip(), 'name': parts[0].strip()})
    
    logger.info(f"Loaded {len(countries)} countries")
    return countries


def load_terms_queue():
    """Load terms from queue file."""
    if not Path(TERMS_QUEUE_FILE).exists():
        logger.error(f"Terms queue file not found: {TERMS_QUEUE_FILE}")
        return []
    
    with open(TERMS_QUEUE_FILE, 'r', encoding='utf-8') as f:
        terms = [line.strip() for line in f if line.strip()]
    
    logger.info(f"Loaded {len(terms)} terms from queue")
    return terms


def load_progress():
    """Load progress state from file."""
    if not Path(PROGRESS_FILE).exists():
        return {}
    
    progress = {}
    with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                parts = line.split('|')
                if len(parts) == 2:
                    country_code, term = parts
                    if country_code not in progress:
                        progress[country_code] = set()
                    progress[country_code].add(term)
    
    return progress


def save_progress(country_code, term):
    """Append processed term for a country to progress file."""
    with open(PROGRESS_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{country_code}|{term}\n")


def get_master_file_path(country_code):
    """Get the master file path for a specific country."""
    return DATA_DIR / f'google_trends_master_{country_code}.csv'


def load_or_create_master_df(country_code):
    """Load existing master file or create new one for a country."""
    master_file = get_master_file_path(country_code)
    
    if master_file.exists():
        df = pd.read_csv(master_file, parse_dates=['date'])
        logger.info(f"Loaded master file for {country_code} with {len(df)} rows and {len(df.columns)-1} terms")
        return df
    else:
        # Create empty dataframe with date column
        empty_df = pd.DataFrame(columns=['date'])
        return empty_df


def save_master_df(df, country_code):
    """Save master dataframe to CSV for a specific country."""
    master_file = get_master_file_path(country_code)
    df.to_csv(master_file, index=False)
    logger.info(f"Saved master file for {country_code} with {len(df)} rows and {len(df.columns)-1} terms")


def fetch_trend_data(pytrends, term, country_code, retries=0):
    """Fetch Google Trends data for a single term and country."""
    try:
        # Build payload - Data from 2012
        pytrends.build_payload(
            [term],
            cat=0,
            timeframe='2012-01-01 2025-10-31',
            geo=country_code,
            gprop=''
        )
        
        # Get interest over time
        interest_df = pytrends.interest_over_time()
        
        if interest_df.empty:
            logger.warning(f"No data returned for term: {term} in {country_code}")
            return None
        
        # Remove the 'isPartial' column if it exists
        if 'isPartial' in interest_df.columns:
            interest_df = interest_df.drop(columns=['isPartial'])
        
        # Reset index to make date a column
        interest_df = interest_df.reset_index()
        
        # Rename the term column to just the term name
        interest_df = interest_df.rename(columns={term: term})
        
        # Keep only date and the term column
        interest_df = interest_df[['date', term]]
        
        logger.info(f"✓ Successfully fetched data for: {term} ({country_code})")
        return interest_df
    
    except Exception as e:
        error_msg = str(e)
        
        # Check for rate limit error
        if '429' in error_msg or 'Too Many Requests' in error_msg:
            logger.warning(f"⚠️ Rate limit hit (429) on term: {term} ({country_code}) - will continue on next run")
            raise Exception("RATE_LIMIT")
        
        # Check for timeout errors
        if 'ReadTimeout' in error_msg or 'ConnectTimeout' in error_msg or 'timeout' in error_msg.lower():
            logger.warning(f"⏱️ Timeout on {term} ({country_code}), retry {retries+1}/{MAX_RETRIES}")
            if retries < MAX_RETRIES:
                time.sleep(random.uniform(10, 15))
                return fetch_trend_data(pytrends, term, country_code, retries + 1)
            else:
                logger.error(f"✗ Failed {term} ({country_code}) after {MAX_RETRIES} timeout retries")
                return None
        
        # Retry on other errors
        if retries < MAX_RETRIES:
            logger.warning(f"Error fetching {term} ({country_code}), retry {retries+1}/{MAX_RETRIES}: {error_msg}")
            time.sleep(random.uniform(5, 10))
            return fetch_trend_data(pytrends, term, country_code, retries + 1)
        
        logger.error(f"✗ Failed to fetch {term} ({country_code}) after {MAX_RETRIES} retries: {error_msg}")
        return None


def update_master_with_new_terms(master_df, new_data_list):
    """Merge new terms data into master dataframe."""
    if not new_data_list:
        return master_df
    
    # Start with master or create from first term
    if master_df.empty:
        result_df = new_data_list[0].copy()
        start_idx = 1
    else:
        result_df = master_df.copy()
        start_idx = 0
    
    # Merge each new term one by one on 'date'
    for df in new_data_list[start_idx:]:
        result_df = pd.merge(result_df, df, on='date', how='outer')
    
    # Sort by date and fill missing values with 0
    result_df = result_df.sort_values('date').fillna(0).reset_index(drop=True)
    
    return result_df


def process_country_batch(country, all_terms, progress, pytrends):
    """Process a batch of terms for a specific country."""
    country_code = country['code']
    country_name = country['name']
    
    logger.info("=" * 60)
    logger.info(f"Processing country: {country_name} ({country_code})")
    logger.info("=" * 60)
    
    # Load master dataframe for this country
    master_df = load_or_create_master_df(country_code)
    
    # Get already processed terms for this country
    processed_terms = progress.get(country_code, set())
    
    # Get existing columns in master file
    existing_columns = set(master_df.columns) if not master_df.empty else set()
    
    # Find truly new terms (not in progress and not in master)
    truly_new_terms = [t for t in all_terms if t not in processed_terms and t not in existing_columns]
    
    if not truly_new_terms:
        logger.info(f"✅ All terms already processed for {country_name}")
        return 0, 0, False
    
    # Select batch
    batch = truly_new_terms[:BATCH_SIZE]
    logger.info(f"Processing batch of {len(batch)} terms for {country_name}")
    logger.info(f"Remaining terms after this batch: {len(truly_new_terms) - len(batch)}")
    
    # Process terms
    new_data_list = []
    successful_count = 0
    failed_count = 0
    rate_limit_hit = False
    
    for idx, term in enumerate(batch, 1):
        logger.info(f"[{idx}/{len(batch)}] {country_name} - Processing: {term}")
        
        try:
            # Fetch data
            df = fetch_trend_data(pytrends, term, country_code)
            
            if df is not None:
                new_data_list.append(df)
                successful_count += 1
                save_progress(country_code, term)
                # Update progress in memory
                if country_code not in progress:
                    progress[country_code] = set()
                progress[country_code].add(term)
            else:
                failed_count += 1
            
            # Random delay between requests
            if idx < len(batch):
                delay = random.uniform(DELAY_MIN, DELAY_MAX)
                logger.info(f"Waiting {delay:.1f}s before next request...")
                time.sleep(delay)
        
        except Exception as e:
            if "RATE_LIMIT" in str(e):
                logger.warning(f"⚠️ Rate limit encountered for {country_name}. Stopping this run gracefully.")
                rate_limit_hit = True
                break
            else:
                logger.error(f"Unexpected error processing {term} for {country_name}: {e}")
                failed_count += 1
    
    # Update master dataframe if we have new data
    if new_data_list:
        updated_master = update_master_with_new_terms(master_df, new_data_list)
        save_master_df(updated_master, country_code)
        logger.info(f"💾 Updated master file for {country_name} with {len(new_data_list)} new terms")
        
        # Create backup with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = DATA_DIR / f'trends_backup_{country_code}_{timestamp}.csv'
        updated_master.to_csv(backup_file, index=False)
        logger.info(f"💾 Created backup: {backup_file}")
    else:
        logger.info(f"No new data to add for {country_name}")
    
    return successful_count, failed_count, rate_limit_hit


def main():
    logger.info("=" * 60)
    logger.info("Starting Multi-Country Google Trends Fetch Job")
    logger.info(f"Batch size per country: {BATCH_SIZE}")
    logger.info("=" * 60)
    
    # Load configuration
    countries = load_countries()
    all_terms = load_terms_queue()
    progress = load_progress()
    
    if not countries:
        logger.error("No countries loaded. Please check countries.txt file.")
        return
    
    if not all_terms:
        logger.error("No terms loaded. Please check terms_queue.txt file.")
        return
    
    # Initialize pytrends
    pytrends = TrendReq(hl='en-US', tz=360, timeout=(30, 60), retries=3, backoff_factor=1.0)
    
    # Track overall statistics
    total_successful = 0
    total_failed = 0
    rate_limit_hit = False
    
    # Process each country
    for country in countries:
        successful, failed, hit_limit = process_country_batch(country, all_terms, progress, pytrends)
        total_successful += successful
        total_failed += failed
        
        if hit_limit:
            rate_limit_hit = True
            logger.warning(f"⚠️ Stopping due to rate limit hit on {country['name']}")
            break
        
        # Add delay between countries to avoid rate limits
        if country != countries[-1]:
            delay = random.uniform(DELAY_MIN * 2, DELAY_MAX * 2)
            logger.info(f"Waiting {delay:.1f}s before next country...")
            time.sleep(delay)
    
    # Final summary
    logger.info("=" * 60)
    logger.info("Overall Job Summary")
    logger.info(f"✓ Total successful: {total_successful}")
    logger.info(f"✗ Total failed: {total_failed}")
    logger.info(f"📊 Countries processed: {len(countries)}")
    logger.info(f"📊 Total terms: {len(all_terms)}")
    
    # Progress summary for each country
    for country in countries:
        country_code = country['code']
        processed = len(progress.get(country_code, set()))
        remaining = len(all_terms) - processed
        logger.info(f"  {country['name']} ({country_code}): {processed}/{len(all_terms)} complete, {remaining} remaining")
    
    if rate_limit_hit:
        logger.warning("⚠️ Rate limit hit - will resume on next scheduled run")
    else:
        # Check if all countries are complete
        all_complete = all(
            len(progress.get(c['code'], set())) >= len(all_terms)
            for c in countries
        )
        if all_complete:
            logger.info("🎉 All terms downloaded for all countries!")
    
    logger.info("=" * 60)


if __name__ == '__main__':
    main()