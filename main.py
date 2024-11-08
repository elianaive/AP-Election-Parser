import argparse
import requests
import os
from datetime import datetime
from tqdm import tqdm
from parser import ElectionDataParser, DetailedDataParser
from formatters import ConsoleFormatter, CSVFormatter, DetailedFormatter

# Election day configuration
ELECTION_DAYS = {
    # Presidential election years
    2024: {"date": "2024-11-05", "types": ["president", "senate", "governor", "house", "ballot"]},
    2020: {"date": "2020-11-03", "types": ["president", "senate", "governor", "house", "ballot"]},
    2016: {"date": "2016-11-08", "types": ["president", "senate", "governor", "house", "ballot"]},
    2012: {"date": "2012-11-06", "types": ["president", "senate", "governor", "house", "ballot"]},
    2008: {"date": "2008-11-04", "types": ["president", "senate", "governor", "house", "ballot"]},
    2004: {"date": "2004-11-02", "types": ["president", "senate", "governor", "house", "ballot"]},
    2000: {"date": "2000-11-07", "types": ["president", "senate", "governor", "house", "ballot"]},
    
    # Midterm election years
    2022: {"date": "2022-11-08", "types": ["senate", "governor", "house", "ballot"]},
    2018: {"date": "2018-11-06", "types": ["senate", "governor", "house", "ballot"]},
    2014: {"date": "2014-11-04", "types": ["senate", "governor", "house", "ballot"]},
    2010: {"date": "2010-11-02", "types": ["senate", "governor", "house", "ballot"]},
    2006: {"date": "2006-11-07", "types": ["senate", "governor", "house", "ballot"]},
    2002: {"date": "2002-11-05", "types": ["senate", "governor", "house", "ballot"]},
    1998: {"date": "1998-11-03", "types": ["senate", "governor", "house", "ballot"]},
}

def get_base_url(year, date):
    """Construct the base URL for the given election year."""
    if year >= 2024:
        return f"https://interactives.apelections.org/election-results/data-live/{date}"
    elif year >= 2018:
        return f"https://interactives.ap.org/election-results/data-live/{date}"
    else:
        raise ValueError("Data for years before 2018 is not accessible with this URL pattern.")

def ensure_year_directory(base_dir, year):
    """Create year-specific directory if it doesn't exist."""
    year_dir = os.path.join(base_dir, str(year))
    os.makedirs(year_dir, exist_ok=True)
    return year_dir

def fetch_election_data(base_url, year):
    """Fetch election data from AP endpoints."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        progress_url = f"{base_url}/results/national/progress.json"
        metadata_url = f"{base_url}/results/national/metadata.json"
        
        progress_data = requests.get(progress_url, headers=headers, timeout=30).json()
        metadata = requests.get(metadata_url, headers=headers, timeout=30).json()
        
        return progress_data, metadata
    except Exception as e:
        print(f"Error fetching data for {year}: {e}")
        return None, None

def validate_year(year):
    """Validate if the provided year is likely to have accessible data."""
    if year not in ELECTION_DAYS:
        return False, f"Year {year} is not a valid election year"
    
    current_year = datetime.now().year
    if year > current_year:
        return False, f"Cannot process future year {year}"
        
    if year < 1998:
        return False, f"Data before 1998 may not be available in digital format"
        
    return True, "Valid election year"

def main():
    """Main entry point for the parser."""
    parser = argparse.ArgumentParser(description='Parse and display AP Election results')
    parser.add_argument('--save', action='store_true', help='Save results to CSV files')
    parser.add_argument('--data-dir', type=str, default='data', help='Directory to save CSV files')
    parser.add_argument('--years', nargs='+', type=int, 
                       default=[max(ELECTION_DAYS.keys())],
                       help='Election years to process (default: most recent year)')
    parser.add_argument('--retry-failed', action='store_true',
                       help='Retry failed data fetches up to 3 times')
    args = parser.parse_args()

    # Validate requested years
    valid_years = []
    for year in args.years:
        is_valid, message = validate_year(year)
        if is_valid:
            valid_years.append(year)
        else:
            print(message)

    if not valid_years:
        print("No valid election years specified")
        return

    for year in valid_years:
        print(f"\nProcessing election year {year}")
        election_info = ELECTION_DAYS[year]
        
        try:
            base_url = get_base_url(year, election_info['date'])
        except ValueError as e:
            print(f"Skipping year {year}: {e}")
            continue

        # Fetch data with retries
        max_attempts = 3 if args.retry_failed else 1
        attempt = 0
        progress_data = metadata = None
        
        while attempt < max_attempts and not (progress_data and metadata):
            if attempt > 0:
                print(f"Retry attempt {attempt} for year {year}")
            progress_data, metadata = fetch_election_data(base_url, year)
            attempt += 1

        if not progress_data or not metadata:
            print(f"Skipping year {year} due to data fetch error")
            continue

        # Parse results
        parser = ElectionDataParser()
        races = parser.parse_results(progress_data, metadata)
        categorized_races = parser.categorize_races(races)
        
        # Filter races based on election year type
        allowed_types = election_info['types']
        filtered_races = {
            category: races 
            for category, races in categorized_races.items()
            if (category.lower() in allowed_types or 
                (category == 'Ballot Measures' and 'ballot' in allowed_types))
        }

        # Show console output
        print(f"\nResults for {year}:")
        ConsoleFormatter.write_results(filtered_races)

        # Save to CSV files if requested
        if args.save:
            year_dir = ensure_year_directory(args.data_dir, year)
            print(f"\nSaving {year} results to {year_dir}/")
            
            CSVFormatter.write_results(filtered_races, year_dir)

            # Process detailed data
            print(f"\nFetching detailed county-level data for {year}...")
            detailed_parser = DetailedDataParser(base_url)

            race_types = [
                ('president', filtered_races.get('Presidential', [])),
                ('senate', filtered_races.get('Senate', [])),
                ('governor', filtered_races.get('Governor', [])),
                ('ballot', filtered_races.get('Ballot Measures', []))
            ]

            # Process each race type
            for race_type, races in race_types:
                if races:
                    print(f"\nProcessing {race_type} races...")
                    for race in tqdm(races, desc=f"Fetching {race_type} county data"):
                        county_results = detailed_parser.get_detailed_results(
                            race.race_id,
                            race.state_postal
                        )
                        if county_results:
                            DetailedFormatter.write_detailed_results(
                                race_type,
                                race.race_id,
                                county_results,
                                metadata[race.race_id]['candidates'],
                                year_dir
                            )
                    print(f"Completed {race_type} data collection")

if __name__ == "__main__":
    main()
