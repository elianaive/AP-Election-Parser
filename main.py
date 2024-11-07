import argparse
import requests
from datetime import datetime
from tqdm import tqdm
from parser import ElectionDataParser, DetailedDataParser
from formatters import ConsoleFormatter, CSVFormatter, DetailedFormatter

def main():
    """Main entry point for the parser."""
    parser = argparse.ArgumentParser(description='Parse and display AP Election results')
    parser.add_argument('--save', action='store_true', help='Save results to CSV files')
    parser.add_argument('--data-dir', type=str, default='data', help='Directory to save CSV files')
    args = parser.parse_args()
   
    BASE_URL = "https://interactives.apelections.org/election-results/data-live/2024-11-05"
   
    print("Fetching AP Election data...")
    progress_data = requests.get(f"{BASE_URL}/results/national/progress.json").json()
    metadata = requests.get(f"{BASE_URL}/results/national/metadata.json").json()
   
    # Parse the results
    parser = ElectionDataParser()
    races = parser.parse_results(progress_data, metadata)
    categorized_races = parser.categorize_races(races)
   
    # Always show console output
    ConsoleFormatter.write_results(categorized_races)
   
    # Save to CSV files if requested
    if args.save:
        # Generate timestamp once for the entire run
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        print(f"\nSaving results to {args.data_dir}/")
        CSVFormatter.write_results(categorized_races, timestamp, args.data_dir)
       
        # After saving basic results, get detailed data
        print("\nFetching detailed county-level data...")
        detailed_parser = DetailedDataParser(BASE_URL)
       
        # Get detailed data for relevant race types
        race_types = [
            ('governor', categorized_races.get('Governor', [])),
            ('ballot', categorized_races.get('Ballot Measures', []))
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
                            timestamp,
                            args.data_dir
                        )
                print(f"Completed {race_type} data collection")

if __name__ == "__main__":
    main()