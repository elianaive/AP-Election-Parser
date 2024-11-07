import argparse
import requests
from parser import ElectionDataParser
from formatters import ConsoleFormatter, CSVFormatter

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
        print(f"\nSaving results to {args.data_dir}/")
        CSVFormatter.write_results(categorized_races, args.data_dir)

if __name__ == "__main__":
    main()