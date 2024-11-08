import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Union, Optional
import pandas as pd
from models import ElectionRace, BallotMeasure, PersonCandidate, BallotOptionCandidate, CountyResult

class ElectionDatabase:
    """Handles database operations for election data storage."""
    
    def __init__(self, db_path: str = "election_data.db"):
        """Initialize database connection and create tables if they don't exist."""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.create_tables()
    
    def create_tables(self):
        """Create tables that directly match CSV structures."""
        with self.conn:
            self.conn.executescript('''
                -- Track data fetches and exports
                CREATE TABLE IF NOT EXISTS election_fetches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    year INTEGER NOT NULL,
                    fetch_timestamp DATETIME NOT NULL,
                    success BOOLEAN NOT NULL,
                    error_message TEXT
                );
                
                CREATE TABLE IF NOT EXISTS csv_exports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    year INTEGER NOT NULL,
                    export_timestamp DATETIME NOT NULL,
                    file_type TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    UNIQUE(year, file_type, file_path)
                );

                -- Ballot measures results
                CREATE TABLE IF NOT EXISTS ballot_measures (
                    race_id TEXT NOT NULL,
                    state_postal TEXT NOT NULL,
                    state_name TEXT NOT NULL,
                    description TEXT NOT NULL,
                    category TEXT,
                    summary TEXT,
                    race_call_status TEXT NOT NULL,
                    last_updated DATETIME NOT NULL,
                    precincts_reporting INTEGER NOT NULL,
                    precincts_total INTEGER NOT NULL,
                    precincts_reporting_pct REAL NOT NULL,
                    expected_vote_pct REAL,
                    total_votes INTEGER NOT NULL,
                    candidate_id TEXT NOT NULL,
                    option_name TEXT NOT NULL,
                    vote_count INTEGER NOT NULL,
                    vote_pct REAL NOT NULL,
                    PRIMARY KEY (race_id, candidate_id)
                );

                -- House races
                CREATE TABLE IF NOT EXISTS house_races (
                    race_id TEXT NOT NULL,
                    state_postal TEXT NOT NULL,
                    state_name TEXT NOT NULL,
                    office_id TEXT NOT NULL,
                    seat_name TEXT,
                    seat_num TEXT,
                    race_call_status TEXT NOT NULL,
                    last_updated DATETIME NOT NULL,
                    precincts_reporting INTEGER NOT NULL,
                    precincts_total INTEGER NOT NULL,
                    precincts_reporting_pct REAL NOT NULL,
                    expected_vote_pct REAL,
                    total_votes INTEGER NOT NULL,
                    candidate_id TEXT NOT NULL,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    party TEXT NOT NULL,
                    incumbent BOOLEAN,
                    vote_count INTEGER NOT NULL,
                    vote_pct REAL NOT NULL,
                    PRIMARY KEY (race_id, candidate_id)
                );

                -- Governor races
                CREATE TABLE IF NOT EXISTS governor_races (
                    race_id TEXT NOT NULL,
                    state_postal TEXT NOT NULL,
                    state_name TEXT NOT NULL,
                    race_call_status TEXT NOT NULL,
                    last_updated DATETIME NOT NULL,
                    precincts_reporting INTEGER NOT NULL,
                    precincts_total INTEGER NOT NULL,
                    precincts_reporting_pct REAL NOT NULL,
                    expected_vote_pct REAL,
                    total_votes INTEGER NOT NULL,
                    candidate_id TEXT NOT NULL,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    party TEXT NOT NULL,
                    incumbent BOOLEAN,
                    vote_count INTEGER NOT NULL,
                    vote_pct REAL NOT NULL,
                    PRIMARY KEY (race_id, candidate_id)
                );

                -- Senate races
                CREATE TABLE IF NOT EXISTS senate_races (
                    race_id TEXT NOT NULL,
                    state_postal TEXT NOT NULL,
                    state_name TEXT NOT NULL,
                    office_id TEXT NOT NULL,
                    seat_name TEXT,
                    seat_num TEXT,
                    race_call_status TEXT NOT NULL,
                    last_updated DATETIME NOT NULL,
                    precincts_reporting INTEGER NOT NULL,
                    precincts_total INTEGER NOT NULL,
                    precincts_reporting_pct REAL NOT NULL,
                    expected_vote_pct REAL,
                    total_votes INTEGER NOT NULL,
                    candidate_id TEXT NOT NULL,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    party TEXT NOT NULL,
                    incumbent BOOLEAN,
                    vote_count INTEGER NOT NULL,
                    vote_pct REAL NOT NULL,
                    PRIMARY KEY (race_id, candidate_id)
                );

                -- Presidential races
                CREATE TABLE IF NOT EXISTS presidential_races (
                    race_id TEXT NOT NULL,
                    state_postal TEXT NOT NULL,
                    state_name TEXT NOT NULL,
                    race_call_status TEXT NOT NULL,
                    last_updated DATETIME NOT NULL,
                    precincts_reporting INTEGER NOT NULL,
                    precincts_total INTEGER NOT NULL,
                    precincts_reporting_pct REAL NOT NULL,
                    expected_vote_pct REAL,
                    total_votes INTEGER NOT NULL,
                    candidate_id TEXT NOT NULL,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    party TEXT NOT NULL,
                    incumbent BOOLEAN,
                    vote_count INTEGER NOT NULL,
                    vote_pct REAL NOT NULL,
                    PRIMARY KEY (race_id, candidate_id)
                );

                -- County-level results for all race types
                CREATE TABLE IF NOT EXISTS county_results (
                    race_id TEXT NOT NULL,
                    state_postal TEXT NOT NULL,
                    county_name TEXT,               -- Allow NULL
                    county_fips TEXT,               -- Allow NULL
                    county_id TEXT,                 -- Allow NULL
                    precincts_reporting INTEGER,    -- Allow NULL
                    precincts_total INTEGER,        -- Allow NULL
                    precincts_reporting_pct REAL,   -- Allow NULL
                    expected_vote_pct REAL,         -- Allow NULL
                    total_votes INTEGER,            -- Allow NULL
                    registered_voters INTEGER,       -- Allow NULL
                    last_updated DATETIME NOT NULL,
                    candidate_id TEXT NOT NULL,
                    first_name TEXT,                -- Allow NULL
                    last_name TEXT,                 -- Allow NULL
                    option_name TEXT,               -- Allow NULL
                    party TEXT,                     -- Allow NULL
                    vote_count INTEGER NOT NULL,
                    vote_pct REAL NOT NULL,
                    PRIMARY KEY (race_id, county_fips, candidate_id)
                );
            ''')
    
    def record_fetch(self, year: int, success: bool, error_message: Optional[str] = None):
        """Record an attempt to fetch election data."""
        with self.conn:
            self.conn.execute('''
                INSERT INTO election_fetches (year, fetch_timestamp, success, error_message)
                VALUES (?, ?, ?, ?)
            ''', (year, datetime.now(), success, error_message))
    
    def record_csv_export(self, year: int, file_type: str, file_path: str):
        """Record a CSV export."""
        with self.conn:
            self.conn.execute('''
                INSERT OR REPLACE INTO csv_exports 
                (year, export_timestamp, file_type, file_path)
                VALUES (?, ?, ?, ?)
            ''', (year, datetime.now(), file_type, file_path))

    def load_csv_file(self, file_path: str, year: int):
        """Load data from a CSV file into appropriate table."""
        file_name = Path(file_path).stem.lower()
        table_name = None
        
        # Updated file pattern matching
        if any(pattern in file_name for pattern in ['_detail', '_detailed']):
            table_name = 'county_results'
            print(f"Processing county results file: {file_name}")
        elif 'ballot' in file_name:
            table_name = 'ballot_measures'
        elif 'house' in file_name:
            table_name = 'house_races'
        elif 'governor' in file_name:
            table_name = 'governor_races'
        elif 'senate' in file_name:
            table_name = 'senate_races'
        elif 'president' in file_name:
            table_name = 'presidential_races'
        
        print(f"\nProcessing {file_path} -> {table_name}")
            
        if table_name:
            try:
                # Read CSV
                df = pd.read_csv(file_path)
                print(f"Read {len(df)} rows with columns: {', '.join(df.columns)}")
                
                # Convert boolean columns if they exist
                if 'incumbent' in df.columns:
                    df['incumbent'] = df['incumbent'].fillna(False).astype(bool)
                
                # Fill NaN values appropriately based on column type
                numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
                for col in numeric_cols:
                    df[col] = df[col].fillna(0)
                
                # Fill NaN values with None for non-numeric columns
                string_cols = df.select_dtypes(include=['object']).columns
                for col in string_cols:
                    df[col] = df[col].where(pd.notnull(df[col]), None)
                
                # Special handling for county results
                if table_name == 'county_results':
                    # Ensure required columns have values
                    df['county_name'] = df['county_name'].fillna('Unknown')
                    df['county_fips'] = df['county_fips'].fillna('00000')
                    df['county_id'] = df['county_id'].fillna('0')
                    df['vote_count'] = df['vote_count'].fillna(0).astype(int)
                    df['vote_pct'] = df['vote_pct'].fillna(0.0).astype(float)
                
                # Clear existing data for these race_ids
                race_ids = df['race_id'].unique()
                with self.conn:  # Ensure atomic transaction
                    self.conn.execute(
                        f"DELETE FROM {table_name} WHERE race_id IN ({','.join('?' for _ in race_ids)})",
                        list(race_ids)
                    )
                    
                    # Load new data
                    df.to_sql(table_name, self.conn, if_exists='append', index=False)
                
                print(f"Successfully loaded {len(df)} rows into {table_name}")
                
                # Record the export
                self.record_csv_export(year, table_name, str(file_path))
                
            except Exception as e:
                print(f"Error loading {file_path}: {str(e)}")
                print(f"Full error details: {e.__class__.__name__}")
                if hasattr(e, '__cause__'):
                    print(f"Caused by: {e.__cause__}")
                import traceback
                traceback.print_exc()

    def load_from_directory(self, data_dir: str):
        """Load all CSV files from a directory structure."""
        data_path = Path(data_dir)
        found_files = list(data_path.rglob('*.csv'))
        
        print(f"\nFound {len(found_files)} CSV files in {data_path}")
        
        # Sort files to handle base files before detailed files
        found_files.sort(key=lambda x: '_detail' in x.name.lower())
        
        # Start a transaction for all loads
        with self.conn:
            for path in found_files:
                try:
                    print(f"\nProcessing file: {path}")
                    print(f"Parent directory: {path.parent.name}")
                    
                    # Get year from directory or filename
                    year_str = path.parent.name
                    try:
                        year = int(year_str)
                        print(f"Using year from directory: {year}")
                    except ValueError:
                        # Try to extract year from filename
                        if path.stem[0:4].isdigit():
                            year = int(path.stem[0:4])
                            print(f"Using year from filename: {year}")
                        else:
                            import re
                            year_match = re.search(r'20\d{2}', path.stem)
                            if year_match:
                                year = int(year_match.group())
                                print(f"Found year in filename: {year}")
                            else:
                                print(f"Skipping {path}: Cannot determine year")
                                continue
                    
                    self.load_csv_file(str(path), year)
                        
                except Exception as e:
                    print(f"Error processing {path}: {str(e)}")
                    if hasattr(e, '__cause__'):
                        print(f"Caused by: {e.__cause__}")
                    continue

    def clear_data(self):
        """Clear all data from tables except tracking tables."""
        with self.conn:
            cursor = self.conn.cursor()
            # Get list of all tables
            tables = cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT IN ('election_fetches', 'csv_exports')
            """).fetchall()
            
            print("Clearing tables:")
            for (table_name,) in tables:
                print(f"  - {table_name}")
                cursor.execute(f"DELETE FROM {table_name}")
            
            print("All data cleared")

    def get_database_summary(self) -> dict:
        """Get a summary of database contents."""
        cursor = self.conn.cursor()
        summary = {
            "tables": {},
            "yearly_stats": {},
            "metadata": {
                "db_path": self.db_path,
                "generated_at": datetime.now().isoformat()
            }
        }
        
        # Get table row counts
        race_tables = [
            'ballot_measures', 'house_races', 'governor_races', 
            'senate_races', 'presidential_races', 'county_results',
            'election_fetches', 'csv_exports'
        ]
        
        for table in race_tables:
            try:
                count = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                summary["tables"][table] = count
            except sqlite3.OperationalError:
                print(f"Warning: Table {table} not found")
                summary["tables"][table] = 0
        
        # Get yearly statistics for regular races
        for table, label in {
            'house_races': 'House',
            'governor_races': 'Governor',
            'senate_races': 'Senate',
            'presidential_races': 'Presidential'
        }.items():
            try:
                yearly_stats = cursor.execute(f"""
                    SELECT SUBSTR(race_id, 1, 4) as year,
                           COUNT(DISTINCT race_id) as races,
                           COUNT(DISTINCT state_postal) as states,
                           COUNT(*) as candidates,
                           COUNT(DISTINCT party) as parties,
                           SUM(CASE WHEN incumbent = 1 THEN 1 ELSE 0 END) as incumbents
                    FROM {table}
                    GROUP BY SUBSTR(race_id, 1, 4)
                """).fetchall()
                
                for year, races, states, candidates, parties, incumbents in yearly_stats:
                    if year not in summary["yearly_stats"]:
                        summary["yearly_stats"][year] = {}
                    summary["yearly_stats"][year][label] = {
                        "races": races,
                        "states": states,
                        "candidates": candidates,
                        "unique_parties": parties,
                        "incumbents": incumbents
                    }
            except sqlite3.OperationalError as e:
                print(f"Warning: Error getting stats for {table}: {e}")

        # Get ballot measure statistics
        try:
            ballot_stats = cursor.execute("""
                SELECT SUBSTR(race_id, 1, 4) as year,
                       COUNT(DISTINCT race_id) as races,
                       COUNT(DISTINCT state_postal) as states,
                       COUNT(*) as options,
                       COUNT(DISTINCT category) as categories
                FROM ballot_measures
                GROUP BY SUBSTR(race_id, 1, 4)
            """).fetchall()
            
            for year, races, states, options, categories in ballot_stats:
                if year not in summary["yearly_stats"]:
                    summary["yearly_stats"][year] = {}
                summary["yearly_stats"][year]["Ballot Measures"] = {
                    "races": races,
                    "states": states,
                    "options": options,
                    "categories": categories
                }
        except sqlite3.OperationalError as e:
            print(f"Warning: Error getting ballot measure stats: {e}")

        # Get county-level statistics
        try:
            county_stats = cursor.execute("""
                SELECT SUBSTR(race_id, 1, 4) as year,
                       COUNT(DISTINCT race_id) as races,
                       COUNT(DISTINCT county_fips) as counties,
                       COUNT(DISTINCT state_postal) as states
                FROM county_results
                GROUP BY SUBSTR(race_id, 1, 4)
            """).fetchall()
            
            for year, races, counties, states in county_stats:
                if year not in summary["yearly_stats"]:
                    summary["yearly_stats"][year] = {}
                summary["yearly_stats"][year]["County Details"] = {
                    "races": races,
                    "counties": counties,
                    "states": states
                }
        except sqlite3.OperationalError as e:
            print(f"Warning: Error getting county stats: {e}")
        
        return summary

    def print_summary(self):
        """Print a formatted summary of the database contents."""
        summary = self.get_database_summary()
        
        print("\nDatabase Summary")
        print("================")
        print(f"Database: {summary['metadata']['db_path']}")
        print(f"Generated: {summary['metadata']['generated_at']}")
        
        print("\nTable Row Counts:")
        print("----------------")
        for table, count in summary["tables"].items():
            print(f"{table}: {count:,} rows")
        
        print("\nYearly Statistics:")
        print("-----------------")
        for year in sorted(summary["yearly_stats"].keys(), reverse=True):
            print(f"\nYear {year}:")
            stats = summary["yearly_stats"][year]
            
            # Regular races
            for category in ['Presidential', 'Senate', 'House', 'Governor']:
                if category in stats:
                    race_stats = stats[category]
                    print(f"  {category}:")
                    print(f"    Races: {race_stats['races']:,} across {race_stats['states']} states")
                    print(f"    Candidates: {race_stats['candidates']:,} ({race_stats['unique_parties']} parties)")
                    print(f"    Incumbents: {race_stats['incumbents']:,}")
            
            # Ballot measures
            if "Ballot Measures" in stats:
                ballot_stats = stats["Ballot Measures"]
                print(f"  Ballot Measures:")
                print(f"    Measures: {ballot_stats['races']:,} across {ballot_stats['states']} states")
                print(f"    Options: {ballot_stats['options']:,}")
                print(f"    Categories: {ballot_stats['categories']:,}")
            
            # County details
            if "County Details" in stats:
                county_stats = stats["County Details"]
                print(f"  County Details:")
                print(f"    Coverage: {county_stats['counties']:,} counties in {county_stats['states']} states")
                print(f"    Races with county data: {county_stats['races']:,}")

    def close(self):
        """Close the database connection."""
        self.conn.close()
