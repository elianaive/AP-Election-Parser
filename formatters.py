from typing import Union, Dict, List, Optional, TextIO
import csv
import os
from datetime import datetime
from models import ElectionRace, BallotMeasure, PersonCandidate, BallotOptionCandidate

class ConsoleFormatter:
    """Formats election data for display or file output."""
    
    @staticmethod
    def format_race_summary(race: Union[ElectionRace, BallotMeasure], show_description: bool = False) -> str:
        """Format a race summary as a string."""
        lines = []
        lines.append(f"\n{race.state_name} - {race.office_name}")
        lines.append(f"AP ID: {race.race_id}")
        
        if show_description and isinstance(race, BallotMeasure):
            if race.description != race.office_name:
                lines.append(f"Description: {race.description}")
            if race.summary:
                lines.append(f"Summary: {race.summary}")
                
        lines.append(f"Reporting: {race.precincts_reporting_pct:.1f}% "
                    f"({race.precincts_reporting:,}/{race.precincts_total:,} precincts)")
        lines.append(f"Last Updated: {race.last_updated.strftime('%I:%M %p')} ET")
        lines.append(f"Status: {race.race_call_status}")
        
        sorted_candidates = sorted(race.candidates, key=lambda x: x.vote_pct, reverse=True)
        
        for candidate in sorted_candidates:
            if isinstance(candidate, PersonCandidate):
                name = f"{candidate.first_name} {candidate.last_name}"
                incumbent = "*" if candidate.incumbent else " "
                lines.append(f"  {incumbent}{name:<30} ({candidate.party:<3}): "
                           f"{candidate.vote_count:>8,} votes ({candidate.vote_pct:>5.1f}%)")
            else:
                lines.append(f"  {candidate.option_name:<32}: "
                           f"{candidate.vote_count:>8,} votes ({candidate.vote_pct:>5.1f}%)")
                
        return "\n".join(lines)

    @staticmethod
    def write_results(categorized_races: dict, output_file: Optional[TextIO] = None) -> None:
        """Write formatted results to file or stdout."""
        def write(text: str) -> None:
            if output_file:
                output_file.write(text + "\n")
            print(text)
        
        # Write summary statistics
        write("\n=== Election Results Summary ===")
        total_races = sum(len(races) for races in categorized_races.values())
        write(f"Total Races: {total_races}")
        for category, race_list in categorized_races.items():
            write(f"{category}: {len(race_list)} races")
        
        # Write detailed race information
        formatter = ConsoleFormatter()
        for category, race_list in categorized_races.items():
            write(f"\n\n=== {category} ===")
            
            sorted_races = sorted(
                race_list,
                key=lambda x: (not x.key_race, -x.precincts_reporting_pct)
            )
            
            for race in sorted_races[:5]:
                write(formatter.format_race_summary(
                    race,
                    show_description=(category == 'Ballot Measures')
                ))
                write("-" * 80)
                
            if len(sorted_races) > 5:
                write(f"...and {len(sorted_races) - 5} more {category} races")


class CSVFormatter:
    """Formats election data into type-specific CSVs."""
    
    PRESIDENT_HEADERS = [
        'race_id', 'state_postal', 'state_name', 'race_call_status', 'last_updated',
        'precincts_reporting', 'precincts_total', 'precincts_reporting_pct',
        'expected_vote_pct', 'total_votes', 'candidate_id', 'first_name', 'last_name',
        'party', 'incumbent', 'vote_count', 'vote_pct'
    ]
    
    CONGRESS_HEADERS = [
        'race_id', 'state_postal', 'state_name', 'office_id', 'seat_name', 'seat_num',
        'race_call_status', 'last_updated', 'precincts_reporting', 'precincts_total',
        'precincts_reporting_pct', 'expected_vote_pct', 'total_votes', 'candidate_id',
        'first_name', 'last_name', 'party', 'incumbent', 'vote_count', 'vote_pct'
    ]
    
    GOVERNOR_HEADERS = [
        'race_id', 'state_postal', 'state_name', 'race_call_status', 'last_updated',
        'precincts_reporting', 'precincts_total', 'precincts_reporting_pct',
        'expected_vote_pct', 'total_votes', 'candidate_id', 'first_name', 'last_name',
        'party', 'incumbent', 'vote_count', 'vote_pct'
    ]
    
    BALLOT_HEADERS = [
        'race_id', 'state_postal', 'state_name', 'description', 'category', 'summary',
        'race_call_status', 'last_updated', 'precincts_reporting', 'precincts_total',
        'precincts_reporting_pct', 'expected_vote_pct', 'total_votes', 'candidate_id',
        'option_name', 'vote_count', 'vote_pct'
    ]

    @classmethod
    def write_results(cls, categorized_races: Dict[str, List[Union[ElectionRace, BallotMeasure]]], 
                     data_dir: str = "data") -> None:
        """Write race data to separate CSV files by type."""
        os.makedirs(data_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Initialize row collections
        presidential_rows = []
        senate_rows = []
        house_rows = []
        governor_rows = []
        ballot_rows = []
        
        # Process each category
        for category, races in categorized_races.items():
            for race in races:
                if isinstance(race, BallotMeasure):
                    for candidate in race.candidates:
                        row = {
                            'race_id': race.race_id,
                            'state_postal': race.state_postal,
                            'state_name': race.state_name,
                            'description': race.description,
                            'category': race.category,
                            'summary': race.summary,
                            'race_call_status': race.race_call_status,
                            'last_updated': race.last_updated.isoformat(),
                            'precincts_reporting': race.precincts_reporting,
                            'precincts_total': race.precincts_total,
                            'precincts_reporting_pct': race.precincts_reporting_pct,
                            'expected_vote_pct': race.expected_vote_pct,
                            'total_votes': race.total_votes,
                            'candidate_id': candidate.candidate_id,
                            'option_name': candidate.option_name,
                            'vote_count': candidate.vote_count,
                            'vote_pct': candidate.vote_pct
                        }
                        ballot_rows.append(row)
                else:
                    base_row = {
                        'race_id': race.race_id,
                        'state_postal': race.state_postal,
                        'state_name': race.state_name,
                        'race_call_status': race.race_call_status,
                        'last_updated': race.last_updated.isoformat(),
                        'precincts_reporting': race.precincts_reporting,
                        'precincts_total': race.precincts_total,
                        'precincts_reporting_pct': race.precincts_reporting_pct,
                        'expected_vote_pct': race.expected_vote_pct,
                        'total_votes': race.total_votes,
                    }
                    
                    for candidate in race.candidates:
                        if isinstance(candidate, PersonCandidate):
                            row = base_row.copy()
                            row.update({
                                'candidate_id': candidate.candidate_id,
                                'first_name': candidate.first_name,
                                'last_name': candidate.last_name,
                                'party': candidate.party,
                                'incumbent': candidate.incumbent,
                                'vote_count': candidate.vote_count,
                                'vote_pct': candidate.vote_pct
                            })
                            
                            if race.office_id == 'P':
                                presidential_rows.append(row)
                            elif race.office_id == 'S':
                                row.update({
                                    'office_id': race.office_id,
                                    'seat_name': race.seat_name,
                                    'seat_num': race.seat_num
                                })
                                senate_rows.append(row)
                            elif race.office_id == 'H':
                                row.update({
                                    'office_id': race.office_id,
                                    'seat_name': race.seat_name,
                                    'seat_num': race.seat_num
                                })
                                house_rows.append(row)
                            elif race.office_id == 'G':
                                governor_rows.append(row)
        
        # Write files
        if presidential_rows:
            filename = os.path.join(data_dir, f'president_{timestamp}.csv')
            cls.write_csv(filename, cls.PRESIDENT_HEADERS, presidential_rows)
            
        if senate_rows:
            filename = os.path.join(data_dir, f'senate_{timestamp}.csv')
            cls.write_csv(filename, cls.CONGRESS_HEADERS, senate_rows)
            
        if house_rows:
            filename = os.path.join(data_dir, f'house_{timestamp}.csv')
            cls.write_csv(filename, cls.CONGRESS_HEADERS, house_rows)
            
        if governor_rows:
            filename = os.path.join(data_dir, f'governor_{timestamp}.csv')
            cls.write_csv(filename, cls.GOVERNOR_HEADERS, governor_rows)
            
        if ballot_rows:
            filename = os.path.join(data_dir, f'ballot_{timestamp}.csv')
            cls.write_csv(filename, cls.BALLOT_HEADERS, ballot_rows)

    @staticmethod
    def write_csv(filename: str, headers: List[str], rows: List[dict]) -> None:
        """Write data to CSV file."""
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Wrote {len(rows)} rows to {filename}")