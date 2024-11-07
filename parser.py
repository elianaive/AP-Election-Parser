from typing import Dict, List, Union
from datetime import datetime
from collections import defaultdict

from models import PersonCandidate, BallotOptionCandidate, ElectionRace, BallotMeasure

class ElectionDataParser:
    """Parser that combines progress and metadata information."""
    
    BALLOT_MEASURE_KEYWORDS = {
        'Amendment', 'Issue', 'Question', 'Measure', 'Proposition', 'Prop'
    }
    
    @staticmethod
    def get_vote_info(data: dict, field: str, default: int = 0) -> int:
        """Safely extract vote information from either progress or metadata."""
        try:
            return data['parameters']['vote'].get(field, default)
        except (KeyError, TypeError):
            try:
                return data['vote'].get(field, default)
            except (KeyError, TypeError):
                return default

    @classmethod
    def is_ballot_measure(cls, race_meta: dict) -> bool:
        """Determine if a race is a ballot measure based on multiple criteria."""
        if race_meta.get('suppOfficeID') == 'IME':
            return True
        
        office_name = race_meta.get('officeName', '').split()
        if any(term in cls.BALLOT_MEASURE_KEYWORDS for term in office_name):
            return True
        
        candidates = race_meta.get('candidates', {}).values()
        candidate_names = {c.get('last', '').lower() for c in candidates}
        if {'yes', 'no'}.issubset(candidate_names) or {'for', 'against'}.issubset(candidate_names):
            return True
        
        return False

    @staticmethod
    def parse_candidate(prog_data: dict, meta_data: dict) -> Union[PersonCandidate, BallotOptionCandidate]:
        """Parse candidate data from progress and metadata."""
        cand_id = prog_data['candidateID']
        meta_cand = meta_data['candidates'][cand_id]
        
        base_args = {
            'candidate_id': cand_id,
            'party': meta_cand['party'],
            'ballot_order': meta_cand['ballotOrder'],
            'vote_count': prog_data['voteCount'],
            'vote_pct': prog_data['votePct'],
            'advance_total': prog_data.get('advanceTotal'),
            'color_index': prog_data.get('colorIndex')
        }
        
        # Determine if this is a ballot option or person
        if 'first' in meta_cand:
            return PersonCandidate(
                first_name=meta_cand['first'],
                last_name=meta_cand['last'],
                incumbent=meta_cand.get('incumbent', False),
                **base_args
            )
        else:
            return BallotOptionCandidate(
                option_name=meta_cand['last'],
                **base_args
            )

    @classmethod
    def parse_race(cls, race_id: str, prog_data: dict, meta_data: dict) -> Union[ElectionRace, BallotMeasure]:
        """Parse race data from progress and metadata."""
        # Get vote information
        total_votes = cls.get_vote_info(meta_data, 'total', 0)
        registered_voters = cls.get_vote_info(meta_data, 'registered', 0)
        
        # Common attributes for all race types
        base_args = {
            'race_id': race_id,
            'state_postal': prog_data['statePostal'],
            'state_name': prog_data['stateName'],
            'race_type': meta_data['raceType'],
            'race_call_status': meta_data['raceCallStatus'],
            'office_name': meta_data['officeName'],
            'last_updated': datetime.fromisoformat(prog_data['lastUpdated']),
            'precincts_reporting': prog_data['precinctsReporting'],
            'precincts_total': prog_data['precinctsTotal'],
            'precincts_reporting_pct': prog_data['precinctsReportingPct'],
            'expected_vote_pct': prog_data['eevp'],
            'total_votes': total_votes,
            'registered_voters': registered_voters,
            'key_race': meta_data.get('keyRace', False)
        }
        
        # Parse all candidates for this race
        candidates = [
            cls.parse_candidate(pc, meta_data)
            for pc in prog_data['candidates']
        ]
        
        # Determine if this is a ballot measure
        if cls.is_ballot_measure(meta_data):
            return BallotMeasure(
                description=meta_data.get('description', meta_data['officeName']),
                category=meta_data.get('category', 'Uncategorized'),
                summary=meta_data.get('summary', ''),
                designation=meta_data.get('designation', ''),
                candidates=candidates,
                **base_args
            )
        else:
            return ElectionRace(
                office_id=meta_data['officeID'],
                candidates=candidates,
                seat_name=meta_data.get('seatName'),
                seat_num=meta_data.get('seatNum'),
                incumbent_id=meta_data.get('incumbentID'),
                **base_args
            )

    @classmethod
    def parse_results(cls, progress_data: dict, metadata: dict) -> Dict[str, Union[ElectionRace, BallotMeasure]]:
        """Parse all races from progress and metadata."""
        races = {}
        
        for race_id in progress_data:
            if race_id in metadata:
                try:
                    races[race_id] = cls.parse_race(
                        race_id,
                        progress_data[race_id],
                        metadata[race_id]
                    )
                except Exception as e:
                    print(f"Error parsing race {race_id}: {e}")
                    continue
        
        return races

    @staticmethod
    def categorize_races(races: Dict[str, Union[ElectionRace, BallotMeasure]]) -> Dict[str, List[Union[ElectionRace, BallotMeasure]]]:
        """Group races by category."""
        categories = defaultdict(list)
        
        for race in races.values():
            if isinstance(race, BallotMeasure):
                categories['Ballot Measures'].append(race)
            else:
                if race.office_id == 'P':
                    categories['Presidential'].append(race)
                elif race.office_id == 'S':
                    categories['Senate'].append(race)
                elif race.office_id == 'H':
                    categories['House'].append(race)
                elif race.office_id == 'G':
                    categories['Governor'].append(race)
                else:
                    categories['Other'].append(race)
        
        return categories