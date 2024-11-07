from dataclasses import dataclass
from typing import List, Optional, Dict
from datetime import datetime

@dataclass
class PersonCandidate:
    """Candidate that is a person (not a ballot option)."""
    candidate_id: str
    party: str
    ballot_order: int
    vote_count: int
    vote_pct: float
    first_name: str
    last_name: str
    incumbent: bool = False
    advance_total: Optional[int] = None
    color_index: Optional[int] = None

@dataclass
class BallotOptionCandidate:
    """Candidate that is a ballot option (e.g., Yes/No)."""
    candidate_id: str
    party: str
    ballot_order: int
    vote_count: int
    vote_pct: float
    option_name: str
    advance_total: Optional[int] = None
    color_index: Optional[int] = None

@dataclass
class ElectionRace:
    """Regular election race."""
    race_id: str
    state_postal: str
    state_name: str
    race_type: str
    race_call_status: str
    office_name: str
    office_id: str
    last_updated: datetime
    precincts_reporting: int
    precincts_total: int
    precincts_reporting_pct: float
    expected_vote_pct: float
    total_votes: int
    registered_voters: int
    candidates: List[PersonCandidate]
    key_race: bool = False
    seat_name: Optional[str] = None
    seat_num: Optional[str] = None
    incumbent_id: Optional[str] = None
    
@dataclass
class BallotMeasure:
    """Ballot measure race."""
    race_id: str
    state_postal: str
    state_name: str
    race_type: str
    race_call_status: str
    office_name: str
    last_updated: datetime
    precincts_reporting: int
    precincts_total: int
    precincts_reporting_pct: float
    expected_vote_pct: float
    total_votes: int
    registered_voters: int
    description: str
    category: str
    summary: str
    designation: str
    candidates: List[BallotOptionCandidate]
    key_race: bool = False

@dataclass
class CountyResult:
    """Represents county-level election results."""
    state_postal: str
    county_name: str
    county_fips: str
    county_id: str
    precincts_reporting: int
    precincts_total: int
    precincts_reporting_pct: float
    expected_vote_pct: float
    total_votes: int
    registered_voters: int
    last_updated: datetime
    candidate_votes: Dict[str, Dict[str, float]] 