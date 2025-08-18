#!/usr/bin/env python3

import json
import os
import re
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Any, Set

# Paths
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHANGES_JSON_PATH = os.path.join(BASE_PATH, "changes.json")
CHANGES_SQL_PATH = os.path.join(BASE_PATH, "changes.sql")
TEAM_DATA_PATH = os.path.join(BASE_PATH, "team-data")
JSON_OUTPUT_PATH = os.path.join(TEAM_DATA_PATH, "json")
SQL_OUTPUT_PATH = os.path.join(TEAM_DATA_PATH, "sql")

def load_json_file(path: str) -> List[Dict[str, Any]]:
    """Load JSON file and return as list."""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if not isinstance(data, list):
                print(f"Warning: Expected list, got {type(data)}")
                return []
            return data
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading {path}: {e}")
        return []

def save_json_file(data: List[Dict[str, Any]], path: str) -> None:
    """Save data to JSON file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)

def extract_teams_from_changes(changes_data: List[Dict[str, Any]]) -> Set[str]:
    """Extract all unique team names from changes data."""
    teams = set()
    
    for entry in changes_data:
        if entry.get("type") == "initial":
            # For initial entries, extract teams from the state data
            state = entry.get("state", {})
            networks = state.get("networks", [])
            for network in networks:
                interfaces = network.get("interface", [])
                for interface in interfaces:
                    team = interface.get("team")
                    if team and team != "-":  # Filter out "-" team
                        teams.add(team)
        
        elif "changes" in entry:
            # For change entries, extract teams from change records
            changes = entry.get("changes", [])
            for change in changes:
                team = change.get("team")
                if team and team != "-":  # Filter out "-" team
                    teams.add(team)
    
    return teams

def parse_changes_by_team(changes_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Parse changes data and separate by team."""
    team_data = defaultdict(list)
    
    for entry in changes_data:
        timestamp = entry.get("timestamp", "")
        
        if entry.get("type") == "initial":
            # Handle initial state entries
            state = entry.get("state", {})
            networks = state.get("networks", [])
            
            for network in networks:
                interfaces = network.get("interface", [])
                for interface in interfaces:
                    team = interface.get("team", "required_versions")
                    
                    # Skip entries with team "-"
                    if team == "-":
                        continue
                    
                    # Create initial entry for this team
                    team_entry = {
                        "timestamp": timestamp,
                        "type": "initial",
                        "team": team,
                        "interface_data": interface
                    }
                    team_data[team].append(team_entry)
        
        elif "changes" in entry:
            # Handle change entries
            changes = entry.get("changes", [])
            
            for change in changes:
                team = change.get("team", "required_versions")
                
                # Skip entries with team "-"
                if team == "-":
                    continue
                
                # Create change entry for this team
                team_entry = {
                    "timestamp": timestamp,
                    "type": "change",
                    "change_data": change
                }
                team_data[team].append(team_entry)
    
    return dict(team_data)

def parse_sql_by_team(sql_file_path: str) -> Dict[str, List[str]]:
    """Parse SQL file and separate INSERT statements by team."""
    team_sql = defaultdict(list)
    
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Split by semicolon to get individual SQL statements
        statements = content.split(';')
        
        for statement in statements:
            statement = statement.strip()
            if not statement or not statement.startswith('INSERT INTO interface_changes'):
                continue
                
            # Extract team from SQL statement
            # Format: INSERT INTO interface_changes (timestamp, team, service, field, full_path, change_type, old_value, new_value) VALUES ('...', 'team_name', ...)
            team_match = re.search(r"'([^']*)', '([^']*)', '([^']*)'", statement)
            if team_match:
                team = team_match.group(2)  # team is the second captured group
                if team and team != 'null' and team != '-':
                    team_sql[team].append(statement + ';')
                elif team == 'null':
                    # Handle null team entries
                    team_sql["required_versions"].append(statement + ';')
                # Skip entries with team "-"
    
    except FileNotFoundError as e:
        print(f"Warning: SQL file not found: {e}")
    
    return dict(team_sql)

def main():
    print("Starting team data parsing...")
    print(f"Reading from: {CHANGES_JSON_PATH}")
    
    # Load changes data
    changes_data = load_json_file(CHANGES_JSON_PATH)
    if not changes_data:
        print("Error: Could not load changes.json")
        return
    
    print(f"Loaded {len(changes_data)} entries from changes.json")
    
    # Extract all teams
    teams = extract_teams_from_changes(changes_data)
    print(f"Found {len(teams)} unique teams: {sorted(teams)}")
    
    # Parse changes by team
    team_changes = parse_changes_by_team(changes_data)
    print(f"Parsed changes for {len(team_changes)} teams")
    
    # Parse SQL by team
    team_sql = parse_sql_by_team(CHANGES_SQL_PATH)
    print(f"Parsed SQL for {len(team_sql)} teams")
    
    # Save team-specific JSON files
    for team, data in team_changes.items():
        # Sanitize team name for filename
        if team is None:
            safe_team_name = "required_versions"
        else:
            safe_team_name = re.sub(r'[^\w\-]', '_', team)
        json_path = os.path.join(JSON_OUTPUT_PATH, f"{safe_team_name}.json")
        save_json_file(data, json_path)
        print(f"Saved {len(data)} entries to {json_path}")
    
    # Save team-specific SQL files
    for team, statements in team_sql.items():
        # Sanitize team name for filename
        if team is None:
            safe_team_name = "required_versions"
        else:
            safe_team_name = re.sub(r'[^\w\-]', '_', team)
        sql_path = os.path.join(SQL_OUTPUT_PATH, f"{safe_team_name}.sql")
        
        os.makedirs(os.path.dirname(sql_path), exist_ok=True)
        with open(sql_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(statements))
        
        print(f"Saved {len(statements)} SQL statements to {sql_path}")
    
    # Print summary
    print("\n=== Summary ===")
    print(f"Total teams processed: {len(teams)}")
    print(f"JSON files created: {len(team_changes)}")
    print(f"SQL files created: {len(team_sql)}")
    print(f"Output directory: {TEAM_DATA_PATH}")
    print("Done!")

if __name__ == "__main__":
    main()
