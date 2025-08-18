#!/usr/bin/env python3

import json
import os
import re
from datetime import datetime, timezone
from typing import Dict, List, Any, Set
from collections import defaultdict

# Paths
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHANGES_JSON_PATH = os.path.join(BASE_PATH, "changes.json")
TEAM_DATA_PATH = os.path.join(BASE_PATH, "team-data")
JSON_OUTPUT_PATH = os.path.join(TEAM_DATA_PATH, "json")
SQL_OUTPUT_PATH = os.path.join(TEAM_DATA_PATH, "sql")
GAP_FILLER_STATE_PATH = os.path.join(BASE_PATH, "gap_filler_state.json")

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

def append_to_json_file(data: List[Dict[str, Any]], path: str) -> None:
    """Append data to existing JSON file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    # Load existing data
    existing_data = load_json_file(path)
    
    # Append new data
    existing_data.extend(data)
    
    # Save back to file
    save_json_file(existing_data, path)

def load_gap_filler_state() -> Dict[str, Any]:
    """Load gap filler state to track last processed timestamp."""
    try:
        with open(GAP_FILLER_STATE_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        # Return default state if file doesn't exist
        return {
            "last_processed_timestamp": None,
            "migration_completed": False,
            "total_entries_processed": 0
        }

def save_gap_filler_state(state: Dict[str, Any]) -> None:
    """Save gap filler state."""
    with open(GAP_FILLER_STATE_PATH, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=4)

def parse_changes_by_team(changes_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Parse changes data and separate by team (same logic as parse_teams.py)."""
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

def get_new_entries_since_timestamp(changes_data: List[Dict[str, Any]], last_timestamp: str) -> List[Dict[str, Any]]:
    """Get entries that are newer than the last processed timestamp."""
    if not last_timestamp:
        return changes_data
    
    new_entries = []
    for entry in changes_data:
        entry_timestamp = entry.get("timestamp", "")
        if entry_timestamp > last_timestamp:
            new_entries.append(entry)
    
    return new_entries

def main():
    print("Starting gap filler...")
    print(f"Reading from: {CHANGES_JSON_PATH}")
    
    # Load gap filler state
    state = load_gap_filler_state()
    last_timestamp = state.get("last_processed_timestamp")
    migration_completed = state.get("migration_completed", False)
    
    if not migration_completed:
        print("Migration not completed yet. Gap filler will wait for migration to complete.")
        print("To mark migration as complete, set 'migration_completed': true in gap_filler_state.json")
        return
    
    print(f"Last processed timestamp: {last_timestamp or 'None (first run)'}")
    
    # Load changes data
    changes_data = load_json_file(CHANGES_JSON_PATH)
    if not changes_data:
        print("Error: Could not load changes.json")
        return
    
    print(f"Loaded {len(changes_data)} total entries from changes.json")
    
    # Get new entries since last run
    new_entries = get_new_entries_since_timestamp(changes_data, last_timestamp)
    print(f"Found {len(new_entries)} new entries since last run")
    
    if not new_entries:
        print("No new entries to process")
        return
    
    # Parse new entries by team
    team_changes = parse_changes_by_team(new_entries)
    print(f"Parsed changes for {len(team_changes)} teams")
    
    # Append to team-specific files
    entries_processed = 0
    for team, data in team_changes.items():
        if data:
            # Sanitize team name for filename
            if team is None:
                safe_team_name = "required_versions"
            else:
                safe_team_name = re.sub(r'[^\w\-]', '_', team)
            
            json_path = os.path.join(JSON_OUTPUT_PATH, f"{safe_team_name}.json")
            append_to_json_file(data, json_path)
            entries_processed += len(data)
            print(f"Appended {len(data)} entries to {json_path}")
    
    # Update state
    if new_entries:
        latest_timestamp = max(entry.get("timestamp", "") for entry in new_entries)
        state["last_processed_timestamp"] = latest_timestamp
        state["total_entries_processed"] = state.get("total_entries_processed", 0) + entries_processed
        save_gap_filler_state(state)
        print(f"Updated last processed timestamp to: {latest_timestamp}")
    
    # Print summary
    print("\n=== Gap Filler Summary ===")
    print(f"New entries processed: {len(new_entries)}")
    print(f"Team entries appended: {entries_processed}")
    print(f"Total entries processed since migration: {state.get('total_entries_processed', 0)}")
    print("Done!")

if __name__ == "__main__":
    main()
