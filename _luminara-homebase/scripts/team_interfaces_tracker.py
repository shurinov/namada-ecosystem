#!/usr/bin/env python3

import json
import os
import re
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict

# POSIX paths for GitHub Actions/Ubuntu
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INTERFACE_STATUS_PATH = os.path.join(BASE_PATH, "interface-status.json")
STATE_PATH = os.path.join(BASE_PATH, "team_state.json")  # Separate state file for team tracker
TEAM_DATA_PATH = os.path.join(BASE_PATH, "team-data")
JSON_OUTPUT_PATH = os.path.join(TEAM_DATA_PATH, "json")
SQL_OUTPUT_PATH = os.path.join(TEAM_DATA_PATH, "sql")

# Set which networks to track. Example: ["namada"] or ["namada", "housefire"]
TRACKED_NETWORKS = ["namada"]  # Only mainnet by default
# To enable housefire, use: TRACKED_NETWORKS = ["namada", "housefire"]

IGNORED_FIELDS = {
    "latest_block_height",  # handled specially in settings
    "script_start_time",
    "script_end_time",
    "reference_latest_block_height"
}

def filter_networks(state: dict, networks: list) -> dict:
    if state and "networks" in state:
        filtered = [n for n in state["networks"] if n.get("network") in networks]
        state = dict(state)  # shallow copy
        state["networks"] = filtered
    return state

def load_json_file(path: str) -> dict:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_json_file(data: dict, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)

def append_to_json_file(data: List[Dict[str, Any]], path: str) -> None:
    """Append data to existing JSON file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    # Load existing data
    try:
        with open(path, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
            if not isinstance(existing_data, list):
                existing_data = []
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = []
    
    # Append new data
    existing_data.extend(data)
    
    # Save back to file
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, indent=4)

def append_to_file(content: str, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'a', encoding='utf-8') as f:
        f.write(content)

def get_change_info(path_parts: List[str], current_state: dict = None) -> Tuple[Optional[str], Optional[str], str]:
    team = None
    service = None
    field = path_parts[-1]
    if path_parts[0] == "required_versions":
        service = path_parts[1] if len(path_parts) > 1 else None
        field = path_parts[-1]
    elif path_parts[0] == "networks":
        try:
            if "team" in path_parts:
                team_idx = path_parts.index("team")
                if team_idx + 1 < len(path_parts):
                    team = path_parts[team_idx + 1]
                    if "service" in path_parts[team_idx:]:
                        service_idx = path_parts.index("service", team_idx)
                        if service_idx + 1 < len(path_parts):
                            service = path_parts[service_idx + 1]
                    else:
                        service = "interface"
            if team is None and current_state and "networks" in current_state:
                for i, part in enumerate(path_parts):
                    if part == "interface" and i + 1 < len(path_parts) and path_parts[i + 1].isdigit():
                        interface_idx = int(path_parts[i + 1])
                        interface_data = current_state["networks"][0]["interface"][interface_idx]
                        team = interface_data["team"]
                        if "settings" in path_parts:
                            settings_idx = path_parts.index("settings")
                            if settings_idx + 1 < len(path_parts) and path_parts[settings_idx + 1].isdigit():
                                service_idx = int(path_parts[settings_idx + 1])
                                if service_idx < len(interface_data["settings"]):
                                    service = interface_data["settings"][service_idx]["service"]
                        else:
                            service = "interface"
        except (KeyError, IndexError, ValueError):
            pass
    return team, service, field

def build_readable_path(path_parts: List[str], operator: Optional[str], service: Optional[str]) -> str:
    if path_parts[0] == "required_versions":
        return ".".join(path_parts)
    if not operator:
        return ".".join(path_parts)
    if service == "interface":
        return f"namada.operator.{operator}.interface.{path_parts[-1]}"
    elif service:
        return f"namada.operator.{operator}.service.{service}.{path_parts[-1]}"
    else:
        return f"namada.operator.{operator}.{'.'.join(path_parts)}"

def create_change_record(path_parts: List[str], change_type: str, old_value: Any, new_value: Any, state: dict = None) -> dict:
    team, service, field = get_change_info(path_parts, state)
    full_path = build_readable_path(path_parts, team, service)
    
    return {
        "team": team,
        "service": service,
        "field": field,
        "full_path": full_path,
        "type": change_type,
        "old_value": old_value,
        "new_value": new_value
    }

def detect_changes(old_state: dict, new_state: dict, path: List[str] = None, root_state: dict = None) -> List[dict]:
    if path is None:
        path = []
    if root_state is None:
        root_state = new_state
    
    changes = []
    
    if isinstance(old_state, dict) and isinstance(new_state, dict):
        # Check for added keys
        for key in new_state:
            if key not in old_state:
                current_path = path + [key]
                if key not in IGNORED_FIELDS:
                    changes.append(create_change_record(
                        current_path,
                        "added",
                        None,
                        new_state[key],
                        root_state
                    ))
        
        # Check for removed keys
        for key in old_state:
            if key not in new_state:
                current_path = path + [key]
                if key not in IGNORED_FIELDS:
                    changes.append(create_change_record(
                        current_path,
                        "removed",
                        old_state[key],
                        None,
                        root_state
                    ))
        
        # Check for modified values
        for key in old_state:
            if key in new_state and key not in IGNORED_FIELDS:
                current_path = path + [key]
                if old_state[key] != new_state[key]:
                    changes.extend(detect_changes(
                        old_state[key],
                        new_state[key],
                        current_path,
                        root_state
                    ))
    
    elif isinstance(old_state, list) and isinstance(new_state, list):
        # Check for changes in list items
        for i in range(min(len(old_state), len(new_state))):
            current_path = path + [str(i)]
            changes.extend(detect_changes(
                old_state[i],
                new_state[i],
                current_path,
                root_state
            ))
        
        # Check for added items
        for i in range(len(old_state), len(new_state)):
            current_path = path + [str(i)]
            changes.append(create_change_record(
                current_path,
                "added",
                None,
                new_state[i],
                root_state
            ))
        
        # Check for removed items
        for i in range(len(new_state), len(old_state)):
            current_path = path + [str(i)]
            changes.append(create_change_record(
                current_path,
                "removed",
                old_state[i],
                None,
                root_state
            ))
    
    elif old_state != new_state:
        # Simple value change
        changes.append(create_change_record(
            path,
            "modified",
            old_state,
            new_state,
            root_state
        ))
    
    return changes

def generate_sql_statement(change: dict, timestamp: str) -> str:
    team_value = "'{}'".format(change['team']) if change['team'] else 'null'
    service_value = "'{}'".format(change['service']) if change['service'] else 'null'
    return (
        "INSERT INTO interface_changes "
        "(timestamp, team, service, field, full_path, change_type, old_value, new_value) "
        "VALUES ('{}', {}, {}, '{}', '{}', '{}', '{}', '{}');\n"
    ).format(
        timestamp,
        team_value,
        service_value,
        change['field'],
        change['full_path'],
        change['type'],
        json.dumps(change['old_value']),
        json.dumps(change['new_value'])
    )

def save_team_changes(team_changes: Dict[str, List[dict]], timestamp: str) -> None:
    """Save changes to team-specific files."""
    for team, changes in team_changes.items():
        if not changes:
            continue
            
        # Skip entries with team "-"
        if team == "-":
            continue
            
        # Sanitize team name for filename
        if team is None:
            safe_team_name = "required_versions"
        else:
            safe_team_name = re.sub(r'[^\w\-]', '_', team)
        
        # Create team entry
        team_entry = {
            "timestamp": timestamp,
            "type": "change",
            "changes": changes
        }
        
        # Append to team JSON file
        json_path = os.path.join(JSON_OUTPUT_PATH, f"{safe_team_name}.json")
        append_to_json_file([team_entry], json_path)
        
        # Generate and append SQL statements
        sql_statements = []
        for change in changes:
            sql_statements.append(generate_sql_statement(change, timestamp))
        
        sql_path = os.path.join(SQL_OUTPUT_PATH, f"{safe_team_name}.sql")
        append_to_file("".join(sql_statements), sql_path)
        
        print(f"Updated {safe_team_name}: {len(changes)} changes, {len(sql_statements)} SQL statements")

def main():
    print("Starting team interface tracker...")
    print("Reading from: {}".format(INTERFACE_STATUS_PATH))
    
    timestamp = datetime.now(timezone.utc).isoformat() + "Z"
    current_state = load_json_file(INTERFACE_STATUS_PATH)
    if not current_state:
        print("Error: Could not load interface status")
        return
    
    previous_state = load_json_file(STATE_PATH)
    is_initial = not previous_state

    # Filter networks based on TRACKED_NETWORKS
    current_state = filter_networks(current_state, TRACKED_NETWORKS)
    previous_state = filter_networks(previous_state, TRACKED_NETWORKS) if previous_state else previous_state

    if is_initial:
        print("Initial run detected - recording complete state")
        
        # Handle initial state for each team
        team_initial_states = defaultdict(list)
        networks = current_state.get("networks", [])
        
        for network in networks:
            interfaces = network.get("interface", [])
            for interface in interfaces:
                team = interface.get("team", "unknown")
                
                # Create initial entry for this team
                team_entry = {
                    "timestamp": timestamp,
                    "type": "initial",
                    "team": team,
                    "interface_data": interface
                }
                team_initial_states[team].append(team_entry)
        
        # Save initial states to team files
        for team, entries in team_initial_states.items():
            # Skip entries with team "-"
            if team == "-":
                continue
                
            if team is None:
                safe_team_name = "required_versions"
            else:
                safe_team_name = re.sub(r'[^\w\-]', '_', team)
            
            json_path = os.path.join(JSON_OUTPUT_PATH, f"{safe_team_name}.json")
            append_to_json_file(entries, json_path)
            
            # Generate initial SQL statement
            sql_statement = (
                "INSERT INTO interface_changes "
                "(timestamp, team, service, field, full_path, change_type, old_value, new_value) "
                "VALUES ('{}', '{}', 'interface', 'initial_state', 'namada.operator.{}.interface', 'initial', 'null', '{}');\n"
            ).format(
                timestamp,
                team or 'null',
                team or 'unknown',
                json.dumps(interface)
            )
            
            sql_path = os.path.join(SQL_OUTPUT_PATH, f"{safe_team_name}.sql")
            append_to_file(sql_statement, sql_path)
            
            print(f"Initial state for {safe_team_name}: {len(entries)} entries")
        
        change_count = len(team_initial_states)
    else:
        detected_changes = detect_changes(previous_state, current_state)
        if detected_changes:
            # Group changes by team
            team_changes = defaultdict(list)
            for change in detected_changes:
                team = change.get('team', 'unknown')
                team_changes[team].append(change)
            
            # Save changes to team-specific files
            save_team_changes(team_changes, timestamp)
            change_count = len(detected_changes)
        else:
            change_count = 0
            print("No changes detected")
    
    if change_count > 0:
        print("Recorded {} changes at {}".format(change_count, timestamp))
    else:
        print("No changes detected at {}".format(timestamp))
    
    save_json_file(current_state, STATE_PATH)
    print("Updated {}".format(STATE_PATH))
    print("Done!")

if __name__ == "__main__":
    main()
