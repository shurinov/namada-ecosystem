#!/usr/bin/env python3
"""
Script to convert team-specific JSON files to CSV format.
Tracks initial state and applies changes over time to create a timeline of field values.
"""

import json
import csv
import os
from typing import Dict, List, Any, Set
from collections import defaultdict
import glob

def extract_initial_state(initial_entry: Dict[str, Any]) -> Dict[str, Any]:
    """Extract all field values from the initial state entry."""
    state = {}
    
    if 'state' not in initial_entry or 'networks' not in initial_entry['state']:
        return state
    
    for network in initial_entry['state']['networks']:
        network_name = network.get('network', 'unknown')
        
        # Extract interface data
        if 'interface' in network:
            for interface in network['interface']:
                team = interface.get('team', 'unknown')
                # Interface level fields
                state[f"{network_name}.interface.{team}.discord"] = interface.get('discord', '')
                state[f"{network_name}.interface.{team}.url"] = interface.get('url', '')
                state[f"{network_name}.interface.{team}.status"] = interface.get('status', '')
                state[f"{network_name}.interface.{team}.version"] = interface.get('version', '')
                state[f"{network_name}.interface.{team}.is_up_to_date"] = interface.get('is_up_to_date', '')
                
                # Settings level fields
                if 'settings' in interface:
                    for setting in interface['settings']:
                        service = setting.get('service', 'unknown')
                        state[f"{network_name}.{service}.{team}.url"] = setting.get('url', '')
                        state[f"{network_name}.{service}.{team}.status"] = setting.get('status', '')
                        state[f"{network_name}.{service}.{team}.version"] = setting.get('version', '')
                        state[f"{network_name}.{service}.{team}.is_up_to_date"] = setting.get('is_up_to_date', '')
                        state[f"{network_name}.{service}.{team}.latest_block_height"] = setting.get('latest_block_height', '')
                        state[f"{network_name}.{service}.{team}.sync_state"] = setting.get('sync_state', '')
                        state[f"{network_name}.{service}.{team}.namada_version"] = setting.get('namada_version', '')
        
        # Extract direct service data (rpc, indexer, masp)
        for service_type in ['rpc', 'indexer', 'masp']:
            if service_type in network:
                for service in network[service_type]:
                    team = service.get('team', 'unknown')
                    state[f"{network_name}.{service_type}.{team}.url"] = service.get('url', '')
                    state[f"{network_name}.{service_type}.{team}.status"] = service.get('status', '')
                    state[f"{network_name}.{service_type}.{team}.version"] = service.get('version', '')
                    state[f"{network_name}.{service_type}.{team}.is_up_to_date"] = service.get('is_up_to_date', '')
                    state[f"{network_name}.{service_type}.{team}.latest_block_height"] = service.get('latest_block_height', '')
                    state[f"{network_name}.{service_type}.{team}.sync_state"] = service.get('sync_state', '')
                    state[f"{network_name}.{service_type}.{team}.namada_version"] = service.get('namada_version', '')
    
    return state

def apply_changes_to_state(state: Dict[str, Any], changes: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Apply changes to the current state."""
    updated_state = state.copy()
    
    for change in changes:
        full_path = change.get('full_path', '')
        new_value = change.get('new_value', '')
        
        if full_path:
            # Convert full_path to our state key format
            # Two possible formats:
            # 1. "namada.operator.TeamName.interface.field" (5 parts) -> "namada.interface.TeamName.field"
            # 2. "namada.operator.TeamName.service.ServiceType.field" (6 parts) -> "namada.ServiceType.TeamName.field"
            parts = full_path.split('.')
            if len(parts) == 5:  # namada.operator.TeamName.interface.field
                network = parts[0]  # namada
                team = parts[2]     # TeamName
                service = parts[3]  # interface
                field = parts[4]    # field
                
                state_key = f"{network}.{service}.{team}.{field}"
                updated_state[state_key] = new_value
                
            elif len(parts) >= 6:  # namada.operator.TeamName.service.ServiceType.field
                network = parts[0]  # namada
                team = parts[2]     # TeamName
                service = parts[4]  # ServiceType (indexer, masp, rpc, etc.)
                field = parts[5]    # field
                
                state_key = f"{network}.{service}.{team}.{field}"
                updated_state[state_key] = new_value
    
    return updated_state

def should_exclude_column(column_name: str) -> bool:
    """Check if a column should be excluded from the CSV output."""
    # Columns to exclude (these will be matched for any team name)
    exclude_patterns = [
        '.latest_block_height',
        '.namada_version', 
        '.service',
        '.url',
        '.discord'
    ]
    
    for pattern in exclude_patterns:
        if pattern in column_name:
            return True
    
    return False

def filter_columns(columns: List[str]) -> List[str]:
    """Filter out unwanted columns from the column list."""
    return [col for col in columns if not should_exclude_column(col)]

def process_team_file(file_path: str, output_dir: str) -> None:
    """Process a single team JSON file and convert it to CSV."""
    team_name = os.path.basename(file_path).replace('.json', '')
    print(f"Processing {team_name}...")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not data:
            print(f"No data found in {file_path}")
            return
        
        # Extract initial state
        current_state = extract_initial_state(data[0])
        initial_timestamp = data[0].get('timestamp', '')
        
        # Prepare CSV output
        csv_filename = f"{team_name}.csv"
        csv_path = os.path.join(output_dir, csv_filename)
        
        # Get all possible field names for consistent columns
        all_fields = set(current_state.keys())
        
        # Process all entries to get complete field list
        for entry in data[1:]:  # Skip initial entry
            if 'changes' in entry:
                for change in entry['changes']:
                    full_path = change.get('full_path', '')
                    if full_path:
                        parts = full_path.split('.')
                        if len(parts) == 5:  # namada.operator.TeamName.interface.field
                            network = parts[0]
                            team = parts[2]
                            service = parts[3]
                            field = parts[4]
                            state_key = f"{network}.{service}.{team}.{field}"
                            all_fields.add(state_key)
                        elif len(parts) >= 6: # namada.operator.TeamName.service.ServiceType.field
                            network = parts[0]
                            team = parts[2]
                            service = parts[4]
                            field = parts[5]
                            state_key = f"{network}.{service}.{team}.{field}"
                            all_fields.add(state_key)
        
        # Sort fields for consistent column order and filter out unwanted columns
        sorted_fields = sorted(all_fields)
        filtered_fields = filter_columns(sorted_fields)
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            header = ['timestamp'] + filtered_fields
            writer.writerow(header)
            
            # Write initial state
            initial_row = [initial_timestamp]
            for field in filtered_fields:
                initial_row.append(current_state.get(field, ''))
            writer.writerow(initial_row)
            
            # Process changes
            for entry in data[1:]:  # Skip initial entry
                if 'changes' in entry:
                    timestamp = entry.get('timestamp', '')
                    changes = entry.get('changes', [])
                    
                    # Apply changes to current state
                    current_state = apply_changes_to_state(current_state, changes)
                    
                    # Write updated state
                    row = [timestamp]
                    for field in filtered_fields:
                        row.append(current_state.get(field, ''))
                    writer.writerow(row)
        
        print(f"Created {csv_path}")
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

def main():
    """Main function to process all team JSON files."""
    # Paths
    BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    TEAM_DATA_PATH = os.path.join(BASE_PATH, "team-data")
    JSON_INPUT_PATH = os.path.join(TEAM_DATA_PATH, "json")
    CSV_OUTPUT_PATH = os.path.join(TEAM_DATA_PATH, "csv")
    
    print("Starting JSON to CSV conversion...")
    print(f"Reading from: {JSON_INPUT_PATH}")
    print(f"Writing to: {CSV_OUTPUT_PATH}")
    
    # Create CSV output directory
    os.makedirs(CSV_OUTPUT_PATH, exist_ok=True)
    
    # Get all team JSON files
    team_files = glob.glob(os.path.join(JSON_INPUT_PATH, '*.json'))
    team_files = [f for f in team_files if not f.endswith('summary.json')]
    
    if not team_files:
        print("No JSON files found to convert")
        return
    
    print(f"Found {len(team_files)} team files to process")
    
    # Process each team file
    for file_path in sorted(team_files):
        process_team_file(file_path, CSV_OUTPUT_PATH)
    
    print(f"\nAll CSV files created in {CSV_OUTPUT_PATH}/ directory")

if __name__ == "__main__":
    main()
