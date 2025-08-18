#!/usr/bin/env python3

import json
import os
import csv
import re
from typing import Dict, List, Any

# Paths
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEAM_DATA_PATH = os.path.join(BASE_PATH, "team-data")
JSON_INPUT_PATH = os.path.join(TEAM_DATA_PATH, "json")
CSV_OUTPUT_PATH = os.path.join(TEAM_DATA_PATH, "csv")

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

def flatten_change_data(entry: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a change entry into CSV-friendly format."""
    flattened = {
        'timestamp': entry.get('timestamp', ''),
        'entry_type': entry.get('type', ''),
        'team': entry.get('team', '')
    }
    
    if entry.get('type') == 'initial':
        # Handle initial state entries
        interface_data = entry.get('interface_data', {})
        flattened.update({
            'service': 'interface',
            'field': 'initial_state',
            'full_path': f"namada.operator.{entry.get('team', '')}.interface",
            'change_type': 'initial',
            'old_value': '',
            'new_value': json.dumps(interface_data)
        })
    elif entry.get('type') == 'change':
        # Handle change entries
        change_data = entry.get('change_data', {})
        flattened.update({
            'service': change_data.get('service', ''),
            'field': change_data.get('field', ''),
            'full_path': change_data.get('full_path', ''),
            'change_type': change_data.get('type', ''),
            'old_value': str(change_data.get('old_value', '')),
            'new_value': str(change_data.get('new_value', ''))
        })
    
    return flattened

def convert_json_to_csv(json_file_path: str, csv_file_path: str) -> int:
    """Convert a single JSON file to CSV format."""
    # Load JSON data
    json_data = load_json_file(json_file_path)
    if not json_data:
        print(f"No data found in {json_file_path}")
        return 0
    
    # Flatten all entries
    flattened_data = []
    for entry in json_data:
        flattened = flatten_change_data(entry)
        flattened_data.append(flattened)
    
    # Write to CSV
    if flattened_data:
        fieldnames = [
            'timestamp', 'entry_type', 'team', 'service', 'field', 
            'full_path', 'change_type', 'old_value', 'new_value'
        ]
        
        os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(flattened_data)
        
        print(f"Converted {len(flattened_data)} entries to {csv_file_path}")
        return len(flattened_data)
    
    return 0

def main():
    print("Starting JSON to CSV conversion...")
    print(f"Reading from: {JSON_INPUT_PATH}")
    print(f"Writing to: {CSV_OUTPUT_PATH}")
    
    # Get all JSON files
    if not os.path.exists(JSON_INPUT_PATH):
        print(f"Error: JSON input directory not found: {JSON_INPUT_PATH}")
        return
    
    json_files = [f for f in os.listdir(JSON_INPUT_PATH) if f.endswith('.json')]
    if not json_files:
        print("No JSON files found to convert")
        return
    
    print(f"Found {len(json_files)} JSON files to convert")
    
    total_entries = 0
    converted_files = 0
    
    # Convert each JSON file to CSV
    for json_file in json_files:
        team_name = json_file.replace('.json', '')
        json_path = os.path.join(JSON_INPUT_PATH, json_file)
        csv_path = os.path.join(CSV_OUTPUT_PATH, f"{team_name}.csv")
        
        entries = convert_json_to_csv(json_path, csv_path)
        if entries > 0:
            converted_files += 1
            total_entries += entries
    
    # Print summary
    print("\n=== Summary ===")
    print(f"JSON files processed: {len(json_files)}")
    print(f"CSV files created: {converted_files}")
    print(f"Total entries converted: {total_entries}")
    print(f"Output directory: {CSV_OUTPUT_PATH}")
    print("Done!")

if __name__ == "__main__":
    main()
