import pandas as pd
import requests
import time
import re

# Configuration
INPUT_CSV = 'locations.csv'      # Input file name
OUTPUT_CSV = 'coordinates.csv'   # Output file name
PROPERTY_ID_COLUMN = 'Property ID'  # Column with Property ID
MAP_URL_COLUMN = 'Map Location'     # Column with Google Map URLs

# Function to expand shortened URLs (Google Maps short URLs)
def expand_short_url(short_url):
    try:
        response = requests.head(short_url, allow_redirects=True)
        return response.url
    except Exception as e:
        print(f"Error expanding URL '{short_url}': {e}")
        return None

# Function to extract coordinates from expanded Google Maps URL
def extract_coordinates_from_url(url):
    try:
        # Match latitude and longitude in the format @lat,lng
        match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', url)
        if match:
            lat, lon = match.group(1), match.group(2)
            return lat, lon
        else:
            print(f"No coordinates found in URL: {url}")
            return None, None
    except Exception as e:
        print(f"Error extracting coordinates from URL '{url}': {e}")
        return None, None

# Main Script
def main():
    # Read input CSV
    print("Reading input CSV...")
    df = pd.read_csv(INPUT_CSV)

    if PROPERTY_ID_COLUMN not in df.columns or MAP_URL_COLUMN not in df.columns:
        raise ValueError(f"'{PROPERTY_ID_COLUMN}' or '{MAP_URL_COLUMN}' column not found in the CSV file.")

    # Create new columns for Latitude and Longitude
    df['Latitude'] = None
    df['Longitude'] = None

    # Process each row
    print("Processing rows...")
    for idx, row in df.iterrows():
        map_url = row[MAP_URL_COLUMN]
        property_id = row[PROPERTY_ID_COLUMN]

        if pd.notnull(map_url):
            print(f"Processing Property ID: {property_id} - Expanding URL...")
            expanded_url = expand_short_url(map_url)

            if expanded_url:
                print(f"Expanded URL: {expanded_url}")
                lat, lon = extract_coordinates_from_url(expanded_url)
                df.at[idx, 'Latitude'] = lat
                df.at[idx, 'Longitude'] = lon
                print(f"Property ID: {property_id} -> Latitude: {lat}, Longitude: {lon}")
                time.sleep(1)  # Respect rate limits
            else:
                print(f"Failed to expand URL for Property ID: {property_id}")
        else:
            print(f"Skipping empty map location for Property ID: {property_id}")

    # Save results to CSV
    print(f"Saving results to {OUTPUT_CSV}...")
    df.to_csv(OUTPUT_CSV, index=False)
    print("Done!")

if __name__ == "__main__":
    main()
