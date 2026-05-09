import sys                      # lets us exit the program with a status code
import csv                     # used to read and write CSV files
import argparse                # used to read command-line arguments
from pathlib import Path       # easier way to handle file paths
import requests                # used to make HTTP requests

base_url = "https://exoplanetarchive.ipac.caltech.edu/cgi-bin/nstedAPI/nph-nstedAPI"
default_table = "cumulative"
default_columns = [
    "kepler_name",
    "koi_period",
    "koi_teq",
    "koi_prad",
    "koi_smass",
    "koi_disposition",
    "koi_score",
    "koi_steff",
    "koi_srad",
    "koi_impact",
]

# client for fetching nasa data from api
class nasa_exoplanet:

    def fetch_exoplanets(self, table=default_table, columns=None):
    # returns a key value pair from api
        if columns is None:
            columns = default_columns

        params = {"table": table,"format": "csv","select": ",".join(columns)}

        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()

        return self._parse_csv(response.text)

    def _parse_csv(self, text):
        #create a list from raw csv data from api

        lines = text.strip().split("\n")

        # use csv.DictReader to turn each row into a dictionary
        reader = csv.DictReader(lines)
        records = []

        for row in reader:
            # strip whitespace from keys and values; convert empty strings to None
            clean_row = {k.strip(): (v.strip() if v and v.strip() else None) for k, v in row.items()}
            records.append(clean_row)

        return records

    def save_to_file(self, data, output_path):

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # write all records to csv using keys from the first record as headers
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(data[0].keys()))
            writer.writeheader()
            writer.writerows(data)

def main():
    # set up argument parser for command-line input
    parser = argparse.ArgumentParser(
        description="Fetch NASA Exoplanet Archive data via public API"
    )
    parser.add_argument(
        "--output",
        default="./data/exoplanets.csv",
        help="Path to output CSV file (default: ./data/exoplanets.csv)"
    )
    parser.add_argument(
        "--table",
        default=default_table,
        help="NASA API table to query (default: cumulative)"
    )

    args = parser.parse_args()

    try:
        client = nasa_exoplanet()
        data = client.fetch_exoplanets(table=args.table)
        client.save_to_file(data, args.output)
        return 0

    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())