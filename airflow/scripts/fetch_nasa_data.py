import sys
import argparse
import csv
import requests

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
    # if you create more modules that use the snowflake conn_id, add a def __init__(): so that you can
    # easily move data from dev to prod by initializing different clients. Otherwise you will have to continuously pass
    # the connection parameters into each additional module.

    def fetch_exoplanets(self, table=default_table, columns=None):
    # returns a key value pair from api
    # Fetches exoplanet data from the NASA API.
    # Args: table (str): The NASA API table to query. Defaults to 'cumulative'.
    #       columns (list): List of column names to retrieve. Defaults to default_columns.
    # Returns: list[dict]: A list of records, each represented as a dictionary.
        if columns is None:
            columns = default_columns

        params = {"table": table,"format": "csv","select": ",".join(columns)}

        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()

        return self._parse_csv(response.text)

    def _parse_csv(self, text):
        #create a list of dicts from raw csv data from api. This will then later be put into snowflake

        lines = text.strip().split("\n")

        # use csv.DictReader to turn each row into a dictionary
        reader = csv.DictReader(lines)
        records = []

        for row in reader:
            # strip whitespace from keys and values; convert empty strings to None
            clean_row = {k.strip(): (v.strip() if v and v.strip() else None) for k, v in row.items()}
            records.append(clean_row)

        return records

def main():
    """
    CLI entry point for local testing only.
    Fetches data and prints the record count to stdout.
    This is NOT called by Airflow — Airflow calls fetch_exoplanets() directly via the DAG.
    """
    parser = argparse.ArgumentParser(
        description="Fetch NASA Exoplanet Archive data via public API (local testing)"
    )
    parser.add_argument(
        "--table",
        default=default_table,
        help="NASA API table to query (default: cumulative)",
    )
    args = parser.parse_args()

    try:
        client = nasa_exoplanet()
        data = client.fetch_exoplanets(table=args.table)
        print(f"Successfully fetched {len(data)} records from NASA API.")
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())