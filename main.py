import asyncio
from pathlib import Path

import arrow
import tqdm

from measurement.lake.measurement_lake_controller import MeasurementLakeController
from measurement.util.logger import get_logger

log = get_logger(__name__)


def extract_domain(lake, domain, start_year=2018, end_year=2025):
    for year in tqdm.tqdm(range(start_year, end_year + 1), desc=domain, leave=False):
        startdate = f"{year}-01-01"
        enddate = f"{year}-12-31"

        view = lake.query(f"""
            SELECT domain, rank, date
            FROM raw
            WHERE date BETWEEN '{startdate}' AND '{enddate}'
        """)

        df = view.query(
            "view",
            f"""
            SELECT date, MIN(rank) AS best_rank, COUNT(*) AS num_subdomains
            FROM view
            WHERE domain LIKE '%.{domain}' OR domain = '{domain}'
            GROUP BY date
            ORDER BY date
        """,
        ).df()

        lake.save(df, domain, year)


START_YEAR = 2018
END_YEAR = 2025

domains = [
    "google.com",
    "zoom.us",
    "teams.microsoft.com",
    "sharepoint.com",
    "office.com",
    "slack.com",
    "linkedin.com",
    "tumblr.com",
    "twitch.tv",
    "netflix.com",
    "reddit.com",
    "steampowered.com",
    "stackoverflow.com",
    "quora.com",
    "w3schools.com",
    "chegg.com",
    "stackexchange.com",
    "openai.com",
    "claude.ai",
    "deepseek.com",
    "blogspot.com",
    "wordpress.com",
    "nessus.org",
    "nflxso.net",
    "ampproject.org",
]


async def main():
    lake = MeasurementLakeController.create(
        path=Path("/mnt/apple/cisco-umbrella-2019-2025"), output=Path("/mnt/apple/cisco-umbrella-2019-2025/processed")
    )
    await lake.download(arrow.get("2018-01-01"), arrow.get("2025-12-31"))
    lake.to_parquet(arrow.get("2018-01-01"), arrow.get("2025-12-31"))

    for domain in tqdm.tqdm(domains, desc="All Domains", leave=False):
        extract_domain(lake, domain, START_YEAR, END_YEAR)


if __name__ == "__main__":
    asyncio.run(main())
