import asyncio
import gc
from pathlib import Path

import arrow
import tqdm

from measurement.lake.measurement_lake_controller import MeasurementLakeController
from measurement.util.logger import get_logger

log = get_logger(__name__)


def extract_domain(lake, domain, start_year=2018, end_year=2025):
    for year in range(start_year, end_year + 1):
        if lake.contains(domain, year):
            log.info(f"Skipping {domain}:{year}")
            continue

        log.info(f"Fetching {domain}:{year}")
        startdate = f"{year}-01-01"
        enddate = f"{year}-12-31"

        df = lake.query(f"""
            SELECT date, MIN(rank) AS best_rank, COUNT(*) AS num_subdomains
            FROM raw
            WHERE date BETWEEN '{startdate}' AND '{enddate}'
              AND (domain LIKE '%.{domain}' OR domain = '{domain}')
            GROUP BY date
            ORDER BY date
        """).df()

        lake.save(df, domain, year)
        lake.reset_connection()
        del df


async def main():
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

    lake = MeasurementLakeController.create(
        path=Path("/mnt/apple/cisco-umbrella-2019-2025"), output=Path("/mnt/apple/cisco-umbrella-2019-2025/processed")
    )
    await lake.download(arrow.get("2018-01-01"), arrow.get("2025-12-31"))
    lake.to_parquet(arrow.get("2018-01-01"), arrow.get("2025-12-31"))

    for domain in domains:
        log.info(f"Starting domain {domain}...")
        extract_domain(lake, domain, START_YEAR, END_YEAR)
        log.info("Reducing memory footprint & gc...")
        gc.collect()
        log.info(f"Done with gc and domain {domain}")


if __name__ == "__main__":
    asyncio.run(main())
