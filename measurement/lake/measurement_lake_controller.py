"""
A lake manager.
"""

import asyncio
import io
import os
import zipfile
from enum import StrEnum
from pathlib import Path
from typing import Self

import arrow
import duckdb
import httpx
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from measurement.ingest.cisco_umbrella_date_resource import CiscoUmbrellaDateResource
from measurement.util.logger import get_logger

from .base_lake_controller import BaseLakeController

log = get_logger(__name__)


class _ConvertResult(StrEnum):
    CONVERTED = "converted"
    SKIPPED = "skipped"
    MISSING = "missing"


class MeasurementLakeController(BaseLakeController):
    def __init__(self, path: Path, output: Path):
        self._output_dir = output
        self._path = path
        self._raw = path / "raw"
        self._raw.mkdir(parents=True, exist_ok=True)

    @classmethod
    def create(cls, path: Path, output: Path) -> Self:
        return cls(path, output)

    def query(self, sql: str) -> duckdb.DuckDBPyRelation:
        """Run a DuckDB SQL query against the lake.

        Use the `raw` view to reference parquet files. Each row includes a `date`
        column (DATE type) derived from the file path, so you can filter with e.g.
        ``WHERE date BETWEEN '2025-01-01' AND '2025-01-31'``.
        """
        if not hasattr(self, "_con"):
            self._con = duckdb.connect()
            self._con.execute(f"""
                CREATE VIEW raw AS
                SELECT
                    *,
                    make_date(
                        regexp_extract(filename, '(\\d{{4}})/(\\d{{2}})/(\\d{{2}})\\.parquet$', 1)::INTEGER,
                        regexp_extract(filename, '(\\d{{4}})/(\\d{{2}})/(\\d{{2}})\\.parquet$', 2)::INTEGER,
                        regexp_extract(filename, '(\\d{{4}})/(\\d{{2}})/(\\d{{2}})\\.parquet$', 3)::INTEGER
                    ) AS date
                FROM parquet_scan('{self._raw}/**/*.parquet', filename=true, hive_partitioning=false)
            """)
        return self._con.sql(sql)
    
    def reset_connection(self):
        if hasattr(self, "_con"):
            self._con.close()
            del self._con

    async def download(self, startdate: arrow.Arrow, enddate: arrow.Arrow) -> None:
        await self._download_all(startdate, enddate)

    def contains(self, domain: str, year: int) -> None:
        os.makedirs(f"{self._output_dir}/{domain}", exist_ok=True)
        return Path(f"{self._output_dir}/{domain}/{year}.parquet").exists()

    def save(self, df, domain: str, year: int) -> None:
        os.makedirs(f"{self._output_dir}/{domain}", exist_ok=True)
        df.to_parquet(f"{self._output_dir}/{domain}/{year}.parquet")

    def to_parquet(self, startdate: arrow.Arrow, enddate: arrow.Arrow) -> None:
        converted = 0
        skipped = 0
        missing = 0

        for date in arrow.Arrow.range("day", startdate, enddate):
            result = self._to_parquet_one(date)
            if result == _ConvertResult.CONVERTED:
                converted += 1
            elif result == _ConvertResult.SKIPPED:
                skipped += 1
            elif result == _ConvertResult.MISSING:
                missing += 1

        log.info(f"to_parquet done: {converted} converted, {skipped} already parquet, {missing} missing")

    def _to_parquet_one(self, date: arrow.Arrow) -> _ConvertResult:
        year_str = date.format("YYYY")
        month_str = date.format("MM")
        day_str = date.format("DD")
        label = f"{year_str}/{month_str}/{day_str}"

        csv_path = self._raw / year_str / month_str / f"{day_str}.csv"
        parquet_path = csv_path.with_suffix(".parquet")

        if parquet_path.exists():
            log.debug(f"{label}.parquet already exists")
            return _ConvertResult.SKIPPED

        if not csv_path.exists():
            log.warning(f"{label}.csv does not exist")
            return _ConvertResult.MISSING

        convert_opts = pcsv.ConvertOptions(
            column_types=pa.schema([("rank", pa.int32()), ("domain", pa.string())]),
        )
        read_opts = pcsv.ReadOptions(column_names=["rank", "domain"])

        try:
            table = pcsv.read_csv(csv_path, read_options=read_opts, convert_options=convert_opts)
            pq.write_table(table, parquet_path, compression="zstd")
            csv_path.unlink()
            log.debug(f"{label}: converted to parquet")
            return _ConvertResult.CONVERTED
        except Exception as e:
            log.error(f"{label}: failed to convert — {e}")
            return _ConvertResult.MISSING

    async def _download_all(self, startdate: arrow.Arrow, enddate: arrow.Arrow) -> None:
        sem = asyncio.Semaphore(4)
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(6000, connect=90, pool=None),
            limits=httpx.Limits(max_connections=4, max_keepalive_connections=4),
        ) as client:
            tasks = [self._download_one(client, sem, d) for d in arrow.Arrow.range("day", startdate, enddate)]
            await asyncio.gather(*tasks)

    async def _download_one(self, client: httpx.AsyncClient, sem: asyncio.Semaphore, date: arrow.Arrow) -> None:
        resource = CiscoUmbrellaDateResource.of_date(date)

        year_str = date.format("YYYY")
        month_str = date.format("MM")
        day_str = date.format("DD")

        dest_folder = self._raw / year_str / month_str
        dest_folder.mkdir(parents=True, exist_ok=True)
        dest = dest_folder / f"{day_str}.csv"

        if dest.exists() or dest.with_suffix(".parquet").exists():
            log.debug(f"{year_str}/{month_str}/{day_str} already exists, skipping")
            return

        async with sem:
            try:
                resp = await client.get(resource.url())
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                log.warning(f"{year_str}/{month_str}/{day_str}: HTTP {e.response.status_code} — likely gap in data")
                return
            except httpx.RequestError as e:
                log.error(f"{year_str}/{month_str}/{day_str}: request failed — {e}")
                return

            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                csv_bytes = zf.read(zf.namelist()[0])

            dest.write_bytes(csv_bytes)
            log.info(f"{year_str}/{month_str}/{day_str}: downloaded ({len(csv_bytes)} bytes)")
