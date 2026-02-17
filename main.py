from pathlib import Path

import arrow

from measurement.lake.measurement_lake_controller import MeasurementLakeController

if __name__ == "__main__":
    lake = MeasurementLakeController.create(Path("/mnt/apple/cisco-umbrella-2019-2025"))

    lake.download(arrow.get("2025-02-01"), arrow.get("2025-02-11"))
