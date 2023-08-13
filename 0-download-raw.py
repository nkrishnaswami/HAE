import calendar
import datetime
import logging
from enum import Enum
from pathlib import Path

import requests
from jsonargparse import ActionConfigFile, ArgumentParser
from jsonargparse.typing import register_type
from requests.adapters import HTTPAdapter, Retry

from harris_tx_justice_extract import (CivilCaseType, CivilExtractType,
                                       CourtId, ExtractFormat,
                                       get_civil_extract)

logger = logging.getLogger(__name__)
LogLevel = Enum("LogLevel", logging.getLevelNamesMapping().items())


def generate_evictions(
    fdate: datetime.date, tdate: datetime.date, courts: list[CourtId] = []
):
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    s.mount("http://", HTTPAdapter(max_retries=retries))

    done = False
    while not done:
        month_end = datetime.date(
            fdate.year, fdate.month, calendar.monthrange(fdate.year, fdate.month)[1]
        )
        range_end = min(tdate, month_end)
        if range_end == tdate:
            done = True
        for court in courts or CourtId:
            logger.info(
                f"Retrieving extract for dates {fdate} to {range_end} "
                f"Court ID {court}"
            )
            extract = get_civil_extract(
                extract=CivilExtractType.CASES_ENTERED,
                court=court,
                casetype=CivilCaseType.EVICTION,
                format=ExtractFormat.XML,
                fdate=fdate,
                tdate=range_end,
                session=s,
            )
            if extract is not None:
                yield court, fdate, range_end, extract
        fdate = range_end + datetime.timedelta(days=1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    register_type(datetime.date, datetime.date.isoformat, datetime.date.fromisoformat)
    parser = ArgumentParser(
        description="Retrieve Harris County, TX, " "eviction filings"
    )
    parser.add_argument("--config", action=ActionConfigFile)
    parser.add_argument("fdate", type=datetime.date, help="From date")
    parser.add_argument("tdate", type=datetime.date, help="To date")
    parser.add_argument("--courts", type=CourtId, nargs="+", help="Courts to include")
    parser.add_argument("--log-level", type=LogLevel)
    parser.add_argument(
        "--out_dir",
        type=Path,
        default=Path("./data"),
        help="Dir into which to write output files",
    )
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    for court, fdate, tdate, extract in generate_evictions(
        args.fdate, args.tdate, args.courts
    ):
        extract.to_csv(
            (args.out_dir / f"evictions-raw-{fdate}_{tdate}_{court}.csv").as_posix(),
            index=False,
        )
