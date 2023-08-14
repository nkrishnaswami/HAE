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
_log_levels = logging.getLevelNamesMapping()
del _log_levels['NOTSET']
LogLevel = Enum('LogLevel', _log_levels.items())


def generate_extracts(
        fdate: datetime.date, tdate: datetime.date,
        case_type: CivilCaseType,
        extract_type: CivilExtractType,
        courts: list[CourtId] = []
):
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    s.mount('http://', HTTPAdapter(max_retries=retries))

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
                f'Retrieving extract for dates {fdate} to {range_end} '
                f'Court ID {court}'
            )
            extract = get_civil_extract(
                extract=extract_type,
                court=court,
                casetype=case_type,
                format=ExtractFormat.XML,
                fdate=fdate,
                tdate=range_end,
                session=s,
            )
            if extract is not None:
                yield court, fdate, range_end, extract
        fdate = range_end + datetime.timedelta(days=1)


if __name__ == '__main__':
    register_type(datetime.date, datetime.date.isoformat, datetime.date.fromisoformat)
    parser = ArgumentParser(
        description='Retrieve Harris County, TX, ' 'justice court data extracts'
    )
    parser.add_argument('--config', action=ActionConfigFile)
    parser.add_argument('fdate', type=datetime.date,
                        help='From date (ISO 8601 format)')
    parser.add_argument('tdate', type=datetime.date,
                        help='To date (ISO 8601 format)')
    parser.add_argument('--courts', type=CourtId, nargs='+',
                        help='Courts to include.  If not specified, all courts')
    parser.add_argument('--case-type', type=CivilCaseType,
                        default=CivilCaseType.EVICTION,
                        help='Type of case to retrieve')
    parser.add_argument('--extract-type', type=CivilExtractType,
                        default=CivilExtractType.CASES_ENTERED,
                        help='Type of extract to retrieve')
    parser.add_argument('--log-level', type=LogLevel, default=LogLevel.INFO)
    parser.add_argument(
        '--out_dir',
        type=Path,
        default=Path('./data'),
        help='Dir into which to write output files',
    )
    args = parser.parse_args()
    log_format = ('%(asctime)s %(levelname)s %(name)s '
                  '%(filename)s:%(lineno)s %(message)s')
    logging.basicConfig(level=args.log_level.value, format=log_format)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    for court, fdate, tdate, extract in generate_extracts(
            args.fdate, args.tdate, args.case_type, args.extract_type,
            args.courts
    ):
        extract.to_csv(
            (args.out_dir /
             'evictions-raw'
             f'-{fdate.isoformat().replace("-", "")}'
             f'-{tdate.isoformat().replace("-", "")}'
             f'-{court}'
             f'-{args.case_type.value}'
             f'-{args.extract_type.name}.csv'
             ).as_posix(),
            index=False
        )
