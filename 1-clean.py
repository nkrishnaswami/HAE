import csv
import logging
import re
from enum import Enum
from pathlib import Path

import pandas as pd
from jsonargparse import ActionConfigFile, ArgumentParser

from harris_tx_justice_extract import CivilCaseType, CivilExtractType

logger = logging.getLogger(__name__)
_log_levels = logging.getLevelNamesMapping()
del _log_levels['NOTSET']
LogLevel = Enum('LogLevel', _log_levels.items())


def load_raw_data(path: Path) -> pd.DataFrame:
    with path.open() as f:
        header = next(iter(csv.reader(f)))
    date_fields = []
    dtypes = {}
    for field in header:
        if re.search(r'(Fees|Costs|Rate|Amount)$', field):
            dtypes[field] = float
        elif field.endswith('Date'):
            date_fields.append(field)
        else:
            dtypes[field] = str
    return pd.read_csv(path, dtype=dtypes, parse_dates=date_fields)


def concatenate_raw_files(data_dir: Path, case_type: CivilCaseType,
                      extract_type: CivilExtractType) -> pd.DataFrame:
    return pd.concat(
        load_raw_data(filename)
        for filename in  data_dir.glob(
                'evictions-raw-*-*-*'
                f'-{case_type.value}'
                f'-{extract_type.name}.csv'))

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--config', action=ActionConfigFile)
    parser.add_argument('--data-dir', type=Path, default=Path('./data'))
    parser.add_argument('--case-type', type=CivilCaseType,
                        default=CivilCaseType.EVICTION)
    parser.add_argument('--extract_type', type=CivilExtractType,
                        default=CivilExtractType.JUDGMENTS_ENTERED)
    parser.add_argument('--log-level', type=LogLevel, default=LogLevel.INFO)

    args = parser.parse_args()

    log_format = ('%(asctime)s %(levelname)s %(name)s '
                  '%(filename)s:%(lineno)s %(message)s')
    logging.basicConfig(level=args.log_level.value, format=log_format)

    logging.info(f'Concatenating {args.case_type.name} cases, '
                 f'{args.extract_type.name} extract')
    df = concatenate_raw_files(args.data_dir, args.case_type, args.extract_type)
    logging.info('Finished loading cases; writing to parquet file')

    output_file = (args.data_dir /
                   f'{args.case_type.name}'
                   f'-{df.FiledDate.min().isoformat().replace("-", "")}'
                   f'-{df.FiledDate.max().isoformat().replace("-", "")}'
                   f'-{args.extract_type.name}'
                   '.pq')
    df.set_index(['FiledDate', 'CaseNumber'], inplace=True)
    df.sort_index(inplace=True)
    df.to_parquet(output_file)
    logging.info('Done merging files')
