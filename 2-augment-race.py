import logging
import re
from enum import Enum
from pathlib import Path

import pandas as pd
import surgeo
from jsonargparse import ActionConfigFile, ArgumentParser
from nameparser import HumanName

logger = logging.getLogger(__name__)
_log_levels = logging.getLevelNamesMapping()
del _log_levels['NOTSET']
LogLevel = Enum('LogLevel', _log_levels.items())


def extract_first_defendant_name(name):
    newname = name
    if match := re.search(r'(all )?(of )?(the )?(other )?occupants', newname, re.I):
        newname = newname[:match.start()].strip()
    if match := re.search(r' (and\b|&|\bet ?all?$)', newname, re.I):
        newname = newname[:match.start()].strip()
    if match := re.search(r' (a/k/a|aka|fka|d/b/a|dba) ', newname, re.I):
        newname = newname[:match.start()].strip()
    if match := re.match(r'mr\.? or mrs\.? ', newname, re.I):
        newname = newname[match.end():].strip()
    if match := re.search(r' or ', newname, re.I):
        newname = newname[match.end():].strip()
    if match := re.search(r' (serving|served) ', newname, re.I):
        newname = newname[match.end():].strip()
    return HumanName(newname)


def calculate_race_probs(df, name_col, zip_col):
    fsg = surgeo.BIFSGModel()
    logger.debug(f'Parsing {len(df)} names')
    name_df = df[[name_col, zip_col]].copy().dropna()
    name_df[['first_name', 'last_name']] = name_df[[name_col]].applymap(
        extract_first_defendant_name
    ).apply(
        lambda x: (x[0].first, x[0].last),
        axis=1,
        result_type='expand'
    )
    index = name_df.index
    name_df.reset_index(inplace=True)
    logger.debug(f'Calculating {len(name_df)} race probabilities')
    race_probs = fsg.get_probabilities(name_df.first_name,
                                       name_df.last_name,
                                       name_df[name_col])
    logger.debug('Finished calculating race probabilities')
    return race_probs.set_index(index)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument(
        '--input', type=Path,
        help='Path to a parquet file created by 1-clean.py')
    parser.add_argument(
        '--output', type=Path,
        help='Path to output parquet file with race probablities')
    parser.add_argument('--config', action=ActionConfigFile)
    parser.add_argument('--log-level', type=LogLevel, default=LogLevel.INFO)

    args = parser.parse_args()

    log_format = ('%(asctime)s %(levelname)s %(name)s '
                  '%(filename)s:%(lineno)s %(message)s')
    logging.basicConfig(level=args.log_level.value, format=log_format)

    logger.debug(f'Loading {args.input}')
    df = pd.read_parquet(args.input)
    race_df = calculate_race_probs(df.DefendantName. df.DefendantAddressZIP1)
    logger.debug(f'Writing {len(race_df)} records to {args.race_output}')
    race_df.to_parquet(args.race_output)
    logger.debug('Done')
