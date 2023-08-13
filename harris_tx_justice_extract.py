"""API wrappers to retrieve case extracts from the Harris County, TX, Justice Courts.
"""

import datetime
import logging
from dataclasses import asdict, dataclass
from enum import IntEnum, StrEnum

import pandas as pd
import requests
from lxml import etree

logger = logging.getLogger(__name__)


class CaseType(StrEnum):
    CRIMINAL = "CR"
    CIVIL = "CV"


class CivilExtractType(IntEnum):
    CASES_ENTERED = 1
    HEARINGS_SET = 6
    JUDGMENTS_ENTERED = 3


class CriminalExtractType(IntEnum):
    CASES_ENTERED = 1
    HEARINGS_SET = 2
    CASES_DISPOSED = 5
    DSC_APPLICATIONS_PAID = 4


class CourtId(IntEnum):
    PRECINCT_1_PLACE_1 = 305
    PRECINCT_1_PLACE_2 = 310
    PRECINCT_2_PLACE_1 = 315
    PRECINCT_2_PLACE_2 = 320
    PRECINCT_3_PLACE_1 = 325
    PRECINCT_3_PLACE_2 = 330
    PRECINCT_4_PLACE_1 = 335
    PRECINCT_4_PLACE_2 = 340
    PRECINCT_5_PLACE_1 = 345
    PRECINCT_5_PLACE_2 = 350
    PRECINCT_6_PLACE_1 = 355
    PRECINCT_6_PLACE_2 = 360
    PRECINCT_7_PLACE_1 = 365
    PRECINCT_7_PLACE_2 = 370
    PRECINCT_8_PLACE_1 = 375
    PRECINCT_8_PLACE_2 = 380


class CivilCaseType(StrEnum):
    ALL_CASE_TYPES = (
        "ADMIN,BRSB,BOND,DD,CTA,DEBT,DCD,DLS,EV,FJ,HGL,ODL,OEPR,"
        "RR,SC,SSP,TAX,TOW,TURN,WRITG,WR,WRU,SEQ,WRJ"
    )
    ADMINISTRATIVE = "ADMIN"
    BILL_OF_REVIEW = "BRSB"
    BOND_FORFEITURE = "BOND"
    CRUELLY_TREATED_ANIMAL = "CTA"
    DANGEROUS_DOG = "DD"
    DEBT_CLAIM = "DEBT"
    DOG_CAUSING_DEATH_OR_SERIOUS_BODILY_INJURY = "DCD"
    DRIVERS_LICENSE_SUSPENSION_HEARING = "DLS"
    EVICTION = "EV"
    FOREIGN_JUDGEMENT = "FJ"
    HANDGUN_LICENSE = "HGL"
    OCCUPATIONAL_DRIVERS_LICENSE = "ODL"
    ORDER_FOR_ENTRY_AND_PROPERTY_RETRIEVAL = "OEPR"
    REPAIR_AND_REMEDY = "RR"
    SMALL_CLAIMS = "SC"
    STOLEN_OR_SEIZED_PROPERTY = "SSP"
    TAX_SUIT = "TAX"
    TOW_HEARING = "TOW"
    TURNOVER = "TURN"
    WRIT_OF_GARNISHMENT = "WRITG"
    WRIT_OF_REENTRY = "WR"
    WRIT_OF_RESTORATION_UTILITY = "WRU"
    WRIT_OF_SEQUESTRATION = "SEQ"
    WRIT_TO_REVIVE_JUDGMENT = "WRJ"


class CriminalCaseType(StrEnum):
    ALL_CASE_TYPES = "CRCIT,CRCOM"
    CRIMINAL_CITATION = "CRCIT"
    CRIMINAL_COMPLAINT = "CRCOM"


class ExtractFormat(StrEnum):
    TSV = "tab"
    CSV = "csv"
    XML = "xml"


@dataclass
class ExtractParams:
    """Extract API request parameters.

    Arguments:

    """

    extractCaseType: CaseType
    extract: CivilExtractType | CriminalExtractType
    court: CourtId
    casetype: CivilCaseType | CriminalCaseType
    format: ExtractFormat
    fdate: str  # %m/%d/%Y
    tdate: str


def get_civil_extract(
    extract: CivilExtractType,
    court: CourtId,
    casetype: CivilCaseType,
    format: ExtractFormat,
    fdate: datetime.date,
    tdate: datetime.date,
    *,
    session=None,
):
    params = ExtractParams(
        CaseType.CIVIL,
        extract,
        court,
        casetype,
        format,
        fdate.strftime("%m/%d/%Y"),
        tdate.strftime("%m/%d/%Y"),
    )
    return get_extract(params, session=session)


def get_criminal_extract(
    extract: CriminalExtractType,
    court: CourtId,
    casetype: CriminalCaseType,
    format: ExtractFormat,
    fdate: datetime.date,
    tdate: datetime.date,
    *,
    session=None,
):
    params = ExtractParams(
        CaseType.CRIMINAL,
        extract,
        court,
        casetype,
        format,
        fdate.strftime("%m/%d/%Y"),
        tdate.strftime("%m/%d/%Y"),
    )
    return get_extract(params, session=session)


def get_extract(params: ExtractParams, *, session=None):
    API_URL = "https://jpwebsite.harriscountytx.gov" "/PublicExtracts/GetExtractData"
    r = (session or requests).get(API_URL, params=asdict(params))
    r.raise_for_status()
    logging.debug("Parsing response")
    doc = etree.fromstring(r.content)
    first_row = doc.find("Row")
    if first_row is None:
        logging.debug("No data in response")
        return None
    columns = ["CourtId"] + [elt.tag for elt in first_row]
    logging.debug(f"{len(columns)} columns")
    data = []
    for row in doc.findall("Row"):
        data.append([params.court.value] + [elt.text for elt in row])
    logging.debug(f"{len(data)} rows")
    if not data:
        return None
    return pd.DataFrame(data, columns=columns)
