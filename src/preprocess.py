"""
This file provides preprocessing functions to load CIS PUMF data.
"""

import pandas as pd
from os import PathLike
from pathlib import Path
from tqdm import tqdm


def load_data(data_dir: str | PathLike, pattern='*.dta') -> pd.DataFrame:
    """
    Load all CIS PUMFs into a single DataFrame.

    Args:
        data_dir: The directory to load data from.
        pattern: The file extension of the PUMFs to be globbed.

    Returns:
        pd.DataFrame: The loaded data.
    """
    files = list(Path(data_dir).glob(pattern))
    dfs = [_read_cis_pumf(f) for f in tqdm(files)]
    return pd.concat(dfs, ignore_index=True)


def _read_cis_pumf(path: str | PathLike) -> pd.DataFrame:
    """
    Read a CIS PUMF Stata file into a Pandas DataFrame.

    Args:
        path: The path of the PUMF DTA.

    Returns:
        pd.DataFrame: The read and filtered data.
    """
    cols = [
        # IDENTIFIERS
        'year',         # Reference year
        'pumfid',       # Household identifier
        'personid',     # Person identifier
        'fweight',      # Final weight
        # DEMOGRAPHY
        'sex',          # Sex
        'agegp',        # Person's age group as of Dec 31 of reference year
        'marst',        # Marital status
        'immst',        # Person is a landed immigrant
        'yrimmg',       # Number of years since person immigrated to Canada
        # GEOGRAPHY
        'prov',         # Province
        'uszgap',       # Adjusted size of area of residence
        # INCOME
        'ttinc',        # Total income before taxes
        'atinc',        # After-tax income
        'mtinc',        # Market income
        'gtr',          # Government transfers, federal and provincial
        # HOUSEHOLD
        'hhsize',       # Number of household members
        'hhcomp',       # Household composition
        # HOUSING
        'dwltyp',       # Type of dwelling
        'dwtenr',       # Ownership of dwelling
        'repa',         # Condition of dwelling
        'suit',         # Dwelling suitable, according to Nat. Occ. Standards
        'mortg',        # Mortgage on dwelling
        'mortgm',       # Monthly mortgage payment, excluding property taxes
        'condmp',       # Monthly condominium fee
        'rentm'         # Monthly rent paid for the household
    ]
    # These variable names were altered for 2018 onward:
    #   marst => marstp
    #   immst => immstp
    #   yrimmg => yrimmgp
    # Rename them to how they were in 2012-17 for consistency.
    col_renamings = {
        'marstp': 'marst',
        'immstp': 'immst',
        'yrimmgp': 'yrimmg'
    }
    # Codes for missing values in the above variables.
    missing_codes = {
        'marst': '99',
        'immst': '9',
        'yrimmg': '9',
        'dwltyp': '9',
        'dwtenr': '9',
        'suit': '9'
    }
    # Codes for age groups. The categories and subsequently the codings changed
    # for 2018 onward.
    agegp_codes_2012_17 = [f"{i:02d}" for i in range(7, 16)]
    agegp_codes_2018_22 = [f"{i:02d}" for i in range(6, 15)]

    # Read the PUMF.
    df = pd.read_stata(path)
    df = df.rename(columns=col_renamings)
    df = df[cols]
    for col, code in missing_codes.items():
        df = df[df[col] != code]
    year = df['year'].iat[0]
    if 2012 <= year <= 2017:
        df = df[df['agegp'].isin(agegp_codes_2012_17)]
    else:
        df = df[df['agegp'].isin(agegp_codes_2018_22)]

    return df
