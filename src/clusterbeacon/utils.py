import hashlib
from datetime import datetime
import pandas as pd
import os
from typing import Iterable, Any, Optional, List, Union

def calc_md5(values: Iterable[Union[str, bytes]]) -> List[str]:
    """
    Compute MD5 hashes for a sequence of strings or bytes.

    Parameters
    ----------
    values : iterable of str or bytes
        Input values to hash.

    Returns
    -------
    list of str
        List of MD5 hash hex digests, one for each input.
    
    Examples
    --------
    >>> calc_md5(["hello", "world"])
    ['5d41402abc4b2a76b9719d911017c592', '7d793037a0760186574b0282f2f435e7']
    """
    return [
        hashlib.md5(v if isinstance(v, bytes) else str(v).encode()).hexdigest()
        for v in values
    ]

def date_now_str() -> str:
    """
    Get the current local date and time as a formatted string.

    Returns
    -------
    str
        The current date and time in ISO 8601-like format: "YYYY-MM-DD HH:MM:SS".

    Notes
    -----
    - Uses the system's local timezone (not UTC).
    - The format is consistent and safe for filenames, logging, and simple timestamps.
    - If you need strict ISO 8601 with timezone info, consider using:
      `datetime.now().isoformat()` instead.
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def filter_df(
    df: pd.DataFrame,
    colname: str,
    filt_values: Optional[Iterable[Any]] = None,
    min_val: Any = None,
    max_val: Any = None,
    *,
    inclusive: str = "both",   # "both" | "left" | "right" | "neither" (pandas ≥ 1.3)
    keep_na: bool = False       # keep NaNs instead of dropping them via comparisons
) -> pd.DataFrame:
    """
    Filter `df` on `colname` by (optional) membership, min, and max in one pass.

    - filt_values: keep rows where value is in this iterable (if provided)
    - min_val/max_val: numeric or comparable bounds; either or both can be given
    - inclusive: controls between() inclusivity when both bounds are given
    - keep_na: if True, NaNs in the column are kept
    """
    if df.empty:
        return df

    s = df[colname]
    mask = pd.Series(True, index=df.index)

    if filt_values:
        # Using set can speed up membership checks for large iterables
        mask &= s.isin(set(filt_values))

    if (min_val is not None) and (max_val is not None):
        mask &= s.between(min_val, max_val, inclusive=inclusive)
    elif min_val is not None:
        mask &= s.ge(min_val)
    elif max_val is not None:
        mask &= s.le(max_val)

    if keep_na:
        mask |= s.isna()

    return df.loc[mask]

def construct_concat_column(
    df: pd.DataFrame,
    columns: List[str],
    colname: str,
    fillna: bool = False,
    sep: str = "_"
) -> pd.DataFrame:
    """
    Create a new concatenated column from multiple columns.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    columns : list of str
        Columns to concatenate.
    colname : str
        Name of the new column.
    fillna : bool, default False
        If True, replace NaN with empty string before concatenation.
    sep : str, default "_"
        Separator string to use between values.

    Returns
    -------
    pd.DataFrame
        DataFrame with the new concatenated column added.
    """
    tmp = df[columns]
    if fillna:
        tmp = tmp.fillna("")

    # Convert all to string, then join row-wise
    df[colname] = tmp.astype(str).apply(lambda row: sep.join(row), axis=1)
    return df


def calc_date_delta(df: pd.DataFrame, date_col: str = "date") -> List[int]:
    """
    Calculate day differences between consecutive dates in a column.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing a date column.
    date_col : str, default 'date'
        Name of the column with datetime values.

    Returns
    -------
    list of int
        List of day deltas; first value is 0.
    """
    # Ensure datetime dtype
    s = pd.to_datetime(df[date_col], errors="coerce")
    
    # Compute differences in days, fill first with 0
    deltas = s.diff().dt.days.fillna(0).astype(int)
    
    return deltas.tolist()


def file_valid(f: str) -> bool:
    """
    Check if a file exists and is non-empty.

    Parameters
    ----------
    f : str
        Path to the file.

    Returns
    -------
    bool
        True if file exists and size > 0, else False.
    """
    try:
        return os.path.getsize(f) > 0
    except OSError:
        return False