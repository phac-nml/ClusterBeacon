
import pandas as pd
from typing import Optional
from src.clusterbeacon.classes.DataLoader import DataLoader
from src.clusterbeacon.classes.ConfigLoader import ConfigLoader

class Workflow:

    def __init__(self):
        self.cfg_path = ''
        self.ll_path = ''
        self.out_path = ''
        self.status = True
        self.errors = []


    def load(self):
        self.cfg = ConfigLoader(config_path=self.cfg_path)
        if not self.cfg.status:
            self.errors += self.cfg.errors
            self.status = False
            return
        
        self.data = DataLoader(filepath=self.ll_path, config=self.cfg)
        if not self.data.status:
            self.errors += self.data.errors
            self.status = False
            return

    @staticmethod
    def prep_heir_labels(
        df: pd.DataFrame,
        source_colname: str,
        split_delim: str = "|",
        code_split_idx: int = 1,
        code_delim: str = ".",
        col_prefix: str = "hc",
    ) -> pd.DataFrame:
        """
        From a source column like 'A|B|C', select one segment (by index), split it by a code delimiter,
        and create new hierarchical columns (hc1, hc2, ...) from the parts.

        Parameters
        ----------
        df : pd.DataFrame
            Input DataFrame.
        source_colname : str
            Column containing the composite string to parse.
        split_delim : str, default '|'
            Delimiter to split the source column.
        code_split_idx : int, default 1
            Index of the segment to select after the first split. Supports negative indexing.
        code_delim : str, default '.'
            Delimiter to split the selected code segment into hierarchical labels.
        col_prefix : str, default 'hc'
            Prefix for the created columns (e.g., hc1, hc2, ...).

        Returns
        -------
        pd.DataFrame
            The original DataFrame with new hierarchical columns added.

        Notes
        -----
        - Non-existent indices after the first split yield NaN for all new columns.
        - Whitespace around parts is stripped.
        - The number of created columns equals the maximum number of parts found in the selected segment.
        """
        # Select the code segment at code_split_idx
        code_series = (
            df[source_colname]
            .astype(str)
            .str.split(split_delim)
            .str.get(code_split_idx)   # safe even if index out of range -> NaN
        )

        # Split the selected segment into hierarchical parts
        parts = code_series.str.split(code_delim, expand=True)

        if isinstance(parts, pd.Series):
            # If there's only one part, expand=True still returns Series; normalize to DataFrame
            parts = parts.to_frame(0)

        # Strip whitespace on each part
        parts = parts.apply(lambda s: s.str.strip() if s.dtype == "object" else s)

        # Create columns hc_1, hc_2, ...
        for i in range(parts.shape[1]):
            df[f"{col_prefix}_{i+1}"] = parts[i]

        return df



