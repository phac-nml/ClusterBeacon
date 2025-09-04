import pandas as pd
from pathlib import Path
from typing import Union
from src.clusterbeacon.utils import file_valid


class DataLoader:
    def __init__(self, filepath, config):
        self.status = True
        self.errors = []
        self.df = pd.DataFrame()

        if file_valid(filepath):
            self.df = self.read_table(filepath=filepath)
            missing_cols = self.get_missing_cols(df=self.df, columns=config.config.fieldNames)
            if len(missing_cols) > 0:
                self.status = False
                self.errors.append(f'file {filepath} is missing columns identified in the config columns="{config.config.fieldNames}"')
        else:
            self.status = False
            self.errors.append(f'file {filepath} does not exist or is empty')
    
    @staticmethod
    def set_subtraction(columns, df_cols):
        return set(columns) - set(df_cols)
    
    def get_missing_cols(self, df, columns):
        return self.set_subtraction(columns=columns, df_cols=list(df.columns))
    
    @staticmethod
    def read_table(filepath: Union[str, Path]) -> pd.DataFrame:
        """
        Read a file into a pandas DataFrame, supporting CSV, TSV, Parquet, and Excel.

        Parameters
        ----------
        filepath : str or Path
            Path to the input file.

        Returns
        -------
        pd.DataFrame
            DataFrame containing the file contents.

        Raises
        ------
        ValueError
            If the file extension is not supported.
        """
        path = Path(filepath)
        ext = path.suffix.lower()

        if ext == ".csv":
            return pd.read_csv(path, header=0)
        elif ext == ".tsv":
            return pd.read_csv(path, sep="\t", header=0)
        elif ext == ".parquet":
            return pd.read_parquet(path)
        elif ext in (".xls", ".xlsx"):
            return pd.read_excel(path)
        else:
            raise ValueError(f"Unsupported file extension: {ext}")