__all__ = ['M3Info', 'Yearly', 'Quarterly', 'Monthly', 'Other', 'M3']


import os
from dataclasses import dataclass
from typing import Optional, Tuple

import pandas as pd

from .utils import Info, convert_tsf_to_dataframe, download_file


@dataclass
class Yearly:
    seasonality: int = 1
    horizon: int = 6
    freq: str = 'YE'
    name: str = 'Yearly'
    n_ts: int = 645
    source_url: str = 'https://zenodo.org/api/records/4656222/files/m3_yearly_dataset.zip/content'
    file_name: str = 'm3_yearly_dataset'

@dataclass
class Quarterly:
    seasonality: int = 4
    horizon: int = 8
    freq: str = 'QE'
    name: str = 'Quarterly'
    n_ts: int = 756
    source_url: str = 'https://zenodo.org/api/records/4656262/files/m3_quarterly_dataset.zip/content'
    file_name: str = 'm3_quarterly_dataset'

@dataclass
class Monthly:
    seasonality: int = 12
    horizon: int = 18
    freq: str = 'ME'
    name: str = 'Monthly'
    n_ts: int = 1428
    source_url: str = 'https://zenodo.org/api/records/4656298/files/m3_monthly_dataset.zip/content'
    file_name: str = 'm3_monthly_dataset'

@dataclass
class Other:
    seasonality: int = 1
    horizon: int = 8
    freq: str = 'D'
    name: str = 'Other'
    n_ts: int = 174
    source_url: str = 'https://zenodo.org/api/records/4656335/files/m3_other_dataset.zip/content'
    file_name: str = 'm3_other_dataset'


M3Info = Info((Yearly, Quarterly, Monthly, Other))


@dataclass
class M3:

    @staticmethod
    def load(directory: str,
             group: str) -> Tuple[pd.DataFrame,
                                  Optional[pd.DataFrame],
                                  Optional[pd.DataFrame]]:
        """
        Downloads and loads M3 data.

        Args:
            directory (str): Directory where data will be downloaded.
            group (str): Group name.
                Allowed groups: 'Yearly', 'Quarterly', 'Monthly', 'Other'.

        Returns:
            pd.DataFrame: Target time series with columns ['unique_id', 'ds', 'y'].
        """
        class_group = M3Info.get_group(group)
        M3.download(directory, class_group)

        path = f'{directory}/m3/datasets/'
        tsf_file = f'{path}/{class_group.file_name}.tsf'

        loaded_data, *_ = convert_tsf_to_dataframe(tsf_file)

        freq = pd.tseries.frequencies.to_offset(class_group.freq)

        rows = []
        for i, (_, row) in enumerate(loaded_data.iterrows()):
            unique_id = class_group.name[0] + str(i + 1)
            start_timestamp = row.get('start_timestamp', pd.Timestamp('1970-01-01'))
            values = row['series_value']
            dates = pd.date_range(
                start=start_timestamp,
                periods=len(values),
                freq=freq,
            )
            for ds, y in zip(dates, values):
                rows.append({'unique_id': unique_id, 'ds': ds, 'y': float(y)})

        df = pd.DataFrame(rows)
        df = df.sort_values(['unique_id', 'ds']).reset_index(drop=True)

        return df, None, None

    @staticmethod
    def download(directory: str, class_group) -> None:
        """
        Download M3 Dataset.

        Args:
            directory (str): Directory path to download dataset.
            class_group: Dataclass with source_url and file_name.
        """
        path = f'{directory}/m3/datasets/'
        tsf_file = f'{path}/{class_group.file_name}.tsf'
        if not os.path.exists(tsf_file):
            download_file(
                path,
                class_group.source_url,
                decompress=True,
                filename=f'{class_group.file_name}.zip',
            )
