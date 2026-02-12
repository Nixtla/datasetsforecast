__all__ = ['logger', 'extract_file', 'download_file', 'async_download_files', 'download_files', 'convert_tsf_to_dataframe', 'Info']


import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional, Tuple, Union

import numpy as np
import pandas as pd

import aiohttp
import requests
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_file(filepath, directory):
    filepath = Path(filepath)
    if '.zip' in filepath.suffix:
        import zipfile
        logger.info('Decompressing zip file...')
        with zipfile.ZipFile(filepath, 'r') as zip_ref:
            zip_ref.extractall(directory)
    else:
        from patoolib import extract_archive
        extract_archive(filepath, outdir=directory)
    logger.info(f'Successfully decompressed {filepath}')

def download_file(
    directory: Union[str, Path],
    source_url: str,
    decompress: bool = False,
    filename: Optional[str] = None,
    max_retries: int = 3,
) -> None:
    """Download data from source_url inside directory.

    Args:
        directory (str, Path): Custom directory where data will be downloaded.
        source_url (str): URL where data is hosted.
        decompress (bool): Whether to decompress downloaded file. Default False.
        filename (str, optional): Override filename for the downloaded file.
            If None, the filename is derived from the URL.
        max_retries (int): Maximum number of retry attempts on transient errors.
    """
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)

    if filename is None:
        filename = source_url.split('/')[-1]
        # On windows file must have only zip in suffix
        if '.zip' in filename:
            filename = Path(filename).stem + ".zip"

    filepath = directory / filename

    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; datasetsforecast/1.0; +https://github.com/Nixtla/datasetsforecast)',
    }

    for attempt in range(max_retries):
        try:
            # Streaming, so we can iterate over the response.
            r = requests.get(source_url, stream=True, headers=headers)
            r.raise_for_status()
            # Total size in bytes.
            total_size = int(r.headers.get('content-length', 0))
            block_size = 1024  # 1 Kibibyte

            t = tqdm(total=total_size, unit='iB', unit_scale=True)
            with open(filepath, 'wb') as f:
                for data in r.iter_content(block_size):
                    t.update(len(data))
                    f.write(data)
                    f.flush()
            t.close()

            if total_size != 0 and t.n != total_size:
                raise IOError(
                    f'Download incomplete: expected {total_size} bytes, '
                    f'got {t.n} bytes.'
                )

            size = filepath.stat().st_size
            logger.info(f'Successfully downloaded {filename}, {size}, bytes.')

            if decompress:
                extract_file(filepath, directory)

            return
        except (requests.exceptions.RequestException, IOError) as e:
            if filepath.exists():
                filepath.unlink()
            if attempt < max_retries - 1:
                wait = 2 ** (attempt + 1)
                logger.warning(
                    f'Download failed (attempt {attempt + 1}/{max_retries}): {e}. '
                    f'Retrying in {wait}s...'
                )
                time.sleep(wait)
            else:
                raise


async def _async_download_file(session: aiohttp.ClientSession, path: Path, source_url: str):
    async with session.get(source_url) as response:
        content = await response.read()
    fname = source_url.split('/')[-1]
    (path / fname).write_bytes(content)
    return fname


async def async_download_files(path: Union[str, Path], urls: Iterable[str]):
    """
    Asynchronously download files from urls inside path.

    Args:
        path (str, Path): Directory where files will be downloaded.
        urls (Iterable[str]): Iterable of URLs to download.

    Example:
    ```python
    import os
    import tempfile

    import requests
    gh_url = 'https://api.github.com/repos/Nixtla/datasetsforecast/contents/'
    base_url = 'https://raw.githubusercontent.com/Nixtla/datasetsforecast/main'

    headers = {}
    gh_token = os.getenv('GITHUB_TOKEN')
    if gh_token is not None:
        headers = {'Authorization': f'Bearer: {gh_token}'}
    resp = requests.get(gh_url, headers=headers)
    if resp.status_code != 200:
        raise Exception(resp.text)
    urls = [f'{base_url}/{e["path"]}' for e in resp.json() if e['type'] == 'file']
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        await async_download_files(tmp, urls)
        files = list(tmp.iterdir())
        assert len(files) == len(urls)
    ```
    """
    path = Path(path)
    path.mkdir(exist_ok=True, parents=True)
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url in urls:
            tasks.append(_async_download_file(session, path, url))
        for task in asyncio.as_completed(tasks):
            fname = await task
            logger.info(f'Downloaded: {fname}')


def download_files(directory: Union[str, Path], urls: Iterable[str]):
    """
    Download files from urls inside directory.

    Args:
        directory (str, Path): Directory where files will be downloaded.
        urls (Iterable[str]): Iterable of URLs to download.

    Example:

    ```python
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        fname = tmp / 'script.py'
        fname.write_text(f'''
    from datasetsforecast.utils import download_files

    download_files('{tmp.as_posix()}', {urls})
        ''')
        !python {fname}
        fname.unlink()
        files = list(tmp.iterdir())
        assert len(files) == len(urls)
    ```
    """
    try:
        asyncio.get_running_loop()
        raise Exception(
            "Can't use this function when there's already a running loop. "
            "Use `await async_download_files(...) instead.`"
        )
    except RuntimeError:
        # No running loop, safe to use asyncio.run()
        pass
    asyncio.run(async_download_files(directory, urls))


def convert_tsf_to_dataframe(
    full_file_path_and_name,
    replace_missing_vals_with="NaN",
    value_column_name="series_value",
):
    col_names = []
    col_types = []
    all_data = {}
    has_lines = False
    frequency = None
    forecast_horizon = None
    contain_missing_values = None
    contain_equal_length = None
    found_data_tag = False
    found_data_section = False

    with open(full_file_path_and_name, "r", encoding="cp1252") as file:
        for line in file:
            line = line.strip()

            if line:
                if line.startswith("@"):
                    if not line.startswith("@data"):
                        line_content = line.split(" ")
                        if line.startswith("@attribute"):
                            if len(line_content) != 3:
                                raise ValueError("Invalid meta-data specification.")

                            col_names.append(line_content[1])
                            col_types.append(line_content[2])
                        else:
                            if len(line_content) != 2:
                                raise ValueError("Invalid meta-data specification.")

                            if line.startswith("@frequency"):
                                frequency = line_content[1]
                            elif line.startswith("@horizon"):
                                forecast_horizon = int(line_content[1])
                            elif line.startswith("@missing"):
                                contain_missing_values = bool(
                                    line_content[1].lower() in ("yes", "true", "t", "1")
                                )
                            elif line.startswith("@equallength"):
                                contain_equal_length = bool(
                                    line_content[1].lower() in ("yes", "true", "t", "1")
                                )

                    else:
                        if len(col_names) == 0:
                            raise ValueError(
                                "Missing attribute section. "
                                "Attribute section must come before data."
                            )

                        found_data_tag = True
                elif not line.startswith("#"):
                    if len(col_names) == 0:
                        raise ValueError(
                            "Missing attribute section. "
                            "Attribute section must come before data."
                        )
                    elif not found_data_tag:
                        raise ValueError("Missing @data tag.")
                    else:
                        if not found_data_section:
                            found_data_section = True
                            all_series = []

                            for col in col_names:
                                all_data[col] = []

                        full_info = line.split(":")

                        if len(full_info) != (len(col_names) + 1):
                            raise ValueError("Missing attributes/values in series.")

                        series_str = full_info[-1]

                        if "?" in series_str:
                            series = series_str.split(",")
                            numeric_series = [
                                replace_missing_vals_with if val == "?" else float(val)
                                for val in series
                            ]
                            if all(v == replace_missing_vals_with for v in numeric_series):
                                raise ValueError(
                                    "All series values are missing. A given series "
                                    "should contain a set of comma separated numeric "
                                    "values. At least one numeric value should be "
                                    "there in a series."
                                )
                            all_series.append(np.array(numeric_series, dtype=np.float32))
                        else:
                            all_series.append(np.fromstring(series_str, sep=",", dtype=np.float32))

                        for name, col_type, value in zip(col_names, col_types, full_info):
                            if col_type == "numeric":
                                all_data[name].append(int(value))
                            elif col_type == "string":
                                all_data[name].append(value)
                            elif col_type == "date":
                                all_data[name].append(
                                    datetime.strptime(value, "%Y-%m-%d %H-%M-%S")
                                )
                            else:
                                raise ValueError("Invalid attribute type.")

                has_lines = True

        if not has_lines:
            raise ValueError("Empty file.")
        if len(col_names) == 0:
            raise ValueError("Missing attribute section.")
        if not found_data_section:
            raise ValueError("Missing series information under data section.")

        all_data[value_column_name] = all_series
        loaded_data = pd.DataFrame(all_data)

        return (
            loaded_data,
            frequency,
            forecast_horizon,
            contain_missing_values,
            contain_equal_length,
        )


@dataclass
class Info:
    """
    Info Dataclass of datasets.
    Args:
        groups (Tuple): Tuple of str groups
        class_groups (Tuple): Tuple of dataclasses.
    """
    class_groups: Tuple[Any, ...]
    groups: Tuple[str, ...] = field(init=False)

    def __post_init__(self):
        self.groups = tuple(cls_.__name__ for cls_ in self.class_groups)

    def get_group(self, group: str):
        """Gets dataclass of group."""
        if group not in self.groups:
            raise Exception(f'Unknown group {group}')
        return self.class_groups[self.groups.index(group)]

    def __getitem__(self, group: str):
        """Gets dataclass of group."""
        if group not in self.groups:
            raise Exception(f'Unknown group {group}')
        return self.class_groups[self.groups.index(group)]

    def __iter__(self):
        for group in self.groups:
            yield group, self.get_group(group)
