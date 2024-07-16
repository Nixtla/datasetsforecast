# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/utils.ipynb.

# %% auto 0
__all__ = ['logger', 'extract_file', 'download_file', 'async_download_files', 'download_files', 'Info']

# %% ../nbs/utils.ipynb 2
import asyncio
import logging
import requests
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Tuple, Union

import aiohttp
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# %% ../nbs/utils.ipynb 3
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

def download_file(directory: str, source_url: str, decompress: bool = False) -> None:
    """Download data from source_ulr inside directory.

    Parameters
    ----------
    directory: str, Path
        Custom directory where data will be downloaded.
    source_url: str
        URL where data is hosted.
    decompress: bool
        Wheter decompress downloaded file. Default False.
    """
    if isinstance(directory, str):
        directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)

    filename = Path(source_url.split('/')[-1])

    # On windows file must have only zip in suffix
    if '.zip' in filename.suffix:
        filename = Path(filename).stem + ".zip"

    filepath = Path(f'{directory}/{filename}')

    # Streaming, so we can iterate over the response.
    headers = {'User-Agent': 'Mozilla/5.0'}
    r = requests.get(source_url, stream=True, headers=headers)
    # Total size in bytes.
    total_size = int(r.headers.get('content-length', 0))
    block_size = 1024 #1 Kibibyte

    t = tqdm(total=total_size, unit='iB', unit_scale=True)
    with open(filepath, 'wb') as f:
        for data in r.iter_content(block_size):
            t.update(len(data))
            f.write(data)
            f.flush()
    t.close()

    if total_size != 0 and t.n != total_size:
        logger.error('ERROR, something went wrong downloading data')

    size = filepath.stat().st_size
    logger.info(f'Successfully downloaded {filename}, {size}, bytes.')

    if decompress:
        extract_file(filepath, directory)

# %% ../nbs/utils.ipynb 4
async def _async_download_file(session: aiohttp.ClientSession, path: Path, source_url: str):
    async with session.get(source_url) as response:
        content = await response.read()
    fname = source_url.split('/')[-1]
    (path / fname).write_bytes(content)
    return fname

# %% ../nbs/utils.ipynb 5
async def async_download_files(path: Union[str, Path], urls: Iterable[str]):
    path = Path(path)
    path.mkdir(exist_ok=True, parents=True)
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url in urls:
            tasks.append(_async_download_file(session, path, url))
        for task in asyncio.as_completed(tasks):
            fname = await task
            logger.info(f'Downloaded: {fname}')

# %% ../nbs/utils.ipynb 8
def download_files(directory: Union[str, Path], urls: Iterable[str]):
    loop = asyncio.get_event_loop()
    if loop.is_running():
        raise Exception(
            "Can't use this function when there's already a running loop. "
            "Use `await async_download_files(...) instead.`"
        )
    asyncio.run(async_download_files(directory, urls))

# %% ../nbs/utils.ipynb 10
@dataclass
class Info:
    """
    Info Dataclass of datasets.
    Args:
        groups (Tuple): Tuple of str groups
        class_groups (Tuple): Tuple of dataclasses.
    """
    class_groups: Tuple[dataclass] 
    groups: Tuple[str] = field(init=False)
    
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