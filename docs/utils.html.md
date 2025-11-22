---
title: Utils
description: Utility functions for datasetsforecast
---

##

::: datasetsforecast.utils.download_file

::: datasetsforecast.utils.extract_file

::: datasetsforecast.utils.async_download_files

```python
import os
import tempfile

import requests
```

```python
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

::: datasetsforecast.utils.download_files

```python
with tempfile.TemporaryDirectory() as tmp:
    tmp = Path(tmp)
    fname = tmp / 'script.py'
    fname.write_text(f"""
from datasetsforecast.utils import download_files

download_files('{tmp.as_posix()}', {urls})
    """)
    !python {fname}
    fname.unlink()
    files = list(tmp.iterdir())
    assert len(files) == len(urls)
```

::: datasetsforecast.utils.Info
    handler: python
    options:
      show_if_no_docstring: false
