import os
import tempfile
from pathlib import Path

import pytest
import requests

from datasetsforecast.utils import async_download_files


@pytest.fixture
def github_urls():
    """Fixture to get GitHub repository file URLs."""
    gh_url = 'https://api.github.com/repos/Nixtla/datasetsforecast/contents/'
    base_url = 'https://raw.githubusercontent.com/Nixtla/datasetsforecast/main'

    headers = {}
    gh_token = os.getenv('GITHUB_TOKEN')
    if gh_token is not None:
        headers = {'Authorization': f'Bearer: {gh_token}'}

    resp = requests.get(gh_url, headers=headers)
    if resp.status_code != 200:
        pytest.skip(f"GitHub API request failed: {resp.text}")

    urls = [f'{base_url}/{e["path"]}' for e in resp.json() if e['type'] == 'file']
    return urls

# @pytest.mark.skip(reason="This test runs inconsistently in CI")
@pytest.mark.asyncio
async def test_async_download_files(github_urls):
    """Test async file downloading functionality."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        await async_download_files(tmp_path, github_urls)
        files = list(tmp_path.iterdir())
        assert len(files) == len(github_urls), f"Expected {len(github_urls)} files, got {len(files)}"


def test_download_files_via_subprocess(github_urls):
    """Test synchronous file downloading via subprocess execution."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        script_file = tmp_path / 'download_script.py'

        # Create a script that uses download_files
        script_content = f"""
from datasetsforecast.utils import download_files

download_files('{tmp_path.as_posix()}', {github_urls!r})
"""
        script_file.write_text(script_content)

        # Execute the script
        exit_code = os.system(f'python {script_file}')
        assert exit_code == 0, "Download script execution failed"

        # Clean up script file
        script_file.unlink()

        # Verify files were downloaded
        files = list(tmp_path.iterdir())
        assert len(files) == len(github_urls), f"Expected {len(github_urls)} files, got {len(files)}"
