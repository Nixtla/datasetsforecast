import io
import json
import logging
import os
import pickle
import shutil
import stat
import sys
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from datasetsforecast import utils
from datasetsforecast.hierarchical import HierarchicalData, HierarchicalInfo
from datasetsforecast.long_horizon import LongHorizon, LongHorizonInfo
from datasetsforecast.m4 import M4, M4Evaluation
from datasetsforecast.m5 import M5, M5Evaluation
from datasetsforecast.utils import download_file, load_cache, safe_extract, save_cache


def _make_zip(path, members):
    with zipfile.ZipFile(path, 'w') as zf:
        for name, content in members.items():
            zf.writestr(name, content)
    return path


@pytest.mark.parametrize('bad_name', ['../evil.txt', 'sub/../../evil.txt', '/abs/evil.txt', 'C:\\evil.txt', '..\\evil.txt'])
def test_safe_extract_rejects_traversal(tmp_path, bad_name):
    archive = _make_zip(tmp_path / 'bad.zip', {'ok.txt': 'ok', bad_name: 'evil'})
    target = tmp_path / 'target'
    target.mkdir()
    with pytest.raises(ValueError, match='Unsafe path'):
        safe_extract(archive, target)
    # nothing is written, not even the valid member
    assert list(target.iterdir()) == []
    assert not (tmp_path / 'evil.txt').exists()


def test_safe_extract_extracts_valid_archive(tmp_path):
    archive = _make_zip(tmp_path / 'good.zip', {'a.csv': 'x', 'nested/b.csv': 'y'})
    target = tmp_path / 'target'
    safe_extract(archive, target)
    assert (target / 'a.csv').read_text() == 'x'
    assert (target / 'nested' / 'b.csv').read_text() == 'y'


def test_safe_extract_rejects_non_zip(tmp_path):
    import tarfile
    member = tmp_path / 'evil.txt'
    member.write_text('evil')
    archive = tmp_path / 'bad.tar'
    with tarfile.open(archive, 'w') as tf:
        tf.add(member, arcname='../evil.txt')
    target = tmp_path / 'target'
    target.mkdir()
    with pytest.raises(ValueError, match='Unsupported archive format'):
        safe_extract(archive, target)
    assert list(target.iterdir()) == []


def _symlink_or_skip(link, target, target_is_directory):
    try:
        os.symlink(target, link, target_is_directory=target_is_directory)
    except (OSError, NotImplementedError):
        pytest.skip('symlinks not supported on this platform')


class _FakeResponse:
    def __init__(self, content):
        self.content = content
        self.headers = {'content-length': str(len(content))}

    def raise_for_status(self):
        pass

    def iter_content(self, block_size):
        buf = io.BytesIO(self.content)
        while chunk := buf.read(block_size):
            yield chunk


def _zip_bytes(members):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as zf:
        for name, content in members.items():
            zf.writestr(name, content)
    return buf.getvalue()


@pytest.fixture
def serve_zip(monkeypatch):
    """Makes `requests.get` inside utils return the given zip bytes."""
    def _serve(members):
        content = _zip_bytes(members)
        monkeypatch.setattr(utils.requests, 'get', lambda *a, **k: _FakeResponse(content))
    return _serve


def test_download_file_extracts_and_merges(tmp_path, serve_zip):
    target = tmp_path / 'ds'
    (target / 'nested').mkdir(parents=True)
    (target / 'nested' / 'keep.csv').write_text('keep')
    serve_zip({'a.csv': 'x', 'nested/b.csv': 'y'})
    download_file(target, 'https://example.com/data.zip', decompress=True)
    assert (target / 'a.csv').read_text() == 'x'
    assert (target / 'nested' / 'b.csv').read_text() == 'y'
    assert (target / 'nested' / 'keep.csv').read_text() == 'keep'
    # staging directory is cleaned up
    assert [p.name for p in tmp_path.iterdir()] == ['ds']


def test_download_file_failed_extraction_leaves_no_partial_files(tmp_path, serve_zip):
    target = tmp_path / 'ds'
    serve_zip({'a.csv': 'x', '../evil.csv': 'evil'})
    with pytest.raises(ValueError, match='Unsafe path'):
        download_file(target, 'https://example.com/data.zip', decompress=True)
    assert not (target / 'a.csv').exists()
    assert not (tmp_path / 'evil.csv').exists()
    assert [p.name for p in tmp_path.iterdir()] == ['ds']


class _Payload:
    """Pickle payload that creates a marker file when unpickled."""
    def __init__(self, marker):
        self.marker = str(marker)

    def __reduce__(self):
        return (open, (self.marker, 'w'))


def _typed_frame():
    df = pd.DataFrame({
        'unique_id': pd.Categorical(['a', 'a', 'b']),
        'ds': pd.to_datetime(['2020-01-01', '2020-01-02', '2020-01-01']),
        'y': np.array([1.5, 2.5, 3.5], dtype=np.float32),
        'u8': np.array([1, 2, 3], dtype=np.uint8),
        'u16': np.array([1, 2, 3], dtype=np.uint16),
        'int_obj': pd.Series([1, 2, 3], dtype=object),
    })
    df.index = [10, 20, 30]
    return df


def test_cache_round_trip(tmp_path):
    Y_df = _typed_frame()
    S_df = pd.DataFrame({'x': [1.0, 0.0]}, index=pd.Index(['t1', 't2'], name='unique_id'))
    tags = {'Level2': ['t1', 't2'], 'Level1': ['t0']}
    cache_dir = tmp_path / '.cache' / 'ds' / 'g'
    save_cache(cache_dir, {'Y_df': Y_df, 'X_df': None, 'S_df': S_df}, extra={'tags': tags})

    cached = load_cache(cache_dir, ['Y_df', 'X_df', 'S_df'])
    assert cached is not None
    frames, extra = cached
    pd.testing.assert_frame_equal(frames['Y_df'], Y_df, check_dtype=True)
    pd.testing.assert_frame_equal(frames['S_df'], S_df, check_dtype=True)
    assert frames['X_df'] is None
    assert extra == {'tags': tags}
    assert list(extra['tags']) == ['Level2', 'Level1']
    # no leftover temp directories
    assert [p.name for p in cache_dir.parent.iterdir()] == ['g']


def test_cache_overwrite(tmp_path):
    cache_dir = tmp_path / 'g'
    save_cache(cache_dir, {'Y_df': pd.DataFrame({'y': [1]})})
    save_cache(cache_dir, {'Y_df': pd.DataFrame({'y': [2]})})
    frames, _ = load_cache(cache_dir, ['Y_df'])
    assert frames['Y_df']['y'].tolist() == [2]


def test_cache_missing_returns_none(tmp_path):
    assert load_cache(tmp_path / 'nothing', ['Y_df']) is None


def test_cache_version_mismatch_returns_none(tmp_path):
    cache_dir = tmp_path / 'g'
    save_cache(cache_dir, {'Y_df': pd.DataFrame({'y': [1]})})
    meta = json.loads((cache_dir / 'meta.json').read_text())
    meta['version'] = 0
    (cache_dir / 'meta.json').write_text(json.dumps(meta))
    assert load_cache(cache_dir, ['Y_df']) is None


def test_cache_failed_write_is_not_visible(tmp_path, monkeypatch, caplog):
    calls = []
    original = pd.DataFrame.to_parquet

    def failing_to_parquet(self, *args, **kwargs):
        calls.append(1)
        if len(calls) == 2:
            raise OSError('disk full')
        return original(self, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, 'to_parquet', failing_to_parquet)
    cache_dir = tmp_path / 'g'
    with caplog.at_level(logging.WARNING):
        save_cache(cache_dir, {'Y_df': pd.DataFrame({'y': [1]}), 'X_df': pd.DataFrame({'x': [1]})})
    assert 'Could not write cache' in caplog.text
    assert not cache_dir.exists()
    assert list(tmp_path.iterdir()) == []
    assert load_cache(cache_dir, ['Y_df', 'X_df']) is None


def test_cache_planted_pickle_is_not_executed(tmp_path, caplog):
    marker = tmp_path / 'pwned'
    cache_dir = tmp_path / 'g'
    save_cache(cache_dir, {'Y_df': pd.DataFrame({'y': [1]})})
    (cache_dir / 'Y_df.parquet').write_bytes(pickle.dumps(_Payload(marker)))
    with caplog.at_level(logging.WARNING):
        assert load_cache(cache_dir, ['Y_df']) is None
    assert 'Ignoring unreadable cache' in caplog.text
    assert not marker.exists()


def test_payload_is_live(tmp_path):
    # sanity check that the payload used above really executes when unpickled
    marker = tmp_path / 'pwned'
    pickle.loads(pickle.dumps(_Payload(marker)))
    assert marker.exists()
    os.remove(marker)


def _plant_legacy_pickle(path, marker):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(pickle.dumps((_Payload(marker), None, None)))


def _write_long_horizon_fixture(directory):
    name = LongHorizonInfo['ETTh1'].name
    base = directory / 'longhorizon' / 'datasets' / name / 'S'
    base.mkdir(parents=True)
    pd.DataFrame({
        'unique_id': ['OT', 'OT', 'OT'],
        'ds': ['2016-07-01 00:00:00', '2016-07-01 01:00:00', '2016-07-01 02:00:00'],
        'y': [0.1, 0.2, 0.3],
    }).to_csv(base / 'df_y.csv', index=False)
    pd.DataFrame({
        'ds': ['2016-07-01 00:00:00', '2016-07-01 01:00:00', '2016-07-01 02:00:00'],
        'ex_1': [1.0, 2.0, 3.0],
    }).to_csv(base / 'df_x.csv', index=False)


def test_long_horizon_ignores_legacy_pickle(tmp_path, monkeypatch):
    marker = tmp_path / 'pwned'
    _write_long_horizon_fixture(tmp_path)
    _plant_legacy_pickle(tmp_path / 'longhorizon' / 'datasets' / 'ETTh1.p', marker)
    monkeypatch.setattr(LongHorizon, 'download', lambda directory: None)

    Y_df, X_df, S_df = LongHorizon.load(str(tmp_path), 'ETTh1')
    assert not marker.exists()
    assert Y_df['y'].tolist() == [0.1, 0.2, 0.3]
    assert X_df['ex_1'].tolist() == [1.0, 2.0, 3.0]
    assert S_df is None

    # the second load comes from the parquet cache (CSVs are gone) and matches the fresh one
    shutil.rmtree(tmp_path / 'longhorizon' / 'datasets' / LongHorizonInfo['ETTh1'].name)
    Y_c, X_c, S_c = LongHorizon.load(str(tmp_path), 'ETTh1')
    assert (tmp_path / '.cache' / 'longhorizon' / 'ETTh1' / 'meta.json').is_file()
    pd.testing.assert_frame_equal(Y_c, Y_df)
    pd.testing.assert_frame_equal(X_c, X_df)
    assert S_c is None
    assert not marker.exists()


def _write_m4_fixture(directory):
    base = directory / 'm4' / 'datasets'
    base.mkdir(parents=True)
    pd.DataFrame({'M4id': ['H1', 'H2', 'Y1'], 'category': ['Other', 'Micro', 'Macro']}).to_csv(base / 'M4-info.csv', index=False)
    pd.DataFrame({'V1': ['H1', 'H2'], 'V2': [1.0, 4.0], 'V3': [2.0, 5.0], 'V4': [3.0, None]}).to_csv(base / 'Hourly-train.csv', index=False)
    pd.DataFrame({'V1': ['H1', 'H2'], 'V2': [10.0, 20.0]}).to_csv(base / 'Hourly-test.csv', index=False)


def test_m4_ignores_legacy_pickle(tmp_path, monkeypatch):
    marker = tmp_path / 'pwned'
    _write_m4_fixture(tmp_path)
    _plant_legacy_pickle(tmp_path / 'm4' / 'datasets' / 'Hourly.p', marker)
    monkeypatch.setattr(M4, 'download', lambda directory, group=None: None)

    Y_df, X_df, S_df = M4.load(str(tmp_path), 'Hourly')
    assert not marker.exists()
    assert Y_df.query('unique_id == "H1"')['y'].tolist() == [1.0, 2.0, 3.0, 10.0]
    assert Y_df.query('unique_id == "H2"')['ds'].tolist() == [1, 2, 3]
    assert X_df is None
    assert S_df['unique_id'].tolist() == ['H1', 'H2']

    for f in ('Hourly-train.csv', 'Hourly-test.csv', 'M4-info.csv'):
        (tmp_path / 'm4' / 'datasets' / f).unlink()
    Y_c, X_c, S_c = M4.load(str(tmp_path), 'Hourly')
    pd.testing.assert_frame_equal(Y_c, Y_df)
    pd.testing.assert_frame_equal(S_c, S_df)
    assert X_c is None
    assert not marker.exists()


def _write_m5_fixture(directory):
    base = directory / 'm5' / 'datasets'
    base.mkdir(parents=True)
    pd.DataFrame({
        'date': ['2011-01-29', '2011-01-30', '2011-01-31'],
        'wm_yr_wk': [11101, 11101, 11101],
        'event_name_1': [None, 'SuperBowl', None],
        'event_type_1': [None, 'Sporting', None],
        'event_name_2': [None, None, None],
        'event_type_2': [None, None, None],
        'snap_CA': [0, 1, 0],
        'snap_TX': [0, 0, 1],
        'snap_WI': [1, 0, 0],
    }).to_csv(base / 'calendar.csv', index=False)
    pd.DataFrame({
        'store_id': ['CA_1', 'CA_1'],
        'item_id': ['FOODS_1_001', 'FOODS_1_002'],
        'wm_yr_wk': [11101, 11101],
        'sell_price': [2.0, 3.5],
    }).to_csv(base / 'sell_prices.csv', index=False)
    ids = {
        'item_id': ['FOODS_1_001', 'FOODS_1_002'],
        'dept_id': ['FOODS_1', 'FOODS_1'],
        'cat_id': ['FOODS', 'FOODS'],
        'store_id': ['CA_1', 'CA_1'],
        'state_id': ['CA', 'CA'],
    }
    pd.DataFrame({**ids, 'd_1': [0.0, 1.0], 'd_2': [2.0, 3.0]}).to_csv(base / 'sales_train_evaluation.csv', index=False)
    pd.DataFrame({**ids, 'd_3': [4.0, 5.0]}).to_csv(base / 'sales_test_evaluation.csv', index=False)


def test_m5_ignores_legacy_pickle(tmp_path, monkeypatch):
    marker = tmp_path / 'pwned'
    _write_m5_fixture(tmp_path)
    _plant_legacy_pickle(tmp_path / 'm5' / 'datasets' / 'm5.p', marker)
    monkeypatch.setattr(M5, 'download', lambda directory: None)

    Y_df, X_df, S_df = M5.load(str(tmp_path))
    assert not marker.exists()
    # the leading zero of FOODS_1_001 is removed
    assert Y_df['y'].tolist() == [2.0, 4.0, 1.0, 3.0, 5.0]
    assert len(S_df) == 2

    shutil.rmtree(tmp_path / 'm5' / 'datasets')
    Y_c, X_c, S_c = M5.load(str(tmp_path))
    pd.testing.assert_frame_equal(Y_c, Y_df)
    pd.testing.assert_frame_equal(X_c, X_df)
    pd.testing.assert_frame_equal(S_c, S_df)
    assert not marker.exists()


def _write_hierarchical_fixture(directory, group):
    base = directory / 'hierarchical' / group
    base.mkdir(parents=True)
    pd.DataFrame({'A': [1.0, 1.0, 0.0], 'B': [1.0, 0.0, 1.0]}, index=['Total', 'A', 'B']).to_csv(base / 'agg_mat.csv')
    pd.DataFrame({'Total': [3.0, 7.0], 'A': [1.0, 3.0], 'B': [2.0, 4.0]}, index=['2020-01-01', '2020-02-01']).to_csv(base / 'data.csv')


def test_hierarchical_ignores_legacy_pickle(tmp_path, monkeypatch):
    group = 'TourismSmall'
    marker = tmp_path / 'pwned'
    _write_hierarchical_fixture(tmp_path, group)
    _plant_legacy_pickle(tmp_path / 'hierarchical' / f'{group}.p', marker)
    monkeypatch.setattr(HierarchicalData, 'download', lambda directory: None)

    Y_df, S_df, tags = HierarchicalData.load(str(tmp_path), group)
    assert not marker.exists()
    assert Y_df.query('unique_id == "Total"')['y'].tolist() == [3.0, 7.0]
    names = HierarchicalInfo[group].tags_names
    assert list(tags) == list(names[:2])
    assert tags[names[0]].tolist() == ['Total']
    assert tags[names[1]].tolist() == ['A', 'B']

    shutil.rmtree(tmp_path / 'hierarchical' / group)
    Y_c, S_c, tags_c = HierarchicalData.load(str(tmp_path), group)
    pd.testing.assert_frame_equal(Y_c, Y_df)
    pd.testing.assert_frame_equal(S_c, S_df)
    assert list(tags_c) == list(tags)
    for k in tags:
        np.testing.assert_array_equal(tags_c[k], tags[k])
        assert tags_c[k].dtype == tags[k].dtype
    assert not marker.exists()


def test_m4_benchmark_archive_cannot_plant_cache(tmp_path, monkeypatch, serve_zip):
    marker = tmp_path / 'pwned'
    _write_m4_fixture(tmp_path)
    serve_zip({
        'Hourly.p': pickle.dumps((_Payload(marker), None, None)),
        'submission-evil.csv': 'id,F1\nH1,1.0\n',
    })
    # the archive holds two members, so reading it as a csv fails afterwards; only the side effects matter
    with pytest.raises(ValueError):
        M4Evaluation.load_benchmark(str(tmp_path), 'Hourly', 'https://example.com/evil.zip')
    assert (tmp_path / 'm4' / 'benchmarks' / 'Hourly.p').exists()
    assert not (tmp_path / 'm4' / 'datasets' / 'Hourly.p').exists()
    assert not (tmp_path / '.cache').exists()

    monkeypatch.setattr(M4, 'download', lambda directory, group=None: None)
    M4.load(str(tmp_path), 'Hourly')
    assert not marker.exists()


def test_m5_benchmark_archive_cannot_plant_cache(tmp_path, monkeypatch, serve_zip):
    marker = tmp_path / 'pwned'
    _write_m5_fixture(tmp_path)
    serve_zip({
        'm5.p': pickle.dumps((_Payload(marker), None, None)),
        'submission.csv': 'id,F1\nFOODS_1_001_CA_1_evaluation,1.0\n',
    })
    with pytest.raises(ValueError):
        M5Evaluation.load_benchmark(str(tmp_path), 'https://example.com/evil.zip')
    assert (tmp_path / 'm5' / 'benchmarks' / 'm5.p').exists()
    assert not (tmp_path / 'm5' / 'datasets' / 'm5.p').exists()
    assert not (tmp_path / '.cache').exists()

    monkeypatch.setattr(M5, 'download', lambda directory: None)
    M5.load(str(tmp_path))
    assert not marker.exists()


@pytest.mark.parametrize('member', ['nested/pwn.txt', 'nested'])
def test_download_file_refuses_symlinked_directory(tmp_path, serve_zip, member):
    outside = tmp_path / 'outside'
    outside.mkdir()
    target = tmp_path / 'ds'
    target.mkdir()
    _symlink_or_skip(target / 'nested', outside, target_is_directory=True)
    serve_zip({'nested/pwn.txt': 'pwn'} if member == 'nested/pwn.txt' else {'nested': 'file replacing the link'})
    with pytest.raises(ValueError, match='symlink'):
        download_file(target, 'https://example.com/data.zip', decompress=True)
    assert list(outside.iterdir()) == []


def test_download_file_refuses_symlinked_file(tmp_path, serve_zip):
    outside = tmp_path / 'outside.txt'
    outside.write_text('original')
    target = tmp_path / 'ds'
    target.mkdir()
    _symlink_or_skip(target / 'a.csv', outside, target_is_directory=False)
    serve_zip({'a.csv': 'pwn'})
    with pytest.raises(ValueError, match='symlink'):
        download_file(target, 'https://example.com/data.zip', decompress=True)
    assert outside.read_text() == 'original'


posix_permissions = pytest.mark.skipif(
    sys.platform == 'win32' or (hasattr(os, 'geteuid') and os.geteuid() == 0),
    reason='needs POSIX permissions enforced for a non-root user',
)


@pytest.fixture
def umask_022():
    old = os.umask(0o022)
    yield
    os.umask(old)


@posix_permissions
def test_cache_dir_follows_umask(tmp_path, umask_022):
    cache_dir = tmp_path / '.cache' / 'ds' / 'g'
    save_cache(cache_dir, {'Y_df': pd.DataFrame({'y': [1]})})
    assert stat.S_IMODE(cache_dir.stat().st_mode) == 0o755
    assert stat.S_IMODE((cache_dir / 'meta.json').stat().st_mode) == 0o644


def test_cache_permission_error_falls_back(tmp_path, monkeypatch, caplog):
    cache_dir = tmp_path / 'g'
    save_cache(cache_dir, {'Y_df': pd.DataFrame({'y': [1]})})
    original = Path.is_file

    def denied(self):
        if self.name == 'meta.json':
            raise PermissionError(13, 'Permission denied', str(self))
        return original(self)

    # on Python 3.10-3.13 pathlib re-raises PermissionError from is_file
    monkeypatch.setattr(Path, 'is_file', denied)
    with caplog.at_level(logging.WARNING):
        assert load_cache(cache_dir, ['Y_df']) is None
    assert 'Ignoring unreadable cache' in caplog.text


@posix_permissions
def test_cache_unreadable_dir_falls_back(tmp_path):
    cache_dir = tmp_path / 'g'
    save_cache(cache_dir, {'Y_df': pd.DataFrame({'y': [1]})})
    cache_dir.chmod(0)
    try:
        assert load_cache(cache_dir, ['Y_df']) is None
    finally:
        cache_dir.chmod(0o755)


@posix_permissions
def test_download_file_does_not_write_to_parent(tmp_path, monkeypatch):
    parent = tmp_path / 'readonly'
    target = parent / 'data'
    target.mkdir(parents=True)
    parent.chmod(0o555)
    calls = []
    content = _zip_bytes({'a.csv': 'x', 'nested/b.csv': 'y'})

    def fake_get(*args, **kwargs):
        calls.append(1)
        return _FakeResponse(content)

    monkeypatch.setattr(utils.requests, 'get', fake_get)
    try:
        download_file(target, 'https://example.com/data.zip', decompress=True)
    finally:
        parent.chmod(0o755)
    assert len(calls) == 1
    assert (target / 'a.csv').read_text() == 'x'
    assert (target / 'nested' / 'b.csv').read_text() == 'y'
    assert not any(p.name.startswith('.extract-') for p in target.iterdir())
