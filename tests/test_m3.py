for group, meta in M3Info:
    data, *_ = M3.load(directory='data', group=group)
    unique_elements = data.groupby(['unique_id', 'ds']).size()
    unique_ts = data.groupby('unique_id').size()

    assert (unique_elements != 1).sum() == 0, f'Duplicated records found: {group}'
    assert unique_ts.shape[0] == meta.n_ts, f'Number of time series not match: {group}'
