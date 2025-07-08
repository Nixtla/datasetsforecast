from fastcore.test import test_close
for group, _ in HierarchicalInfo:
    #if group not in ['OldTraffic', 'OldTourismLarge']:
    Y_df, S_df, tags = HierarchicalData.load('./data', group)
    assert all(S_df.loc[cats].values.sum() == S_df.shape[1] for _, cats in tags.items())
    assert len(S_df) == sum(len(v) for _, v in tags.items()), group
    S_hiers = [S_df.loc[cats].values * np.arange(1, len(cats) + 1).reshape(-1, 1) for _, cats in tags.items()]
    S_hiers = np.vstack(S_hiers)
    S_hiers = S_hiers.sum(axis=0)
    is_strictly_hierarchical = np.array_equal(S_hiers, np.sort(S_hiers))
    print(f'Is {group} strictly hierarchical? {is_strictly_hierarchical}')
    
    # test S recovers Y_df
    for key, hiers in tags.items():
        for ts, bottom_ts in S_df.loc[hiers].iterrows():
            actual_bottom_ts = bottom_ts.loc[lambda x: x == 1].index
            test_close(
                Y_df.query('unique_id == @ts')['y'].sum(), 
                Y_df.query('unique_id in @actual_bottom_ts')['y'].sum()
            )
# Meta information
meta = pd.DataFrame(
    columns=['Frequency', 'Series', 'Levels', 'Observations per Series', 'Test Observations per Series', 'Horizon'],
    index=pd.Index(HierarchicalInfo.groups, name='Dataset')
)
for group, cls_group in HierarchicalInfo:
    #if group not in ['OldTraffic', 'OldTourismLarge']:
    Y_df, S_df, tags = HierarchicalData.load('./data', group)
    meta.loc[group, 'Frequency'] = cls_group.freq
    meta.loc[group, 'Horizon'] = cls_group.horizon
    meta.loc[group, 'Papers\' horizon'] = int(cls_group.papers_horizon)
    meta.loc[group, 'Series'] = Y_df['unique_id'].nunique()
    meta.loc[group, 'Levels'] = len(tags)
    meta.loc[group, 'Observations per Series'] = Y_df.groupby('unique_id').size().unique().item()
    meta.loc[group, 'Test Observations per Series'] =  meta.loc[group, 'Observations per Series'] // 4
meta

