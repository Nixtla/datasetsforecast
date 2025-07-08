#| eval: false
import matplotlib.pyplot as plt
from fastcore.test import test_eq
from nbdev.showdoc import show_doc
show_doc(numpy_balance, title_level=4)
show_doc(numpy_ffill, title_level=4)
show_doc(numpy_bfill, title_level=4)
show_doc(one_hot_encoding, title_level=4)
show_doc(nested_one_hot_encoding, title_level=4)
show_doc(get_levels_from_S_df, title_level=4)
show_doc(FavoritaRawData, title_level=4)
show_doc(FavoritaRawData._load_raw_group_data, title_level=4)
show_doc(FavoritaData, title_level=4)
show_doc(FavoritaData.load_preprocessed, title_level=4)
show_doc(FavoritaData.load, title_level=4)
#| eval: false
verbose = True
group = 'Favorita200'
# group = 'Favorita500'
# group = 'FavoritaComplete'
directory = './data/favorita'
# directory = f's3://favorita'
#| eval: false
filter_items, filter_stores, filter_dates, raw_group_data = \
    FavoritaRawData._load_raw_group_data(directory=directory, group=group, verbose=verbose)

S_df, item_store_df, static_bottom, static_agg = \
                    FavoritaData._get_static_data(filter_items=filter_items, 
                                                  filter_stores=filter_stores,
                                                  items=raw_group_data['items'], 
                                                  store_info=raw_group_data['store_info'], 
                                                  temporal=raw_group_data['temporal'], 
                                                  verbose=verbose)
#| eval: false
static_agg.head(5)
#| eval: false
static_bottom.head(5)
#| eval: false
temporal_bottom = FavoritaData._get_temporal_bottom(temporal=raw_group_data['temporal'],
                                                    item_store_df=item_store_df,
                                                    filter_dates=filter_dates,
                                                    verbose=verbose)
temporal_bottom.head(5)
#| eval: false
temporal_agg = FavoritaData._get_temporal_agg(filter_items=filter_items,
                                              filter_stores=filter_stores,
                                              filter_dates=filter_dates,
                                              oil=raw_group_data['oil'],
                                              holidays=raw_group_data['holidays'],
                                              transactions=raw_group_data['transactions'],
                                              temporal_bottom=temporal_bottom, verbose=verbose)
temporal_agg.head(5)
# #| hide
# #| eval: false
# # Test the equality of created and loaded datasets columns and rows
# static_agg1, static_bottom1, temporal_agg1, temporal_bottom1, S_df1 = \
#                         FavoritaData.load_preprocessed(directory=directory, group=group, cache=False)

# static_agg2, static_bottom2, temporal_agg2, temporal_bottom2, S_df2 = \
#                         FavoritaData.load_preprocessed(directory=directory, group=group)

# test_eq(len(static_agg1)+len(static_agg1.columns), 
#         len(static_agg2)+len(static_agg2.columns))
# test_eq(len(static_bottom1)+len(static_bottom1.columns), 
#         len(static_bottom2)+len(static_bottom2.columns))

# test_eq(len(temporal_agg1)+len(temporal_agg1.columns), 
#         len(temporal_agg2)+len(temporal_agg2.columns))
# test_eq(len(temporal_bottom1)+len(temporal_bottom1.columns), 
#         len(temporal_bottom2)+len(temporal_bottom2.columns))

