---
title: Favorita
---


::: datasetsforecast.favorita.numpy_balance

::: datasetsforecast.favorita.numpy_ffill

::: datasetsforecast.favorita.numpy_bfill

::: datasetsforecast.favorita.one_hot_encoding

::: datasetsforecast.favorita.nested_one_hot_encoding

::: datasetsforecast.favorita.get_levels_from_S_df

::: datasetsforecast.favorita.distance_to_holiday

::: datasetsforecast.favorita.make_holidays_distance_df

::: datasetsforecast.favorita.CodeTimer

::: datasetsforecast.favorita.Favorita200

::: datasetsforecast.favorita.Favorita500

::: datasetsforecast.favorita.FavoritaComplete

::: datasetsforecast.favorita.FavoritaRawData

```python
from datasetsforecast.favorita import FavoritaRawData
verbose = True
group = 'Favorita200'  # 'Favorita500', 'FavoritaComplete'
directory = './data/favorita'  # directory = f's3://favorita'
filter_items, filter_stores, filter_dates, raw_group_data = FavoritaRawData._load_raw_group_data(directory=directory, group=group, verbose=verbose)
n_items = len(filter_items)
n_stores = len(filter_stores)
n_dates = len(filter_dates)
print('\n')
print('n_stores: \t', n_stores)
print('n_items: \t', n_items)
print('n_dates: \t', n_dates)
print('n_items * n_dates: \t\t', n_items * n_dates)
print('n_items * n_stores: \t\t', n_items * n_stores)
print('n_items * n_dates * n_stores: \t', n_items * n_dates * n_stores)
```

::: datasetsforecast.favorita.unzip

::: datasetsforecast.favorita.FavoritaData

::: datasetsforecast.favorita.load

**Example:**

```python
# Qualitative evaluation of hierarchical data
from datasetsforecast.favorita import FavoritaData
from hierarchicalforecast.utils import HierarchicalPlot

group = 'Favorita200' # 'Favorita500', 'FavoritaComplete'
directory = './data/favorita'
Y_df, S_df, tags = FavoritaData.load(directory=directory, group=group)

Y_item_df = Y_df[Y_df.item_id==1916577] # 112830, 1501570, 1916577
Y_item_df = Y_item_df.rename(columns={'hier_id': 'unique_id'})
Y_item_df = Y_item_df.set_index('unique_id')
del Y_item_df['item_id']

hplots = HierarchicalPlot(S=S_df, tags=tags)
hplots.plot_hierarchically_linked_series(
    Y_df=Y_item_df, bottom_series='store_[40]',
)
```

::: datasetsforecast.favorita.load_preprocessed
