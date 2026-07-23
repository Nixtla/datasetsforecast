---
title: Favorita
description: Favorita dataset
---

##

::: datasetsforecast.favorita.FavoritaData
    handler: python
    options:
      show_if_no_docstring: false

::: datasetsforecast.favorita.load

#### Example

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

## Kaggle-Competition References

The evaluation metric of the Favorita Kaggle competition was the
normalized weighted root mean squared logarithmic error (NWRMSLE).
Perishable items have a score weight of 1.25; otherwise, the weight is
1.0.

$$ NWRMSLE = \sqrt{\frac{\sum^{n}_{i=1} w_{i}\left(log(\hat{y}_{i}+1)  - log(y_{i}+1)\right)^{2}}{\sum^{n}_{i=1} w_{i}}}$$

| Kaggle Competition Forecasting Methods | 16D ahead NWRMSLE |
|:---------------------------------------------------------------:|:-----:|
| [LGBM](https://www.kaggle.com/shixw125/1st-place-lgb-model-public-0-506-private-0-511/comments) \[1\] | 0.5091 |
| [Seq2Seq WaveNet](https://arxiv.org/abs/1803.04037) \[2\] | 0.5129 |

1. [Corporación Favorita. Corporación favorita grocery sales
    forecasting. Kaggle Competition Leaderboard,
    2018.](https://www.kaggle.com/c/favorita-grocery-sales-forecasting/leaderboard)
2. [Glib Kechyn, Lucius Yu, Yangguang Zang, and Svyatoslav Kechyn.
    Sales forecasting using wavenet within the framework of the Favorita
    Kaggle competition. Computing Research Repository, abs/1803.04037,
    2018](https://arxiv.org/abs/1803.04037).

## Auxiliary Functions

This auxiliary functions are used to efficiently create and wrangle
Favorita’s series.

## Numpy Wrangling

::: datasetsforecast.favorita.numpy_balance

::: datasetsforecast.favorita.numpy_ffill

::: datasetsforecast.favorita.numpy_bfill

::: datasetsforecast.favorita.one_hot_encoding

::: datasetsforecast.favorita.nested_one_hot_encoding

::: datasetsforecast.favorita.get_levels_from_S_df

::: datasetsforecast.favorita.distance_to_holiday

::: datasetsforecast.favorita.make_holidays_distance_df

::: datasetsforecast.favorita.CodeTimer
    handler: python
    options:
      show_if_no_docstring: false

::: datasetsforecast.favorita.Favorita200
    handler: python
    options:
      show_if_no_docstring: false

::: datasetsforecast.favorita.Favorita500
    handler: python
    options:
      show_if_no_docstring: false

::: datasetsforecast.favorita.FavoritaComplete
    handler: python
    options:
      show_if_no_docstring: false

::: datasetsforecast.favorita.FavoritaRawData
    handler: python
    options:
      show_if_no_docstring: false

::: datasetsforecast.favorita.unzip
