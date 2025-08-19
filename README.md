# datasetsforecast

> Datasets for time series forecasting

## Install

``` sh
pip install datasetsforecast
```

## Datasets

- [Favorita](https://nixtlaverse.nixtla.io/datasetsforecast/favorita)
- [Hierarchical](https://nixtlaverse.nixtla.io/datasetsforecast/hierarchical)
- [Longhorizon](https://nixtlaverse.nixtla.io/datasetsforecast/long_horizon)
- [M3](https://nixtlaverse.nixtla.io/datasetsforecast/m3)
- [M4](https://nixtlaverse.nixtla.io/datasetsforecast/m4)
- [M5](https://nixtlaverse.nixtla.io/datasetsforecast/m5)
- [PHM2008](https://nixtlaverse.nixtla.io/datasetsforecast/phm2008)

## How to use

All the modules have a `load` method which you can use to load the
dataset for a specific group. If you don’t have the data locally it will
be downloaded for you.

``` python
from datasetsforecast.phm2008 import PHM2008
```

``` python
train_df, test_df = PHM2008.load(directory='data', group='FD001')
train_df.shape, test_df.shape
```

    ((20631, 17), (13096, 17))
