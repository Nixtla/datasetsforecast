from datasetsforecast.phm2008 import PHM2008
train_df, test_df = PHM2008.load(directory='data', group='FD001')
train_df.shape, test_df.shape
