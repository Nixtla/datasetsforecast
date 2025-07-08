from nbdev import *
from fastcore.test import test_close
%load_ext autoreload
%autoreload 2
Y_df, X_df, S_df = M5.load('./data')
n_series = 30_490
assert Y_df['unique_id'].unique().size == n_series
assert X_df['unique_id'].unique().size == n_series
assert S_df.shape[0] == 30_490
Y_df.head()
X_df.head()
S_df.head()
m5_winner_url = 'https://github.com/Nixtla/m5-forecasts/raw/main/forecasts/0001 YJ_STU.zip'
winner_evaluation = M5Evaluation.evaluate('data', m5_winner_url)
# Test of the same evaluation as the original one
test_close(winner_evaluation.loc['Total'].item(), 0.520, eps=1e-3)
winner_evaluation
m5_second_place_url = 'https://github.com/Nixtla/m5-forecasts/raw/main/forecasts/0002 Matthias.zip'
m5_second_place_forecasts = M5Evaluation.load_benchmark('data', m5_second_place_url)
second_place_evaluation = M5Evaluation.evaluate('data', m5_second_place_forecasts)
# Test of the same evaluation as the original one
test_close(second_place_evaluation.loc['Total'].item(), 0.528, eps=1e-3)
second_place_evaluation
winner_benchmark = M5Evaluation.load_benchmark('data')
winner_evaluation = M5Evaluation.evaluate('data', winner_benchmark)
# Test of the same evaluation as the original one
test_close(winner_evaluation.loc['Total'].item(), 0.520, eps=1e-3)
winner_evaluation
winner_benchmark_val = M5Evaluation.load_benchmark('data', validation=True)
winner_evaluation_val = M5Evaluation.evaluate('data', winner_benchmark_val, validation=True)
winner_evaluation_val
