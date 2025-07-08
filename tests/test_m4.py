%load_ext autoreload
%autoreload 2
group = 'Hourly'
await M4.async_download('data', group=group)
df, *_ = M4.load(directory='data', group=group)
n_series = len(np.unique(df.unique_id.values))
display_str  = f'Group: {group} '
display_str += f'n_series: {n_series}'
print(display_str)
from fastcore.test import test_close
esrnn_url = 'https://github.com/Nixtla/m4-forecasts/raw/master/forecasts/submission-118.zip'
esrnn_evaluation = M4Evaluation.evaluate('data', 'Hourly', esrnn_url)
# Test of the same evaluation as the original one
test_close(esrnn_evaluation['SMAPE'].item(), 9.328, eps=1e-3)
test_close(esrnn_evaluation['MASE'].item(), 0.893, eps=1e-3)
test_close(esrnn_evaluation['OWA'].item(), 0.440, eps=1e-3)
esrnn_evaluation
fforma_url = 'https://github.com/Nixtla/m4-forecasts/raw/master/forecasts/submission-245.zip'
fforma_forecasts = M4Evaluation.load_benchmark('data', 'Hourly', fforma_url)
fforma_evaluation = M4Evaluation.evaluate('data', 'Hourly', fforma_forecasts)
# Test of the same evaluation as the original one
test_close(fforma_evaluation['SMAPE'].item(), 11.506, eps=1e-3)
test_close(fforma_evaluation['MASE'].item(), 0.819, eps=1e-3)
test_close(fforma_evaluation['OWA'].item(), 0.484, eps=1e-3)
fforma_evaluation
