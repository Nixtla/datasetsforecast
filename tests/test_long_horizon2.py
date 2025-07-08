#| eval: false
import matplotlib.pyplot as plt
from nbdev import *
%load_ext autoreload
%autoreload 2
#| eval: false
# Checking qualitatively that series are correctly normalized
group = 'ETTh1'
Y_df1 = LongHorizon2.load(directory='./data', group=group)
Y_df2 = LongHorizon2.load(directory='./data', group=group, normalize=False)

unique_id = 'OT'

plot_df1 = Y_df1[Y_df1.unique_id==unique_id]
plot_df2 = Y_df2[Y_df2.unique_id==unique_id]

fig, ax1 = plt.subplots()

ax2 = ax1.twinx()
#ax1.plot(plot_df1.y[-2000:], 'b-')
#ax1.plot((plot_df2.y[-2000:]-38)/10, 'g-')

ax1.plot(plot_df1.y[-2000:], 'b-')
ax2.plot(plot_df2.y[-2000:], 'g-')

plt.show()
# Unit testing for abscense of duplicate unique_id-ds
# Unit testing for correct number of series
# Unit testing for correct number of date stamps
for group, meta in LongHorizon2Info:
    Y_df = LongHorizon2.load(directory='data', group=group)
    unique_elements = Y_df.groupby(['unique_id', 'ds']).size()
    unique_ts = Y_df.groupby('unique_id').size()
    n_time = len(Y_df.ds.unique())

    assert (unique_elements != 1).sum() == 0, f'Duplicated records found: {group}'
    assert unique_ts.shape[0] == meta.n_ts, f'Number of time series not match: {group}'
    assert n_time == meta.n_time, f'Number of time observations not match: {group} {n_time} { meta.n_time}'

