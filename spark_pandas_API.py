#%%
import pandas as pd
import numpy as np
import pyspark.pandas as ps
from pyspark.sql import SparkSession
#%%
s = ps.Series([1, 3, 5, np.nan, 6, 8])
s

# %%
psdf = ps.DataFrame(
    {'a': [1, 2, 3, 4, 5, 6],
     'b': [100, 200, 300, 400, 500, 600],
     'c': ["one", "two", "three", "four", "five", "six"]},
    index=[10, 20, 30, 40, 50, 60])

psdf
# %%
dates = pd.date_range('20130101', periods=6)
dates
# %%
pdf = pd.DataFrame(np.random.randn(6, 4), index=dates, columns=list('ABCD'))
pdf
# %%
psdf = ps.from_pandas(pdf)
type(psdf)
psdf
# %%
psdf.dtypes

# %%
psdf.head()
# %%
psdf.index
# %%
psdf.columns

# %%
psdf.to_numpy()
# %%
psdf.describe()
# %%
psdf.T
# %%
psdf.sort_index(ascending=False)
# %%
psdf.sort_values(by='B')