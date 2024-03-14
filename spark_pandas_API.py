#%%
import pandas as pd
import numpy as np
import pyspark.pandas as ps
from pyspark.sql import SparkSession
#%%
s = ps.Series([1, 3, 5, np.nan, 6, 8])
s

# %%
