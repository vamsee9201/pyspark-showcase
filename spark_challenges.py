#%%
import pyspark.pandas as ps
from pyspark.sql import SparkSession
#%%
spark = SparkSession.builder.getOrCreate()
#%%
diabetesPrediction = spark.read.csv('diabetes_prediction_dataset.csv', header=True)
diabetesPrediction.show()
#%%
