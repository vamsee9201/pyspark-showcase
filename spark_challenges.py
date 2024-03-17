#%%
import pyspark.pandas as ps
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
#%%
spark = SparkSession.builder.getOrCreate()
#%%
diabetesPrediction = spark.read.csv('diabetes_prediction_dataset.csv', header=True)
diabetesPrediction.show()
#%%
#basic data exploration
diabetesPrediction.printSchema()
# %%
diabetesPrediction.describe().show()
# %%
diabetesPrediction.summary("count", "min", "25%", "75%", "max").show()
# %%
diabetesPrediction.show()
# %%
genderAvgAge = diabetesPrediction.groupBy(diabetesPrediction.gender).agg({'age':'mean'}).collect()
genderAvgAge
# %%
diabetesPrediction.count()
# %%
len(diabetesPrediction.columns)
# %%
diabetesPrediction.select('gender').distinct().show()
# %%
diabetesPrediction.count()
# %%
diabetesPrediction.dropDuplicates().collect()
# %%
diabetesPrediction.groupBy('gender').count().show()
# %%
diabetesPrediction.filter(col("age").isNull()).count().show()
# %%
