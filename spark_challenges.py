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
diabetesPrediction.filter(col("age").isNull()).count()
# %%
diabetesPrediction.withColumn('age2',diabetesPrediction.age**2).show()
# %%
diabetesPrediction.drop('age2').show()
# %%
diabetesPrediction.describe().show()
# %%
diabetesPrediction.filter( (diabetesPrediction.age > 25) & (diabetesPrediction.age<35)).show()
# %%
diabetesPrediction.where( (diabetesPrediction.age > 25) & (diabetesPrediction.age<35)).show()

# %%
df1 = spark.createDataFrame([("Alice", 1), ("Bob", 2)], ["name", "id"])
df2 = spark.createDataFrame([(3, "Charlie"), (4, "Dave")], ["id", "name"])
df1.union(df2).show()
# %%
spark.sql('SELECT * FROM {table1}',table1=diabetesPrediction).show()
# %%
spark.sql("SELECT {table1}.age FROM {table1}",table1=diabetesPrediction).show()
# %%
