#%%
import pyspark.pandas as ps
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
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
type(spark.sql('SELECT * FROM {table1}',table1=diabetesPrediction))
# %%
spark.sql("SELECT {table1}.age FROM {table1}",table1=diabetesPrediction).show()
# %%
spark.sql("SELECT t1.age FROM {table1} t1",table1=diabetesPrediction).show()
# %%
ageFilter = 25
spark.sql("SELECT t1.age FROM {table1} t1 WHERE t1.age > {ageFilter}",table1=diabetesPrediction,ageFilter=ageFilter).show()
# %%
spark.sql("SELECT t1.age FROM {table1} t1 WHERE t1.age > :ageFilter",{"ageFilter":5},table1=diabetesPrediction).show()
# %%
spark.sql("SELECT t1.age FROM {table1} t1 WHERE t1.age > ? and ? < t1.age",args=[25,70],table1=diabetesPrediction).show()
# %%
diabetesPrediction.groupBy("gender").pivot("smoking_history").agg(avg("bmi")).fillna(0).show()
# %%
diabetesPrediction.groupBy("gender").pivot("smoking_history",["No Info","never"]).agg(avg("bmi")).fillna(0).show()
# %%
diabetesPrediction.show()
# %%
diabetesPrediction.groupBy("smoking_history").agg(count("*").alias("count")).show()
"""
smoking_history = diabetesPrediction.groupBy("smoking_history").count()
smoking_history
"""
# %%
type(diabetesPrediction)

# %%
#conversions of spark dataframe to pandas needs an intermediary like pandas-on-spark dataframe

#bar plot
genders = diabetesPrediction.groupby("gender").count()
gendersBarPlot = genders.pandas_api()
gendersBarPlot.plot.bar(x='gender', y='count') 


# %%
#pyspark and streaming data
#MLLIB here