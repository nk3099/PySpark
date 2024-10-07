#1.remove_double_quotes_from_value_of_json_string.py
"""
example: {"id":"1","name":"neeraj"kumar","city":"del"} 
i.e. to remove " in "neeraj kumar"

Split function in python:
s1="a|b|c"
print(s1.split('|'))
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col,lit,concat_ws,concat

spark=SparkSession.builder.getOrCreate()

jsonString="""{"id":"1","name":"neeraj"kumar","city":"del"}"""
print(jsonString)


data=[(1,'{"id":"1","name":"nitin","city":"blr"}'),(2,jsonString)]
cols=["col1","col2"]

df=spark.createDataFrame(data,cols)
display(df)

split_df = df.withColumn("col3",split(col('col2'),'"name":"')[0]) \
.withColumn("col4",lit('"name":"')) \
.withColumn("col5",split(col('col2'),'"name":"')[1])

display(split_df)

split_two_parts_df = split_df.withColumn("col6",split(col("col5"),'"',2))

display(split_two_parts_df)

#Spark SQL functions lit() used to add a new column to DataFrame by assigning a literal or constant value. This function return Column type as return type. 

split_concat_df = split_two_parts_df.withColumn("col7",concat_ws(' ',col("col6")))
display(split_concat_df)

merged_df = split_concat_df.withColumn("col8",concat(col("col3"),col("col4"),col("col7")))
display(merged_df)

final_df=merged_df.select(col("col2"),col("col8"))
display(final_df)

final_df.show(truncate=False)

final_df.collect()

