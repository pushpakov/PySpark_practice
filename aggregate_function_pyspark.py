from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("aggregation_function").getOrCreate()

simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
schema = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema = schema)
# df.printSchema()
# df.show(truncate=False)


# to find the distinct salary 
print(" approx_count_distinct : " + str(df.select(approx_count_distinct("salary")).collect()[0][0])) 

# to find the average salary 
print("avg : " + str(df.select(avg("salary")).collect()[0][0]))
 
# collect_list to return all the duplicate values from the input columns 
print("collected list : ")
df.select(collect_list("salary")).show(truncate=False)


print("collected set with all the duplicate value eliminated : ")
df.select(collect_set("salary")).show(truncate=False)


# to return the number of distinct element in a column 
df2 = df.select(countDistinct("department", "salary"))
df2.show(truncate=False)
print("Distinct Count of Department & Salary: "+str(df2.collect()[0][0])) 


# to return the number of element in column 
print("count: "+str(df.select(count("salary")).collect()[0][0]))


# to return the first element of the column 
df.select(first("salary")).show(truncate=False)

# to return the last element of the column 
df.select(last("salary")).show(truncate=False)



df.select(kurtosis("salary")).show(truncate=False)

df.select(max("salary")).show(truncate=False)

df.select(min("salary")).show(truncate=False)

df.select(mean("salary")).show(truncate=False)

df.select(skewness("salary")).show(truncate=False)

df.select(stddev("salary"), stddev_samp("salary"), stddev_pop("salary")).show(truncate=False)

df.select(sum("salary")).show(truncate=False)

df.select(sumDistinct("salary")).show(truncate=False)

df.select(variance("salary"),var_samp("salary"),var_pop("salary")).show(truncate=False)


