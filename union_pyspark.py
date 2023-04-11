from pyspark.sql import SparkSession
from pyspark.sql.functions import col 

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = [("James", "Sales", "NY", 90000, 34, 10000),
              ("Michael", "Sales", "NY", 86000, 56, 20000),
              ("Robert", "Sales", "CA", 81000, 30, 23000),
              ("Maria", "Finance", "CA", 90000, 24, 23000)
              ]

columns = ["employee_name", "department", "state", "salary", "age", "bonus"]
df = spark.createDataFrame(data=simpleData, schema=columns)
df.printSchema()
print("first dataframe " + str(df.count())) 
df.show(truncate=False)

simpleData2 = [("James", "Sales", "NY", 90000, 34, 10000),
               ("Maria", "Finance", "CA", 90000, 24, 23000),
               ("Jen", "Finance", "NY", 79000, 53, 15000),
               ("Jeff", "Marketing", "CA", 80000, 25, 18000),
               ("Kumar", "Marketing", "NY", 91000, 50, 21000)
               ]
columns2 = ["employee_name", "department", "state", "salary", "age", "bonus"]

df2 = spark.createDataFrame(data=simpleData2, schema=columns2)

df2.printSchema()
print("second dataframe " + str(df2.count()))
df2.show(truncate=False) 

unionDF = df.union(df2)

print("union dataframe " + str(unionDF.count()))  
unionDF.show(truncate=False)

disDF = df.union(df2).distinct()
print("union and distinct dataframe " + str(disDF.count())) 
disDF.show(truncate=False)

# added filter with union all to filter all the employee above age 30 
unionAllDF = df.unionAll(df2).filter(col("age")>30) 
 
print("using union all dataframe " + str(unionAllDF.count()))
unionAllDF.show(truncate=False) 

# performing union by name 
byNameUnion = df.unionByName(df2)
print("union by name dataframe " + str(byNameUnion.count()))
byNameUnion.show(truncate=False) 




# union by name with different number of columns 
# Create DataFrames with different column names
df1 = spark.createDataFrame([[5, 2, 6]], ["col0", "col1", "col2"])
df2 = spark.createDataFrame([[6, 7, 3]], ["col1", "col2", "col3"])

# Using allowMissingColumns
df3 = df1.unionByName(df2, allowMissingColumns=True)
df3.printSchema
df3.show()

