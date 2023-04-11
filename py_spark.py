from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


spark = SparkSession.builder.appName('Spark_practice_app').getOrCreate()

# creating empty RDD using sparkContext
emptyRDD = spark.sparkContext.emptyRDD()
# print(emptyRDD) 


# creating schema 
schema = StructType([
    StructField('fname', StringType(), True),
    StructField('mname', StringType(), True),
    StructField('lname', StringType(), True)
]) 

# creating dataframe 
df = spark.createDataFrame(emptyRDD,schema)

# df.printSchema() 

# converting to df using toDF 
df1 = emptyRDD.toDF(schema)
# df1.printSchema()
# df1.show() 

# passing python list to RDD
dept = [('finance',10),('marketing',55),('sales',455)]
deptCol = ['dept_name','dept_id']
rdd = spark.sparkContext.parallelize(dept)
df2 = rdd.toDF()
df3 = rdd.toDF(deptCol)
# df2.printSchema()
# df2.show() 
df3.show() 









