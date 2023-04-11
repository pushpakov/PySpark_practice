from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from pyspark.sql.functions import lit
colObj = lit("Spark_practice_app")   


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
dept = [('finance',10),('marketing',55),('sales',455),('quantity',4545)]
deptCol = ['dept_name','dept_id']
rdd = spark.sparkContext.parallelize(dept)
df2 = rdd.toDF()

# adding custom header for column 
df3 = rdd.toDF(deptCol)
# df2.printSchema()
# df2.show() 
# df3.show() 




# dataframe to pandas 
# pandasDf = df3.toPandas()
# print(pandasDf) 


#Display full column contents
# df3.show(truncate=False)


# Display 2 rows and full column contents
# df3.show(2,truncate=False) 

# Display 2 rows & column values 25 characters
# df3.show(2,truncate=10) 

# Display DataFrame rows & columns vertically
# df3.show(n=3,truncate=25,vertical=True)

# syntax for show function 
# def show(self, n=20, truncate=True, vertical=False):


# nested structtype object struct 
structureData = [
    (("James","Jony","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("James","Jony","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("James","Jony","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("James","Jony","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("James","Jony","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("James","Jony","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("James","Jony","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("James","Jony","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]
structureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])

df4 = spark.createDataFrame(data=structureData,schema=structureSchema)
# df4.printSchema()


# this will print all the values for the column 
df4.show(n=df4.count(),truncate=False)  

# converting to json 
# print(df4.schema.json())













