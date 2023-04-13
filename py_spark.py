from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit


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
df = spark.createDataFrame(emptyRDD, schema)

# df.printSchema()

# converting to df using toDF
df1 = emptyRDD.toDF(schema)
# df1.printSchema()
# df1.show()

# passing python list to RDD
dept = [('finance', 10), ('marketing', 55), ('sales', 455), ('quantity', 4545)]
deptCol = ['dept_name', 'dept_id']
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


# Display full column contents
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
    (("James", "Jony", "Smith"), "36636", "M", 3100),
    (("Michael", "Rose", ""), "40288", "M", 4300),
    (("Robert", "", "Williams"), "42114", "M", 1400),
    (("Maria", "Anne", "Jones"), "39192", "F", 5500),
    (("James", "Jony", "Smith"), "36636", "M", 3100),
    (("Michael", "Rose", ""), "40288", "M", 4300),
    (("Robert", "", "Williams"), "42114", "M", 1400),
    (("Maria", "Anne", "Jones"), "39192", "F", 5500),
    (("James", "Jony", "Smith"), "36636", "M", 3100),
    (("Michael", "Rose", ""), "40288", "M", 4300),
    (("Robert", "", "Williams"), "42114", "M", 1400),
    (("Maria", "Anne", "Jones"), "39192", "F", 5500),
    (("James", "Jony", "Smith"), "36636", "M", 3100),
    (("Michael", "Rose", ""), "40288", "M", 4300),
    (("Robert", "", "Williams"), "42114", "M", 1400),
    (("Maria", "Anne", "Jones"), "39192", "F", 5500),
    (("James", "Jony", "Smith"), "36636", "M", 3100),
    (("Michael", "Rose", ""), "40288", "M", 4300),
    (("Robert", "", "Williams"), "42114", "M", 1400),
    (("Maria", "Anne", "Jones"), "39192", "F", 5500),
    (("James", "Jony", "Smith"), "36636", "M", 3100),
    (("Michael", "Rose", ""), "40288", "M", 4300),
    (("Robert", "", "Williams"), "42114", "M", 1400),
    (("Maria", "Anne", "Jones"), "39192", "F", 5500),
    (("James", "Jony", "Smith"), "36636", "M", 3100),
    (("Michael", "Rose", ""), "40288", "M", 4300),
    (("Robert", "", "Williams"), "42114", "M", 1400),
    (("Maria", "Anne", "Jones"), "39192", "F", 5500),
    (("James", "Jony", "Smith"), "36636", "M", 3100),
    (("Michael", "Rose", ""), "40288", "M", 4300),
    (("Robert", "", "Williams"), "42114", "M", 1400),
    (("Maria", "Anne", "Jones"), "39192", "F", 5500),
    (("Jen", "Mary", "Brown"), "", "F", -1)
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

df4 = spark.createDataFrame(data=structureData, schema=structureSchema)
# df4.printSchema()


# this will print all the values for the column
# df4.show(n=df4.count(),truncate=False)

# converting to json
# print(df4.schema.json())


# creating new column using lit and col
lit_fun = df4.select(col("id"), lit("22").alias("business_id"))
# lit_fun.show()

# df4.show(n=df4.count(),truncate=False)


# selecting column from dataframe

# selecting single or multiple column from pyspark
# df4.select("name","id").show()

# selecting all the column from the list
# df4.select(df4['*']).show()
# df4.select('*').show()


# select column by index
# df4.select(df4.columns[:3]).show(3)  # this will show first 3 column and first 3 rows


# Selects columns 1 to 3  and top 3 rows
# df4.select(df4.columns[1:3]).show(3)

# this will show name with first 3 rows
# df4.select('name').show(3)

# to select the firstname and lastname from the name column
# df4.select("name.firstname","name.lastname").show(truncate=False)


# to get all the column from the struct column
# df4.select('name.*').show(truncate=False)


# pyspark collect function to retrieve the data from dataframe

# collect to retrieve data from df4
# dataCollect = df4.collect()
# print(dataCollect)

# for row in dataCollect:
#     name = row['name']['firstname'] + ' ' + row['name']['middlename'] + ' ' + row['name']['lastname']
#     id = row['id']
#     gender = row['gender']
#     salary = row['salary']

#     print(f"Name: {name}, ID: {id}, Gender: {gender}, Salary: {salary}")


# change datatype using pyspark withColumn() and updating the existing value of column
# df4.withColumn("salary",col("salary").cast("String")*5).show()


# add a new column using withColumn()
# df4.withColumn("Country",lit("USA")).show()


# rename column name
# df4.withColumnRenamed("gender","sex").show()


# drop column from pyspark data
# df4.drop("salary").show()
