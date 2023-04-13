from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import array_contains


spark = SparkSession.builder.appName('Spark_practice_app').getOrCreate()

# from py_spark import spark

# print("this is spark ------------", spark)
data = [
    (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
    (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
    (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
    (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
    (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
    (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
]

schema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('languages', ArrayType(StringType()), True),
    StructField('state', StringType(), True),
    StructField('gender', StringType(), True)
])

df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
df.show(truncate=False)


# # Using equals condition
# df.filter(df.state == "OH").show(truncate=False)


# # not equals condition
# df.filter(df.state != "OH").show(truncate=False)
# df.filter(~(df.state == "OH")).show(truncate=False)


# #Using SQL col() function
# df.filter(col("state") == "OH").show(truncate=False)


# #Using SQL Expression
# df.filter("gender == 'M'").show()
# #For not equal
# df.filter("gender != 'M'").show()
# df.filter("gender <> 'M'").show()


# # Filter multiple condition
# df.filter( (df.state  == "OH") & (df.gender  == "M") ).show(truncate=False)


# # filter value based on list values
# li = ['OH','CA','DE']
# df.filter(df.state.isin(li)).show()


# # filter based on start with , end with , contains

# # Using startswith
# df.filter(df.state.startswith("N")).show()

# #using endswith
# df.filter(df.state.endswith("H")).show()

# #contains
# df.filter(df.state.contains("H")).show()


# # like and rlike
# data2 = [(2,"Michael Rose"),(3,"Robert Williams"),
#      (4,"Rames Rose"),(5,"Rames rose")
#   ]
# df2 = spark.createDataFrame(data = data2, schema = ["id","name"])

# # like - SQL LIKE pattern
# df2.filter(df2.name.like("%rose%")).show()


# # rlike - SQL RLIKE pattern (LIKE with Regex)
# #This check case insensitive
# df2.filter(df2.name.rlike("(?i)^*rose$")).show()


# # filter on array column
# df.filter(array_contains(df.languages, "Java")).show()


# filter on nested struct columns
df.filter(df.name.lastname == "Williams").show()
