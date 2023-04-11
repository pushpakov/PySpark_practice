# Importing pySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Create SparkSession
spark = SparkSession.builder.appName('drop_duplicate').getOrCreate()

# Prepare Data
data = [("James", "Sales", 3000),
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

# Create DataFrame
columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=data, schema=columns)

df.printSchema()
df.show(truncate=False)


# # get distinct rows by comparing all the columns 
# distinctDF = df.distinct()
# print("Total count : " +str(df.count())) 

# print("Distinct count : " +str(distinctDF.count())) 
# distinctDF.show() 


# distinct of selected multiple columns 
dropDisDF = df.dropDuplicates(["department","salary"])

print("Distinct count of department and salary :  "+ str(dropDisDF.count())) 
dropDisDF.show()   






