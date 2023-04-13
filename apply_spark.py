from pyspark.sql import SparkSession

Spark = SparkSession.builder.appName('SparkPractice').getOrCreate()

column = ["seq_No", "Names"]

data = [("1", "john Player john Player john Player john Player"),
        ("2", "spark Player"),
        ("3", "apply Player"),
        ("4", "team Player"),
        ("5", "music Player"),
        ("6", "video Player"),
        ]
df = Spark.createDataFrame(data=data,schema=column)

df.show(truncate=False) 



# Apply function using withColumn
from pyspark.sql.functions import upper
df.withColumn("Upper_Name", upper(df.Names)).show() 


# Apply function using select  
df.select("seq_No","Names", upper(df.Names)).show() 





