from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()



# implementing bradcast on rdd 
states = {"NY": "New York", "CA": "California", "FL": "Florida"}
broadcastStates = spark.sparkContext.broadcast(states)

data = [("James", "Smith", "USA", "CA"),
        ("Michael", "Rose", "USA", "NY"),
        ("Robert", "Williams", "USA", "CA"),
        ("Maria", "Jones", "USA", "FL")
        ]

rdd = spark.sparkContext.parallelize(data)


def state_convert(code):
    return broadcastStates.value[code]


result = rdd.map(lambda x: (x[0], x[1], x[2], state_convert(x[3]))).collect()
print(result)


# implementing broadcast on dataframe
columns = ["fname","lname","country","state"]
df = spark.createDataFrame(data=data,schema=columns)

df.show()

result1 = df.rdd.map(lambda x: (x[0], x[1], x[2], state_convert(x[3]))).toDF(columns) 
result1.show()   




