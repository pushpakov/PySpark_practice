# # Importing pySpark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import expr

# # Create SparkSession
# spark = SparkSession.builder.master("local[4]").appName("learnSpark").getOrCreate()


# # Create spark session with local[5]
# rdd = spark.sparkContext.parallelize(range(0,20))
# print("From local[5] : "+str(rdd.getNumPartitions()))

# rdd.write.mode('overwrite').csv('c:tmp/partition.csv')
 
# # Use parallelize with 6 partitions
# rdd1 = spark.sparkContext.parallelize(range(0,25), 6)
# print("parallelize : "+str(rdd1.getNumPartitions()))

# rddFromFile = spark.sparkContext.textFile("/home/allbanero/Downloads/test.txt",10)
# print("TextFile : "+str(rddFromFile.getNumPartitions()))     


# # rdd1.saveAsTextFile("/tmp/partition") 



# # Using repartition
# rdd2 = rdd1.repartition(4)
# print("Repartition size : "+str(rdd2.getNumPartitions()))

# # rdd2.saveAsTextFile("/tmp/re-partition")


# # Using coalesce()
# rdd3 = rdd1.coalesce(4)
# print("Repartition size : "+str(rdd3.getNumPartitions()))

# rdd3.saveAsTextFile("/tmp/coalesce")



from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com') \
        .master("local[5]").getOrCreate()

df=spark.range(0,20)
print(df.rdd.getNumPartitions())

df.write.mode("overwrite").csv("/tmp/partition.csv")

