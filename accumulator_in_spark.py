from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('accumulator_learning').getOrCreate()

accum = spark.sparkContext.accumulator(2)

rdd = spark.sparkContext.parallelize([1,2,3,4,5,6,7,8,9,0]).toDF().show()

rdd.foreach(lambda x: accum.add(x)) 

print(rdd)
print(accum)
print(accum.value) 


accuSum=spark.sparkContext.accumulator(10)
def countFun(x):
    global accuSum
    accuSum+=x


rdd.foreach(countFun)
print(accuSum.value)


