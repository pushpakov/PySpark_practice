from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit


spark = SparkSession.builder.appName('Spark_practice_app').getOrCreate()


emp = [(1, "Smith", -1, "2018", "10", "M", 3000),
       (2, "Rose", 1, "2010", "20", "M", 4000),
       (3, "Williams", 1, "2010", "10", "M", 1000),
       (4, "Jones", 2, "2005", "10", "F", 2000),
       (5, "Brown", 2, "2010", "40", "", -1),
       (6, "Brown", 2, "2010", "50", "", -1)
       ]
empColumns = ["emp_id", "name", "superior_emp_id",
              "year_joined", "emp_dept_id", "gender", "salary"]

empDF = spark.createDataFrame(data=emp, schema=empColumns)
empDF.printSchema()
empDF.show(truncate=False)

dept = [("Finance", 10),
        ("Marketing", 20),
        ("Sales", 30),
        ("IT", 40)
        ]
deptColumns = ["dept_name", "dept_id"]
deptDF = spark.createDataFrame(data=dept, schema=deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)


# inner join dataframe
print("inner")
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "inner").show()

# outer join
print("outer")
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "outer").show()

# full join
print("full")
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "full").show()

# full outer
print("fullouter")
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "fullouter").show()


# left join
print("left")
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "left").show()


# right join
print("right")
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "right").show()


# left semi join (it is similar to inner join but it returns all columns from the left dataset and ignores all columns from the right dataset )

print("leftsemi")
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "leftsemi").show()


# left anti join (it does the exact opposite of the leftsemi, leftanti join returns only columns from the left dataset for non-matched records)

print("leftanti")
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "leftanti").show()


# self join to join dataframe to itself
print("self join") 
empDF.alias("emp1").join(empDF.alias("emp2"),
                         col("emp1.superior_emp_id") == col("emp2.emp_id"), "inner") \
    .select(col("emp1.emp_id"), col("emp1.name"),
            col("emp2.emp_id").alias("superior_emp_id"),
            col("emp2.name").alias("superior_emp_name")) \
    .show(truncate=False)
