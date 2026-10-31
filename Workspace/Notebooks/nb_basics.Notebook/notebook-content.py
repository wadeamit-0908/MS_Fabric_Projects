# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d74aa5e6-a4d3-476e-bc88-08317a2930f4",
# META       "default_lakehouse_name": "lk_vdp",
# META       "default_lakehouse_workspace_id": "11047665-e8ec-4669-81d9-d5567582b37a",
# META       "known_lakehouses": [
# META         {
# META           "id": "d74aa5e6-a4d3-476e-bc88-08317a2930f4"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# **Pyspark Basics**

# CELL ********************

#library
from pyspark.sql.functions import col
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#create dataframe
emp_data = [
    (1, "Amit", "Pune", "2026-09-26 11:12:20"),
    (2, "Ganesh", "Mumbai", "2026-09-26 11:12:21"),
    (3, "Vipul", "Goa", "2026-09-26 11:12:22"),
    (3, "Vipul", "Goa", "2026-09-26 11:12:23"),
    (3, "Vipul", "Janmu", "2026-09-26 11:12:24")
]

#create dataframe from above data
df_emp = spark.createDataFrame(emp_data, ["id", "names", "city", "created_date"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display(df_emp)

# df_emp.printSchema()

# display(df_emp.select("names","city"))

# df_filter = df_emp.filter(df_emp.city == "Goa")
# display(df_filter)

# df_filter = df_emp.filter(F.col("city") == "Goa")
# display(df_filter)

# df_filter = df_emp.filter(F.col("city").isin("Goa","Pune"))
# display(df_filter)

# df_filter = df_emp.filter(
#     (F.col("city").isin("Goa","Pune")) &
#     (F.col("id") == 3)
#     )
# display(df_filter)

# df_filter = df_emp.filter(
#     (~F.col("city").isin("Goa","Pune"))
#     )
# display(df_filter)

# df_filter = df_emp.filter(
#     (F.col("city").like("Go%"))
#     )
# display(df_filter)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#add new column in existing dataframe
df_emp = df_emp.withColumn("country", F.lit("India"))
df_emp = df_emp.withColumnRenamed("country", "countries")
df_emp = df_emp.orderBy(F.desc("names"),F.asc("city"))
display(df_emp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#dataframe display with alias
display(df_emp.select(F.col("id").alias("serialNumber")))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#find a count of dataframe
# df_emp.count()
selectedColumnCount = df_emp.select(F.col("id")).distinct().count()
print("selectedColumnCount => ", selectedColumnCount)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

duplicatesRemove = df_emp.dropDuplicates()
display(duplicatesRemove)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#how to remove partial  using ranking function
# window_spect = F.partitionBy("id", "name").orderBy("created_date")
# df_rank = duplicatesRemove.withColumn("rn", F.row_number().over(window_spect))
# display(df_rank)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#how to remove partial  using ranking function
from pyspark.sql.window import Window
window_spect = Window.partitionBy("id", "names").orderBy(F.desc("created_date"))
df_rank = duplicatesRemove.withColumn("rn", F.row_number().over(window_spect)).filter(F.col("rn") == 1)
display(df_rank)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#aggregate functions
salary_data = [
    (1, "Amit", 1000, "IT"),
    (2, "Vipul", 2000, "IT"),
    (3, "Pratik", 3000, "HR")
]
salary_df = spark.createDataFrame(salary_data, ["id", "name", "salary", "department"])
display(salary_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

salary_df.agg(F.count("*").alias("employee_count"),
F.sum("salary").alias("total_salary")
).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

salary_df.groupBy("department").agg(F.count("*").alias("employee_count"),
F.sum("salary").alias("total_salary")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#joins
emp = [
    (1,"Ganesh"),
    (2,"Pratik"),
    (3,"Vipul")
]

department = [
    (1,"IT"),
    (2,"HR"),
    (4,"ADMIN")
]

df_emp = spark.createDataFrame(emp,["id","name"])

df_department = spark.createDataFrame(department,["id","department"])

display(df_emp)
display(df_department)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df_emp.join(df_department,"id","inner").show()
# df_emp.join(df_department,"id","left").show()
# df_emp.join(df_department,"id","right").show()
# df_emp.join(df_department,"id","outer").show()
# df_emp.join(df_department,"id","left_anti").show()
# df_department.join(df_emp,"id","anti").show()
# df_department.join(df_emp,"id","semi").show()
df_department.join(df_emp,how="cross").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#joins with multiple columns
emp = [
    (1,"Ganesh",101),
    (2,"Pratik",101),
    (3,"Vipul",104)
]

department = [
    (1,"IT",101),
    (2,"HR",102),
    (4,"ADMIN",104)
]

df_emp = spark.createDataFrame(emp,["emp_id","name","emp_dept_id"])

df_department = spark.createDataFrame(department,["employee_id","department","emp_department"])

display(df_emp)
display(df_department)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_emp.join(df_department, (df_emp.emp_id==df_department.employee_id)&
(df_emp.emp_dept_id==df_department.emp_department),"inner").select("emp_id","name","department").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# read all files from emp folder
%fs ls Files/emp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# read all files from emp folder
emp_df = spark.read.format("csv")\
.option("delimiter","|")\
.option("header",True)\
.option("inferSchema",True)\
.load("Files/emp")
display(emp_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Design custom schema
from pyspark.sql.types import StructField, IntegerType, StringType, StructType

custom_schema = StructType(
    [StructField("emp_id", IntegerType(), True),
    StructField("emp_name", StringType(), True),
    StructField("emp_city", StringType(), True)]
)

# read all files from emp folder
emp_df = spark.read.format("csv")\
.option("delimiter","|")\
.option("header",True)\
.option("inferSchema",True)\
.schema(custom_schema)\
.load("Files/emp")
display(emp_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# read files one by one from emp folder

# make a list of all files in one array
all_files = mssparkutils.fs.ls("Files/emp")
# print(all_files)
for f in all_files:
    # print(f.path)
    emp_df = spark.read.format("csv")\
    .option("delimiter","|")\
    .option("header",True)\
    .option("inferSchema",True)\
    .load(f.path)

    emp_df.write.mode("append").saveAsTable("emp_data")
display(emp_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM emp_data

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("emp_data").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
