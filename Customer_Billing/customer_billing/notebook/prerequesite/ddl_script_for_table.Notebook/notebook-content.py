# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "de0f46b7-bf19-48b7-a80c-9bfa8a6aaac3",
# META       "default_lakehouse_name": "lk_customer_billing",
# META       "default_lakehouse_workspace_id": "11047665-e8ec-4669-81d9-d5567582b37a",
# META       "known_lakehouses": [
# META         {
# META           "id": "de0f46b7-bf19-48b7-a80c-9bfa8a6aaac3"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# **This notebook is to create a table using system id**

# CELL ********************

#import libraries
import yaml

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#declaring varibales
# yaml_path = "/lakehouse/default/Files/config/yaml/control/etl_run_log.yaml"
yaml_path = yaml_base_path+yaml_file_name

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#read yaml file
with open(yaml_path, "r") as yaml_files:
    yaml_config = yaml.safe_load(yaml_files)
# print(yaml_config)
# print(yaml_config["table"])
table_name = yaml_config["table"]["name"]
table_schema_name = yaml_config["table"]["schema_name"]
table_columns = yaml_config["table"]["columns"]
table_column_name = ",\n ".join([f'{table_column["name"]} {table_column["data_type"]}' for table_column in table_columns])
# print(table_column_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create schema if it does not exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {table_schema_name}")

#Prepare CREATE TABLE statement
create_table_sql = f"""CREATE TABLE IF NOT EXISTS {table_schema_name}.{table_name} ({table_column_name}) USING DELTA"""

#Prepare complete table name
full_table_name = f"{table_schema_name}.{table_name}"

# Check whether table already exists
if spark.catalog.tableExists(full_table_name):
    print(f"Table already exists: {full_table_name}")
else:
    # Create table only when it does not exist
    spark.sql(create_table_sql)
    print(f"Table created successfully: {full_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# create_table_sql = f"""
# create table if not exists {table_schema_name}.{table_name}({table_column_name})
# """
# # print(create_table_sql)
# spark.sql(create_table_sql)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
