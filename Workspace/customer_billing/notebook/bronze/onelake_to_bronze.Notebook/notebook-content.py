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

# **This notebook is used to load a file data as it is into the table**

# CELL ********************

%run "./common_functions"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#variable declarion
# yaml_path = "/lakehouse/default/Files/config/yaml/bronze/customers.yaml"
# summary_file_path = "Files/unzip/20261011/summary_file.txt"
run_start_time = datetime.now()
# pipeline_name = "test_pipeline_1"
# task_name = "test_task_1"
# run_id = mssparkutils.env.getJobId()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# yaml_path = "/lakehouse/default/Files/config/yaml/bronze/"
final_unzip_path = unzip_folder.replace("/lakehouse/default/", "")
file_name_prefix = "summary"
summary_file_name, summary_file_path = read_recursive_file_data(final_unzip_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#commented by AW
# def bronze_data_load(yaml_path):
#     data_load_message = ""

#     #Variable declarion by reading yaml file
#     reading_yaml_data = open_file_read_data(yaml_path)
#     ytable_name = reading_yaml_data["table"]["name"]
#     ytable_schema_name = reading_yaml_data["table"]["schema_name"]
#     final_table_name = ytable_schema_name+"."+ytable_name
#     ytable_source_file = reading_yaml_data["table"]["source_file"]
#     ytable_columns_names = reading_yaml_data["table"]["columns"]
#     global file_name_prefix, file_name_pattern
#     file_name_prefix = ytable_source_file.split("*")[0]
#     file_name_pattern = rf"^{file_name_prefix}_(\d{{8}})_(\d{{6}})\.csv$"

#     #Recursive search
#     final_file_name, final_file_path = read_recursive_file_data(final_unzip_path)
#     print("final_file_name => ",final_file_name)
#     print("final_file_path => ",final_file_path)
    
#     #File name validation
#     file_name_validation = match_file_name_pattern(file_name_pattern, final_file_name)
#     print("file_name_validation => ", file_name_validation)

#     #Data load process
#     df_summary = spark.read.format('csv').option("header", True).load(summary_file_path)
#     df_column = df_summary.columns
#     df_summary = df_summary.toDF(*[dataframe_col.strip() for dataframe_col in df_column])
#     final_file_name_without_ext = final_file_name.replace(".csv","")
#     summary_count = df_summary.filter(col("file_name").contains(final_file_name_without_ext)).select("count").first()[0]
#     df_final_file_name = spark.read.format('csv').option("header", True).load(final_file_path)
#     table_count = df_final_file_name.count()
#     data_load_message = "Data load successfully"
#     if table_count == (int(summary_count)-1):
#         df_final_file_name.write.format("delta").mode("overwrite").saveAsTable(final_table_name)
#         return data_load_message
#     else:
#         data_load_message = "Summary count is not matched hence process stopped"
#         raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# #calling unzip function with email notification-original commented by AW
# all_files = mssparkutils.fs.ls("Files/config/yaml/bronze/")
# for actual_file_path in all_files:
#     run_id = mssparkutils.env.getJobId()+f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
#     absolute_path = yaml_path + actual_file_path.name
#     print("absolute_path => ", absolute_path)
#     table_name = actual_file_path.name.split(".")[0]
#     try:
#         process_log("Start", table_name=table_name, layer="bronze")
#         spark.sql("""SELECT * FROM control.etl_run_log ORDER BY run_end_time DESC LIMIT 1""").show()
#         data_load_message = bronze_data_load(absolute_path)
#         if "Error : " in data_load_message:
#             raise Exception(data_load_message)
#         print(f"validation message after execution : {data_load_message}")

#         #To get total record inserted
#         latest_history = (spark.sql(f"DESCRIBE HISTORY bronze.{table_name}").select("operationMetrics").first())

#         if latest_history:
#             op_metrics = latest_history["operationMetrics"]
#             total_rows = int(op_metrics.get("numOutputRows", 0))
#         else:
#             total_rows = 0
#         print("total_inserted_count =>",total_rows)    
#         process_log("Success",total_inserted_count = total_rows)
#         spark.sql("""SELECT * FROM control.etl_run_log ORDER BY run_end_time DESC LIMIT 1""").show()

#     except Exception as e:
#         process_log("Failed",  table_name = table_name, layer = "bronze", error_message = f"{e}")
#         send_mail_fn(f'{pipeline_name}-{task_name}-{table_name}-bronze layer is failed',f'{e}')
#         raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#AW
# Calling unzip function with email notification
all_files = mssparkutils.fs.ls(bronze_all_files)
for actual_file_path in all_files:
    run_id = mssparkutils.env.getJobId() + f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    absolute_path = bronze_yaml_path + actual_file_path.name
    table_name = actual_file_path.name.split(".")[0]
    try:
        # Read YAML configuration
        reading_yaml_data = open_file_read_data(absolute_path)
        ytable_name = reading_yaml_data["table"]["name"]
        ytable_schema_name = reading_yaml_data["table"]["schema_name"]
        final_table_name = ytable_schema_name + "." + ytable_name
        ytable_source_file = reading_yaml_data["table"]["source_file"]
        ytable_columns_names = reading_yaml_data["table"]["columns"]

        # File name pattern
        file_name_prefix = ytable_source_file.split("*")[0]
        file_name_pattern = rf"^{file_name_prefix}_(\d{{8}})_(\d{{6}})\.csv$"

        # Recursive search
        final_file_name, final_file_path = read_recursive_file_data(final_unzip_path)
        print("final_file_name =>", final_file_name)
        print("final_file_path =>", final_file_path)

        # START LOG: CSV filename is now available
        process_log("Start", table_name=table_name, layer="bronze", file_name=final_file_name)

        # File name validation
        file_name_validation = match_file_name_pattern(file_name_pattern, final_file_name)
        print("file_name_validation =>", file_name_validation)

        # Data load process
        df_summary = (spark.read.format("csv").option("header", True).load(summary_file_path))
        df_column = df_summary.columns
        df_summary = df_summary.toDF(*[c.strip() for c in df_column])
        final_file_name_without_ext = final_file_name.replace(".csv", "")
        summary_count = (df_summary.filter(col("file_name").contains(final_file_name_without_ext)).select("count").first()[0])
        df_final_file_name = (spark.read.format("csv").option("header", True).load(final_file_path))
        table_count = df_final_file_name.count()
        if table_count == (int(summary_count) - 1):
            df_final_file_name.write.format("delta").mode("overwrite").saveAsTable(final_table_name)
            data_load_message = "Data load successfully"
            print("data_load_message => ", data_load_message)
            # Get total records inserted
            latest_history = (spark.sql(f"DESCRIBE HISTORY bronze.{table_name}").select("operationMetrics").first())
            if latest_history:
                op_metrics = latest_history["operationMetrics"]
                total_rows = int(op_metrics.get("numOutputRows", 0))
            else:
                total_rows = 0
            print("total_inserted_count =>", total_rows)
            process_log("Success", table_name=table_name, layer="bronze", total_inserted_count=total_rows, file_name=final_file_name)
        else:
            data_load_message = "Summary count is not matched hence process stopped"
            print("data_load_message => ", data_load_message)
            raise
        spark.sql("""SELECT * FROM control.etl_run_log ORDER BY run_end_time DESC LIMIT 1""").show()
    except Exception as e:
        process_log("Failed", table_name=table_name, layer="bronze", error_message=str(e))
        send_mail_fn(f"{pipeline_name}-{task_name}-{table_name}- Bronze layer is failed", str(e))
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
