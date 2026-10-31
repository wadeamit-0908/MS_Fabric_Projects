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

# **Executing this notebook to check zip file is good and extracting to right folder**

# CELL ********************

%run "./common_functions"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#declaring variables
folder_name = datetime.now().strftime("%Y%m%d")
# print(folder_name)
run_start_time = datetime.now()
# pipeline_name = "test_pipeline_1"
# task_name = "test_task_1"
run_id = mssparkutils.env.getJobId() + f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
# file_name = "client_input_multi_empty_file_03102026_014005.zip"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_input_folder = input_folder+file_name

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#calling unzip function with email notification
try:
    process_log("Start",file_name=file_name)
    spark.sql("""SELECT * FROM control.etl_run_log ORDER BY run_end_time DESC LIMIT 1""").show()
    validation_message = unzip_files(final_input_folder, unzip_folder)
    if "Error : " in validation_message:
        raise Exception(validation_message)
    print(f"validation message after execution : {validation_message}")
    time.sleep(15)
    process_log("Success",file_name=file_name)
    spark.sql("""SELECT * FROM control.etl_run_log ORDER BY run_end_time DESC LIMIT 1""").show()
except Exception as e:
    process_log("Failed",file_name=file_name, error_message=f"{validation_message}")
    send_mail_fn(f'{pipeline_name}-{task_name}-{file_name} is failed',f'{validation_message}')
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
