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

# **This notebook we use to create a variable from dev.config**

# CELL ********************

#import libraries
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#declaring variable
file_location = "/lakehouse/default/Files/config/dev_config.json"
folder_name = datetime.now().strftime("%Y%m%d")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#open json file
with open(file_location) as f:
    config_data = json.load(f)
#declaring variables
env = config_data["env"]
input_folder = config_data["input_folder"]
unzip_folder = config_data["unzip_folder"]
excepted_files = config_data["excepted_files"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
