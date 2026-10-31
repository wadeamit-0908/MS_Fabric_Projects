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

# CELL ********************

%run "./common_functions"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

run_start_time = datetime.now()
yaml_path = "/lakehouse/default/Files/config/yaml/silver/"
dummy_date = "9999-01-01"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Reading yaml files from silver
all_files = mssparkutils.fs.ls("Files/config/yaml/silver/")
if not all_files:
    raise FileNotFoundError(f"No YAML files found in {yaml_path}")
for actual_file_path in all_files:
    run_id = mssparkutils.env.getJobId() + f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    absolute_path = yaml_path + actual_file_path.name
    table_name = actual_file_path.name.split(".")[0]
    try:
        # Read YAML configuration
        reading_yaml_data = open_file_read_data(absolute_path)
        ytable_name = reading_yaml_data["table"]["name"]
        ytable_schema_name = reading_yaml_data["table"]["schema_name"]
        ytable_source_schema_name = reading_yaml_data["table"]["source_schema_name"]
        final_source_table_name = ytable_source_schema_name + "." + ytable_name
        final_table_name = ytable_schema_name + "." + ytable_name
        # ytable_columns_names = reading_yaml_data["table"]["columns"]
        ytable_columns_names = [col["name"] for col in reading_yaml_data["table"]["columns"]]
        ytable_business_key = reading_yaml_data["table"]["business_key"]
        ytable_hash_key_columns = reading_yaml_data["table"]["hash_key"]["columns"]
        
        # START LOG
        process_log("Start", table_name=table_name, layer="silver")
        metadata_columns = ["hash_key", "inserted_date", "updated_date", "is_latest"]
        data_columns = [col for col in ytable_columns_names if col not in metadata_columns]
        #validation for business key
        if not ytable_business_key:
            print(f"No businees key in {final_table_name}")
            raise
        #validation for hash key columns
        if not ytable_hash_key_columns:
            print(f"No hash key columns {final_table_name}")
            raise
        #Check source and target tables
        if not spark.catalog.tableExists(final_source_table_name):
            print(f"Source table not found: {final_source_table_name}")
            raise
        if not spark.catalog.tableExists(final_table_name):
            print(f"Target table not found: {final_table_name}")
            raise
        #Read Bronze data and generate hash
        df_bronze = spark.table(final_source_table_name).select(*data_columns)
        if df_bronze.groupBy(*ytable_business_key).count().filter( F.col("count") > 1).limit(1).count():
            print(f"Duplicate business keys found in Bronze - {final_table_name}")
            raise
        temp_hash_bronze = generate_hash_key(df_bronze)
        #Read current Silver records
        df_silver = spark.table(final_table_name).filter(F.col("is_latest") == 1).select(*(data_columns + ["hash_key"]))
        if df_silver.groupBy(*ytable_business_key).count().filter( F.col("count") > 1).limit(1).count():
            print(f"Duplicate business keys found in Silver - {final_table_name}")
            raise
        #Compare Bronze with current Silver
        b = temp_hash_bronze.alias("b")
        s = df_silver.withColumn("_silver_match", F.lit(1)).alias("s")
        join_condition = [
            F.col(f"b.`{key}`") == F.col(f"s.`{key}`")
            for key in ytable_business_key
        ]
        join_condition = join_condition[0]
        for key in ytable_business_key[1:]:
            join_condition = join_condition & (F.col(f"b.`{key}`") == F.col(f"s.`{key}`"))
        comparison = b.join(s, join_condition, "left")
        is_new = F.col("s._silver_match").isNull()
        is_changed = (
            F.col("s._silver_match").isNotNull()
            & ~F.col("b.hash_key").eqNullSafe(F.col("s.hash_key"))
        )
        new_records = comparison.filter(is_new).select("b.*")
        changed_records = comparison.filter(is_changed).select("b.*")
        new_count = new_records.count()
        changed_count = changed_records.count()
        print(f"New records: {new_count}")
        print(f"Changed records: {changed_count}")
        #Prepare new current versions
        new_versions = (
            new_records.unionByName(changed_records)
            .withColumn("inserted_date", F.current_date())
            .withColumn("updated_date",F.to_date(F.lit(dummy_date)))
            .withColumn("is_latest", F.lit(1))
            .select(*ytable_columns_names)
        )
        #Prepare rows to expire and rows to insert
        expire_records = changed_records.select(*ytable_business_key).withColumn("_scd_action", F.lit("EXPIRE"))
        insert_records = new_versions.withColumn("_scd_action", F.lit("INSERT"))
        merge_records = expire_records.unionByName(insert_records, allowMissingColumns=True)
        #Expire old versions and insert new versions
        if new_count + changed_count > 0:
            merge_condition = " AND ".join(
                [f"t.`{key}` <=> s.`{key}`" for key in ytable_business_key] + ["t.`is_latest` = 1", "s.`_scd_action` = 'EXPIRE'"]
            )
            insert_values = {
                c: f"s.`{c}`" for c in ytable_columns_names
            }
            (DeltaTable.forName(spark, final_table_name).alias("t").merge(merge_records.alias("s"), merge_condition).whenMatchedUpdate(
                    condition="s.`_scd_action` = 'EXPIRE'",set={"is_latest": "0", "updated_date": "current_date()"}).whenNotMatchedInsert(
                    condition="s.`_scd_action` = 'INSERT'", values=insert_values).execute()
            )
        # SUCCESS LOG
        process_log("Success", table_name=table_name, layer="silver", total_inserted_count=new_count + changed_count)
        print(f"Completed: {final_table_name} | "f"New={new_count} | Changed={changed_count}")
    except Exception as e:
        print(f"Failed: {final_table_name} | {type(e).__name__}: {e}")
        process_log("Failed", table_name=table_name, layer="silver", error_message=str(e))
        send_mail_fn(f"{pipeline_name}-{task_name}-{table_name}- Silver layer is failed", str(e))
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
