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
yaml_path = "/lakehouse/default/Files/config/yaml/gold/"
dummy_date = "9999-01-01"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Reading yaml files from gold
all_files = mssparkutils.fs.ls("Files/config/yaml/gold/")
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
        ytable_columns_names = [col["name"] for col in reading_yaml_data["table"]["columns"]]
        ytable_business_key = reading_yaml_data["table"]["business_key"]
        ytable_hash_key_columns = reading_yaml_data["table"]["hash_key"]["columns"]
        
        # START LOG
        process_log("Start", table_name=table_name, layer="gold")
        metadata_columns = ["hash_key", "inserted_date", "updated_date"]
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
        # Read source data
        df_source = spark.table(final_source_table_name).select(*data_columns)
        # Validate source business keys
        if df_source.groupBy(*ytable_business_key).count().filter(F.col("count") > 1).limit(1).count():
            raise ValueError(f"Duplicate business keys found in {final_source_table_name}")
        # Generate hash key
        df_source = generate_hash_key(df_source)
        source_count = df_source.count()
        # Read current Gold records
        df_gold = spark.table(final_table_name)
        missing_target_columns = [
            c for c in ytable_columns_names
            if c not in df_gold.columns
        ]
        if missing_target_columns:
            raise ValueError(f"Target table missing columns: {missing_target_columns}")
        df_gold = df_gold.select(*(data_columns + ["hash_key"]))
        # Compare source with Gold
        s = df_source.alias("s")
        g = df_gold.select(*ytable_business_key, F.col("hash_key").alias("_gold_hash"), F.lit(1).alias("_gold_exists")).alias("g")
        join_condition = reduce(lambda a, b: a & b,
            [
                F.col(f"s.`{key}`") == F.col(f"g.`{key}`")
                for key in ytable_business_key
            ]
        )
        comparison = s.join(g, join_condition, "left")
        is_new = F.col("g._gold_exists").isNull()
        is_changed = (
            F.col("g._gold_exists").isNotNull()
            & ~F.col("s.hash_key").eqNullSafe(F.col("g._gold_hash"))
        )
        new_records = comparison.filter(is_new).select("s.*")
        changed_records = comparison.filter(is_changed).select("s.*")
        inserted_count = new_records.count()
        updated_count = changed_records.count()
        unchanged_count = source_count - inserted_count - updated_count
        print(f"New records: {inserted_count}")
        print(f"Changed records: {updated_count}")
        print(f"Unchanged records: {unchanged_count}")
        # Prepare records for Gold
        merge_source = (
            new_records.unionByName(changed_records)
            .withColumn("inserted_date", F.current_date())
            .withColumn("updated_date", F.to_date(F.lit(dummy_date)))
            .select(*ytable_columns_names)
        )
        # SCD Type 1 MERGE
        if inserted_count + updated_count > 0:
            merge_condition = " AND ".join([
                f"t.`{key}` <=> s.`{key}`"
                for key in ytable_business_key
            ])
            # Update changed records; preserve original inserted_date
            update_values = {
                col: f"s.`{col}`" for col in data_columns
            }
            update_values["hash_key"] = "s.`hash_key`"
            update_values["updated_date"] = "current_date()"
            # Insert new records
            insert_values = {
                col: f"s.`{col}`" for col in data_columns
            }
            insert_values["hash_key"] = "s.`hash_key`"
            insert_values["inserted_date"] = "current_date()"
            insert_values["updated_date"] = f"to_date('{dummy_date}')"
            (
                DeltaTable.forName(spark, final_table_name)
                .alias("t")
                .merge(merge_source.alias("s"), merge_condition)
                .whenMatchedUpdate(
                    condition="NOT (t.`hash_key` <=> s.`hash_key`)",
                    set=update_values
                )
                .whenNotMatchedInsert(values=insert_values)
                .execute()
            )
        # SUCCESS LOG
        process_log("Success", table_name=table_name, layer="gold", total_inserted_count=inserted_count + updated_count)
        print(f"Completed: {final_table_name} | "f"New={inserted_count} | Changed={updated_count} | "f"Unchanged={unchanged_count}")
        send_mail_fn(f"{pipeline_name}-{task_name}-{table_name}- Gold layer is successfully executed")
    except Exception as e:
        print(f"Failed: {final_table_name} | "f"{type(e).__name__}: {e}")
        process_log("Failed", table_name=table_name, layer="gold", error_message=str(e))
        send_mail_fn(f"{pipeline_name}-{task_name}-{table_name}- Gold layer is failed", str(e))
        raise
print("All configured Gold SCD1 tables processed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
