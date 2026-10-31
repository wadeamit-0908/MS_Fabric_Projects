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
# silver_yaml_path = "/lakehouse/default/Files/config/yaml/silver/"
# dummy_date = "9999-01-01 00:00:00"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read all Silver YAML files
all_files = mssparkutils.fs.ls(silver_all_files)
if not all_files:
    raise FileNotFoundError(f"No YAML files found in {silver_yaml_path}")

for yaml_file in all_files:
    inserted_count = 0
    updated_count = 0
    table_name = yaml_file.name.rsplit(".", 1)[0]

    try:
        run_id = mssparkutils.env.getJobId() + "_" + datetime.now().strftime("%Y%m%d_%H%M%S")
        config = open_file_read_data(silver_yaml_path + yaml_file.name)["table"]

        table_name = config["name"]
        source_table = f'{config["source_schema_name"]}.{config.get("source_table_name", table_name)}'
        target_table = f'{config["schema_name"]}.{table_name}'

        columns = config["columns"]
        column_names = [c["name"] for c in columns]
        column_types = {c["name"]: c["data_type"].upper() for c in columns}

        created_by_col = "created_by"
        updated_by_col = "updated_by"

        business_keys = config["business_key"]
        hash_config = config["hash_key"]
        hash_name = hash_config.get("name", "hash_key")
        excluded_columns = config["excluded_columns"]

        scd = config["scd"]
        inserted_col = scd["inserted_date_column"]
        updated_col = scd["updated_date_column"]
        latest_col = scd["latest_flag_column"]
        latest_value = scd.get("latest_flag_value", 1)

        process_log("Start", table_name=table_name, layer="silver")
        #Metadata columns are generated in Silver, not read from Bronze
        metadata_columns = [hash_name, inserted_col, updated_col, latest_col, created_by_col, updated_by_col]
        data_columns = [c for c in column_names if c not in metadata_columns]
        hash_columns = [c for c in column_names if c not in excluded_columns]
        # data_columns = [c for c in column_names if c not in excluded_columns]
        # print("data_columns_old => ", data_columns_old)
        # print("excluded_columns => ", excluded_columns)
        # print("data_columns => ", data_columns)

        #Read Bronze data and cast columns using YAML data types
        bronze = spark.table(source_table).select(*data_columns)
        for c in data_columns:
            bronze = bronze.withColumn(c, F.col(c).cast(column_types[c]))

        #Check for NULL business keys
        null_condition = F.lit(False)
        for key in business_keys:
            null_condition = null_condition | F.col(key).isNull()
        if bronze.filter(null_condition).limit(1).count() > 0:
            raise ValueError(f"Null business key found in {source_table}")

        #Identify duplicate business keys
        duplicate_keys = (bronze.groupBy(*business_keys).count().filter(F.col("count") > 1))

        #Get all Bronze records belonging to duplicate groups
        duplicate_rows = bronze.join(duplicate_keys.select(*business_keys, F.col("count").alias("duplicate_count")), on=business_keys, how="inner")

        #Log duplicate records using reusable function
        duplicate_count = duplicate_log(duplicate_rows, run_id,  source_table,  business_keys, data_columns)

        #Remove duplicate records from Bronze processing
        bronze = bronze.join(duplicate_keys.select(*business_keys), on=business_keys, how="left_anti")

        #Validate the remaining Bronze records
        validate_business_keys(bronze, business_keys, source_table)

        #Generate hash for valid Bronze records
        bronze = generate_hash_key(bronze, hash_columns, hash_name)
        bronze.createOrReplaceTempView(f"tmp_{table_name}_bronze")

        #Read only current Silver records and generate their hash
        silver = (spark.table(target_table).filter(F.col(latest_col) == latest_value).select(*data_columns))

        validate_business_keys(silver, business_keys, target_table)
        silver = generate_hash_key(silver, hash_columns, hash_name)
        silver.createOrReplaceTempView(f"tmp_{table_name}_silver")

        #Compare Bronze and Silver using business key and hash
        b = bronze.alias("b")
        s = silver.withColumn("_found", F.lit(1)).alias("s")

        join_condition = None
        for key in business_keys:
            condition = F.col(f"b.`{key}`") == F.col(f"s.`{key}`")
            join_condition = condition if join_condition is None else join_condition & condition

        comparison = b.join(s, join_condition, "left")
        is_new = F.col("s._found").isNull()
        is_changed = (F.col("s._found").isNotNull() & (F.col(f"b.`{hash_name}`") != F.col(f"s.`{hash_name}`")))

        new_records = comparison.filter(is_new).select("b.*")
        changed_records = comparison.filter(is_changed).select("b.*")

        #Prepare new/current versions (both dates are TIMESTAMP in YAML)
        new_versions = (new_records.unionByName(changed_records)
                        .withColumn(inserted_col, F.current_timestamp())
                        .withColumn(updated_col, F.lit(dummy_date).cast("timestamp"))
                        .withColumn(latest_col, F.lit(latest_value).cast("int"))
                        .withColumn(created_by_col, F.lit(run_by_user))
                        .withColumn(updated_by_col, F.lit(run_by_user))
                        .select(*column_names))

        #Prepare rows that expire old current versions
        expire_records = (changed_records.select(*business_keys).withColumn("_scd_action", F.lit("EXPIRE")))

        insert_records = new_versions.withColumn("_scd_action", F.lit("INSERT"))
        merge_source = expire_records.unionByName(insert_records, allowMissingColumns=True)

        #Run one MERGE: expire old version, insert new/current version
        if new_records.limit(1).count() or changed_records.limit(1).count():
            delta_table = DeltaTable.forName(spark, target_table)

            merge_condition = " AND ".join(
                [f"t.`{key}` = s.`{key}`" for key in business_keys] +
                [f"t.`{latest_col}` = {int(latest_value)}", "s.`_scd_action` = 'EXPIRE'"]
            )

            insert_values = {c: f"s.`{c}`" for c in column_names}

            (delta_table.alias("t")
             .merge(merge_source.alias("s"), merge_condition)
             .whenMatchedUpdate(
                 set={latest_col: "0", updated_col: "current_timestamp()", updated_by_col: F.lit(run_by_user)}
             )
             .whenNotMatchedInsert(
                 condition="s.`_scd_action` = 'INSERT'",
                 values=insert_values
             )
             .execute()
            )
            inserted_count, updated_count = get_delta_merge_counts(target_table)
        process_log("Success", table_name=table_name, layer="silver", total_inserted_count=inserted_count, total_updated_count=updated_count)
        print(f"Completed {target_table}: inserted={inserted_count}, updated={updated_count}")
        send_mail_fn(f"{pipeline_name}-{task_name}-{table_name}- Silver layer is processed successfully", f"Inserted: {inserted_count}, Updated: {updated_count}")
    except Exception as e:
        error_text = str(e)
        print(f"Failed {table_name}: {error_text}")
        try:
            process_log("Failed", table_name=table_name, layer="silver", total_inserted_count=inserted_count, total_updated_count=updated_count, error_message=error_text)
        except Exception as log_error:
            print(f"Could not write failure log: {log_error}")
        try:
            send_mail_fn(f"{pipeline_name}-{task_name}-{table_name}- Silver layer is failed", error_text)
        except Exception as mail_error:
            print(f"Could not send failure email: {mail_error}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
