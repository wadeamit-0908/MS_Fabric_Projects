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

# **This notebook we use as a common functions in entire project**

# CELL ********************

#import libraries
import zipfile
import os
import smtplib
import time
import yaml
import re
import traceback
from pyspark.sql.functions import col
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from functools import reduce
from html import escape
from pyspark.sql.functions import col, lit, to_timestamp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run "./variable_assignments"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#UserName who run the pipeline
run_by_user = mssparkutils.env.getUserName()
# print(run_by_user)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#define functions - unzip_files
def unzip_files(zip_path, unzip_path):
    #validation 1: file exists
    if not os.path.exists(zip_path):
        validation_message = f"Error : zip file is not found: {zip_path}"
        raise FileNotFoundError(f"Error : zip file is not found: {zip_path}")
    else:
        print(f"file found: {zip_path}")
        validation_message = f"file found: {zip_path}"
    #validation 2: file is not empty - this is not working as blank zip file cannot be zero bytes most of the time.
    # if os.path.getsize(zip_path) == 0:
    #     raise Exception(f"zip file: {zip_path} is empty")
    # else:
    #     print(f"zip file: {zip_path} is not empty")
    #validation 2: zip file is empty based on list of the files
    with zipfile.ZipFile(zip_path, 'r') as zip_reference:
        list_files = zip_reference.namelist()
        if len(list_files) == 0:
            validation_message = f"Error : zip file: {zip_path} is empty"
            raise Exception(f"Error : zip file: {zip_path} is empty")
        else:
            print(f"zip file: {zip_path} is not empty")
            validation_message = f"zip file: {zip_path} is not empty"
    #validation 3: zip file is not empty but underneeth file is empty 
        empty_files =   [
                            file_info.filename
                            for file_info in zip_reference.infolist() if file_info.file_size == 0 
                        ]
        print(f"{empty_files} Error : These are the empty files present in: {zip_path} ")
        validation_message = f"{empty_files} Error : These are the empty files present in: {zip_path}"
    #validation 4: is zip file is a bad file
        corrupted_file = zip_reference.testzip()
        if corrupted_file:
            validation_message = f"Error : zip file is corrupted : {zip_path}"
            raise Exception(f"Error : zip file is corrupted : {zip_path}")
        else:
            print(f"zip file is not corrupted : {zip_path}")
            validation_message = f"zip file is not corrupted : {zip_path}"
    #validation 5: expected number of file inside the zip file
        list_files = zip_reference.namelist()
        total_files = len(list_files)
        print(total_files)
        print(excepted_files)
        if int(total_files) == 5:
            print(f"all files are available under zip file: {zip_path}")
            validation_message = f"all files are available under zip file: {zip_path}"
        else:
            print(f"Error : total files missing: {int(excepted_files) - int(total_files)}")
            validation_message = f"Error : total files missing: {int(excepted_files) - int(total_files)}"
    #to make a directory to extract file
        actual_unzip_path = unzip_path+folder_name
        os.makedirs(actual_unzip_path, exist_ok=True)
        zip_reference.extractall(actual_unzip_path)
    return validation_message

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Loggin Function
def process_log(
        run_status,
        file_name = None,
        table_name = None,
        layer = None,
        total_inserted_count = None,
        total_updated_count = None,
        error_message = None,
        email_status = None,
        business_date = None,
        run_end_time = None
    ):
    global run_id, run_start_time, pipeline_name, task_name 
    log_schema = """run_id string,
                    pipeline_name string,
                    task_name string,
                    file_name string,
                    table_name string,
                    layer string,
                    business_date date,
                    run_start_time timestamp,
                    run_end_time timestamp,
                    run_status string,
                    total_inserted_count long,
                    total_updated_count long,
                    error_message string,
                    email_status string
                 """
    run_end_time = datetime.now()
    business_date = run_end_time.date()
    log_data = [(
                    run_id, 
                    pipeline_name, 
                    task_name,
                    file_name,
                    table_name,
                    layer,
                    business_date, 
                    run_start_time, 
                    run_end_time, 
                    run_status, 
                    total_inserted_count, 
                    total_updated_count, 
                    error_message, 
                    email_status
                )]             
    log_df = spark.createDataFrame(log_data, schema=log_schema)
    log_df.createOrReplaceTempView("temp_etl_run_log")
    spark.sql("""
        MERGE INTO control.etl_run_log AS t
        USING temp_etl_run_log AS s
        ON t.run_id=s.run_id
        WHEN MATCHED THEN
        UPDATE SET t.run_status = s.run_status,
        t.total_inserted_count = s.total_inserted_count
        WHEN NOT MATCHED THEN INSERT *
        """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# #email notification
# def send_mail_fn(f_subject,b_message): 
#     sender_email = "amit.aigk@gmail.com"
#     to_email = "amit.aigk@gmail.com"
#     host = "smtp.gmail.com"
#     port = 587
#     server = smtplib.SMTP(host,port)
#     server.starttls()
#     server.login(sender_email,"phmo embc isbs gvlw")
#     m_text = b_message
#     test = MIMEText(m_text,"html","utf-8")
#     test["subject"] = f_subject
#     test["From"] = sender_email
#     test["to"] = to_email
#     server.send_message(test)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#This function is to open the file and assign the variables
def open_file_read_data(file_path):
    with open(file_path) as file_read:
        file_load = yaml.safe_load(file_read)
    return file_load

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read recursive file
def read_recursive_file_data(file_path):
    for item in mssparkutils.fs.ls(file_path):
        if item.isDir:
            item_path = read_recursive_file_data(item.path)
            if item_path:
                return item_path
        elif item.name.startswith(file_name_prefix):
            final_file_name = item.name
            final_file_path = item.path
    return final_file_name, final_file_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Match file name patttern
def match_file_name_pattern(file_name_patern, file_name, date_time_format=None):
    is_matched = re.match(file_name_pattern, file_name)
    if date_time_format:
        date_time_format = date_time_format
    else:
        date_time_format = "%Y%m%d%H%M%S"
    if is_matched:
        try:
            if datetime.strptime(f"{is_matched.group(1)}{is_matched.group(2)}", date_time_format):
                file_name_validation_message = "Date is valid"
        except ValueError:
            file_name_validation_message = "Date is invalid"
    else:
        file_name_validation_message =  "File format is wrong"
    return file_name_validation_message

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Reusable functions from silver layer
#Generate hash key
def generate_hash_key(df, hash_columns, hash_name="hash_key"):
    payload = F.to_json(
        F.struct(*[F.col(c).cast("string").alias(c) for c in hash_columns]),
        options={"ignoreNullFields": "false"}
    )
    return df.withColumn(hash_name, F.sha2(payload, 256))

#Validate duplicates based on business key
def validate_business_keys(df, business_keys, table_name):
    for key in business_keys:
        if df.filter(F.col(key).isNull()).limit(1).count():
            raise ValueError(f"Null business key found in {table_name}: {key}")

    if df.groupBy(*business_keys).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"Duplicate business keys found in {table_name}")

#Get count after succesful
def get_delta_merge_counts(table_name):
    history = DeltaTable.forName(spark, table_name).history(1).first()

    if history is None or history["operation"] != "MERGE":
        raise ValueError(f"No MERGE operation found in Delta history for {table_name}")

    metrics = history["operationMetrics"] or {}
    inserted_count = int(metrics.get("numTargetRowsInserted", "0"))
    updated_count = int(metrics.get("numTargetRowsUpdated", "0"))

    return inserted_count, updated_count

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to log duplicate records
def duplicate_log(duplicate_df, run_id, source_table, business_keys, data_columns):
    duplicate_log_table = "control.duplicate_data_log"

    # Check whether duplicate records exist
    if duplicate_df.limit(1).count() == 0:
        print(f"No duplicate records found in {source_table}")
        return 0

    # Prepare duplicate log data
    duplicate_log_df = duplicate_df.select(
        F.lit(run_id).alias("run_id"),
        F.lit(source_table).alias("source_table"),
        F.lit(",".join(business_keys)).alias("business_key"),
        F.to_json(
            F.struct(*[F.col(c) for c in business_keys])
        ).alias("business_key_value"),
        F.col("duplicate_count").cast("int"),
        F.to_json(
            F.struct(*[F.col(c) for c in data_columns])
        ).alias("record_data"),
        F.current_timestamp().alias("logged_at")
    )

    # Insert duplicate records into control table
    duplicate_log_df.write.format("delta").mode("append").saveAsTable(duplicate_log_table)
    duplicate_count = duplicate_df.count()
    print(f"Duplicate records logged successfully: "f"{duplicate_count} | Table: {duplicate_log_table}")
    return duplicate_count

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Send email notification function
sender_email = "amit.aigk@gmail.com"
to_email = "amit.aigk@gmail.com"
gmail_app_password = "djvj girr ycun xrqp"
host = "smtp.gmail.com"
port = 587
def send_mail_fn(f_subject, b_message):
    safe_subject = escape(str(f_subject))
    safe_message = (escape(str(b_message)).replace("\r\n", "\n").replace("\n", "<br>"))
    html_template = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body style=" margin:0; padding:20px; background-color:#f4f6f9; font-family:Arial,Helvetica,sans-serif; "><table width="100%" cellpadding="0" cellspacing="0" style="max-width:650px;margin:auto; background-color:#ffffff;border-collapse:collapse;"><tr><td style=" background-color:#17365D; padding:22px; text-align:center; color:#ffffff; "><h2 style="margin:0;"> Data Engineering Notification </h2><p style="margin:8px 0 0;font-size:13px;"> Customer Billing Data Platform </p></td></tr><tr><td style=" padding:25px; color:#333333; font-size:14px; line-height:1.7; "><h3 style="margin-top:0;color:#17365D;"> {safe_subject} </h3><div style=" background-color:#f4f6f9; border-left:4px solid #17365D; padding:15px; border-radius:4px; overflow-wrap:anywhere; "> {safe_message} </div></td></tr><tr><td style=" background-color:#f1f3f5; padding:15px; text-align:center; color:#777777; font-size:12px; "> This is an automated email notification.<br> Data Engineering Team </td></tr></table></body></html>"""
    server = None
    try:
        mail = MIMEMultipart("alternative")
        mail["Subject"] = str(f_subject)
        mail["From"] = sender_email
        mail["To"] = to_email
        mail.attach(MIMEText(html_template, "html", "utf-8"))
        server = smtplib.SMTP(host, port, timeout=30)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(sender_email, gmail_app_password)
        server.send_message(mail)
        print("Email sent successfully!")
    except Exception as e:
        print("Email sending failed:", str(e))
        raise
    finally:
        if server is not None:
            try:
                server.quit()
            except Exception:
                pass
def send_pipeline_notification_fn(pipeline_run_id, pipeline_name, pipeline_start_time=None, log_table="control.etl_run_log"):
    if not pipeline_run_id or not str(pipeline_run_id).strip():
        raise ValueError("pipeline_run_id is required.")
    if not pipeline_name or not str(pipeline_name).strip():
        raise ValueError("pipeline_name is required.")
    required_columns = [
        "run_id", "pipeline_name", "task_name", "file_name", "table_name",
        "layer", "business_date", "run_start_time", "run_end_time",
        "run_status", "total_inserted_count", "total_updated_count",
        "error_message", "email_status"
    ]
    log_df = spark.table(log_table)
    missing_columns = [c for c in required_columns if c not in log_df.columns]
    if missing_columns:
        raise ValueError(
            f"Table {log_table} is missing columns: {', '.join(missing_columns)}"
        )
    run_records = (
        log_df
        .filter(log_df["run_id"] == str(pipeline_run_id))
        .orderBy("run_start_time", "layer", "task_name")
        .collect()
    )
    if not run_records:
        raise RuntimeError(
            f"No records found in {log_table} for run_id={pipeline_run_id}. "
            "Verify that all layer notebooks log the same pipeline RunId."
        )
    def _value(value, default="N/A"):
        if value is None:
            return default
        value = str(value).strip()
        return value if value else default
    def _category(status):
        value = _value(status, "").lower()
        if any(word in value for word in ("fail", "error", "exception")):
            return "failed"
        if any(word in value for word in (
            "success", "succeeded", "complete", "completed"
        )):
            return "success"
        if any(word in value for word in ("running", "in progress", "started")):
            return "running"
        return "other"
    def _format_time(value):
        return value.strftime("%Y-%m-%d %H:%M:%S") if value else "N/A"
    total_tasks = len(run_records)
    counts = {"success": 0, "failed": 0, "running": 0, "other": 0}
    total_inserted = 0
    total_updated = 0
    layer_summary = {}
    activity_details = []
    failed_details = []
    start_times = []
    end_times = []
    for row in run_records:
        category = _category(row["run_status"])
        layer = _value(row["layer"], "Unknown")
        counts[category] += 1
        inserted = int(row["total_inserted_count"] or 0)
        updated = int(row["total_updated_count"] or 0)
        total_inserted += inserted
        total_updated += updated
        if layer not in layer_summary:
            layer_summary[layer] = {
                "total": 0,
                "success": 0,
                "failed": 0,
                "running": 0,
                "other": 0,
                "inserted": 0,
                "updated": 0
            }
        layer_summary[layer]["total"] += 1
        layer_summary[layer][category] += 1
        layer_summary[layer]["inserted"] += inserted
        layer_summary[layer]["updated"] += updated
        start_value = row["run_start_time"]
        end_value = row["run_end_time"]
        if start_value is not None:
            start_times.append(start_value)
        if end_value is not None:
            end_times.append(end_value)
        detail = {
            "layer": layer,
            "task": _value(row["task_name"]),
            "file": _value(row["file_name"]),
            "table": _value(row["table_name"]),
            "status": _value(row["run_status"]),
            "start": _format_time(start_value),
            "end": _format_time(end_value),
            "inserted": inserted,
            "updated": updated,
            "error": _value(row["error_message"], "")
        }
        activity_details.append(detail)
        if category == "failed":
            failed_details.append(detail)
    overall_start = min(start_times) if start_times else None
    overall_end = max(end_times) if end_times else None
    if pipeline_start_time:
        try:
            parsed_start = datetime.fromisoformat(
                str(pipeline_start_time).replace("Z", "+00:00")
            )
            if parsed_start.tzinfo is not None and overall_end is not None:
                parsed_start = parsed_start.replace(tzinfo=None)
            overall_start = parsed_start
        except (TypeError, ValueError):
            print(
                "Warning: pipeline_start_time could not be parsed. "
                "Using the earliest run_start_time in the log table."
            )
    duration_text = "Not available"
    if overall_start is not None and overall_end is not None:
        if overall_start.tzinfo is not None and overall_end.tzinfo is None:
            overall_start = overall_start.replace(tzinfo=None)
        elif overall_start.tzinfo is None and overall_end.tzinfo is not None:
            overall_end = overall_end.replace(tzinfo=None)
        seconds = max(0, int((overall_end - overall_start).total_seconds()))
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        duration_text = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    if counts["failed"] > 0:
        overall_status = "FAILED"
    elif counts["running"] > 0 or counts["other"] > 0:
        overall_status = "COMPLETED WITH NON-SUCCESS TASKS"
    else:
        overall_status = "SUCCESS"
    subject = f"{pipeline_name} - {overall_status} - Pipeline Execution Summary"
    lines = [
        "PIPELINE EXECUTION SUMMARY",
        "=" * 34,
        f"Pipeline Name : {pipeline_name}",
        f"Pipeline Run ID: {pipeline_run_id}",
        f"Overall Status: {overall_status}",
        f"Pipeline Start: {_format_time(overall_start)}",
        f"Pipeline End  : {_format_time(overall_end)}",
        f"Total Duration: {duration_text}",
        "",
        "OVERALL COUNTS",
        "-" * 34,
        f"Total activity records : {total_tasks}",
        f"Successful             : {counts['success']}",
        f"Failed                 : {counts['failed']}",
        f"Running                : {counts['running']}",
        f"Other / unknown        : {counts['other']}",
        f"Total inserted records : {total_inserted:,}",
        f"Total updated records  : {total_updated:,}",
        "",
        "LAYER SUMMARY",
        "-" * 34
    ]
    for layer in sorted(layer_summary):
        item = layer_summary[layer]
        lines.append(
            f"{layer}: total={item['total']}, success={item['success']}, "
            f"failed={item['failed']}, running={item['running']}, "
            f"other={item['other']}, inserted={item['inserted']:,}, "
            f"updated={item['updated']:,}"
        )
    lines.extend(["", "ACTIVITY DETAILS", "-" * 34])
    for item in activity_details:
        lines.append(
            f"Layer: {item['layer']} | Task: {item['task']} | "
            f"File: {item['file']} | Table: {item['table']}"
        )
        lines.append(
            f"Status: {item['status']} | Start: {item['start']} | "
            f"End: {item['end']} | Inserted: {item['inserted']:,} | "
            f"Updated: {item['updated']:,}"
        )
        if item["error"]:
            lines.append(f"Error: {item['error']}")
        lines.append("")
    if failed_details:
        lines.extend(["FAILED ACTIVITY DETAILS", "-" * 34])
        for item in failed_details:
            lines.append(
                f"{item['layer']} | {item['task']} | "
                f"File: {item['file']} | Table: {item['table']}"
            )
            lines.append(f"Error: {item['error'] or 'No error_message recorded'}")
            lines.append("")
    email_body = "\n".join(lines)
    send_mail_fn(subject, email_body)
    print(
        f"Pipeline notification sent. Run ID={pipeline_run_id}, "
        f"status={overall_status}, records={total_tasks}."
    )
    return {
        "run_id": str(pipeline_run_id),
        "pipeline_name": str(pipeline_name),
        "overall_status": overall_status,
        "total_tasks": total_tasks,
        "success_count": counts["success"],
        "failed_count": counts["failed"],
        "running_count": counts["running"],
        "other_count": counts["other"],
        "total_inserted_count": total_inserted,
        "total_updated_count": total_updated,
        "duration": duration_text,
        "layer_summary": layer_summary
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC TRUNCATE TABLE gold.products

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
