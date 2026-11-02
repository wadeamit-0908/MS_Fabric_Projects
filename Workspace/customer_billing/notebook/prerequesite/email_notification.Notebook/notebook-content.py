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

if not log_table or not str(log_table).strip():
    log_table = "control.etl_run_log"
# Read the actual log table schema supplied for this solution.
log_df = spark.table(log_table)
required_columns = {"run_id", "pipeline_name", "task_name", "file_name", "table_name","layer", "business_date", "run_start_time", "run_end_time", "run_status", "total_inserted_count", 
"total_updated_count","error_message", "email_status"}
missing_columns = sorted(required_columns - set(log_df.columns))

if missing_columns:
    raise ValueError(f"{log_table} is missing expected columns: {', '.join(missing_columns)}")

run_df = log_df.filter((F.col("pipeline_name") == pipeline_name) & 
(F.to_timestamp(F.col("run_start_time")) >= F.to_timestamp(F.lit(pipeline_start_time))))
records = run_df.orderBy("run_start_time", "layer", "task_name").collect()

if not records:
    raise RuntimeError(f"No records found for pipeline={pipeline_name} " f"since {pipeline_start_time}.")
print(f"Found {len(records)} log record(s) " f"for pipeline {pipeline_name}.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _text(value, default="N/A"):
    if value is None:
        return default
    value = str(value).strip()
    return value if value else default


def _status_category(value):
    status = _text(value, "").lower()
    if any(word in status for word in ("fail", "error", "exception")):
        return "failed"
    if any(word in status for word in ("success", "succeeded", "complete", "completed")):
        return "success"
    if any(word in status for word in ("running", "in progress", "started")):
        return "running"
    if any(word in status for word in ("cancel", "abort", "skip")):
        return "other"
    return "other"


def _format_datetime(value):
    if value is None:
        return "N/A"
    try:
        return value.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(value)

# Aggregate activity-level results.
total_tasks = len(records)
success_count = 0
failed_count = 0
running_count = 0
other_count = 0
total_inserted = 0
total_updated = 0

layer_summary = {}
failed_details = []
activity_details = []
start_times = []
end_times = []

for row in records:
    status_raw = row["run_status"]
    category = _status_category(status_raw)
    layer = _text(row["layer"], "Unknown")

    if category == "success":
        success_count += 1
    elif category == "failed":
        failed_count += 1
    elif category == "running":
        running_count += 1
    else:
        other_count += 1

    inserted = int(row["total_inserted_count"] or 0)
    updated = int(row["total_updated_count"] or 0)
    total_inserted += inserted
    total_updated += updated

    if layer not in layer_summary:
        layer_summary[layer] = {"total": 0, "success": 0, "failed": 0, "running": 0, "other": 0, "inserted": 0, "updated": 0}

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
        "task": _text(row["task_name"]),
        "file": _text(row["file_name"]),
        "table": _text(row["table_name"]),
        "status": _text(status_raw),
        "start": _format_datetime(start_value),
        "end": _format_datetime(end_value),
        "inserted": inserted,
        "updated": updated,
        "error": _text(row["error_message"], "")
    }
    activity_details.append(detail)

    if category == "failed":
        failed_details.append(detail)

# Pipeline start parameter takes precedence over activity start times.
overall_start = min(start_times) if start_times else None
overall_end = max(end_times) if end_times else None

if pipeline_start_time:
    try:
        parsed_start = datetime.fromisoformat(
            str(pipeline_start_time).replace("Z", "+00:00")
        )
        overall_start = parsed_start
    except Exception:
        print(
            "Warning: pipeline_start_time could not be parsed; "
            "using earliest run_start_time from the log table."
        )

if overall_start and overall_end:
    # Normalize timezone awareness to avoid subtracting aware from naive datetimes.
    if getattr(overall_start, "tzinfo", None) is not None and getattr(overall_end, "tzinfo", None) is None:
        overall_start = overall_start.replace(tzinfo=None)
    elif getattr(overall_start, "tzinfo", None) is None and getattr(overall_end, "tzinfo", None) is not None:
        overall_end = overall_end.replace(tzinfo=None)

    duration_seconds = max(0, (overall_end - overall_start).total_seconds())
    hours, remainder = divmod(int(duration_seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    duration_text = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
else:
    duration_text = "Not available"

if failed_count:
    overall_status = "FAILED"
elif running_count or other_count:
    overall_status = "COMPLETED WITH NON-SUCCESS TASKS"
else:
    overall_status = "SUCCESS"

print(f"Overall status: {overall_status}")
print(f"Tasks: {total_tasks}; Success: {success_count}; Failed: {failed_count}")
print(f"Inserted: {total_inserted}; Updated: {total_updated}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build a plain-text message. The shared send_mail_fn applies the common HTML template.

subject = f"{pipeline_name} - {overall_status} - Pipeline Execution Summary"

message = []
message.append("PIPELINE EXECUTION SUMMARY")
message.append("=" * 32)
message.append(f"Pipeline Name : {pipeline_name}")
message.append(f"Pipeline Run ID: {pipeline_run_id}")
message.append(f"Overall Status: {overall_status}")
message.append(f"Pipeline Start: {_format_datetime(overall_start)}")
message.append(f"Pipeline End  : {_format_datetime(overall_end)}")
message.append(f"Total Duration: {duration_text}")
message.append("")
message.append("OVERALL COUNTS")
message.append("-" * 32)
message.append(f"Total activity records : {total_tasks}")
message.append(f"Successful             : {success_count}")
message.append(f"Failed                 : {failed_count}")
message.append(f"Running                : {running_count}")
message.append(f"Other / unknown        : {other_count}")
message.append(f"Total inserted records : {total_inserted:,}")
message.append(f"Total updated records  : {total_updated:,}")
message.append("")
message.append("LAYER SUMMARY")
message.append("-" * 32)

for layer in sorted(layer_summary.keys()):
    summary = layer_summary[layer]
    message.append(
        f"{layer}: total={summary['total']}, success={summary['success']}, "
        f"failed={summary['failed']}, running={summary['running']}, "
        f"other={summary['other']}, inserted={summary['inserted']:,}, "
        f"updated={summary['updated']:,}"
    )

message.append("")
message.append("ACTIVITY DETAILS")
message.append("-" * 32)

for item in activity_details:
    message.append(
        f"Layer: {item['layer']} | Task: {item['task']} | "
        f"File: {item['file']} | Table: {item['table']}"
    )
    message.append(
        f"Status: {item['status']} | Start: {item['start']} | "
        f"End: {item['end']} | Inserted: {item['inserted']:,} | "
        f"Updated: {item['updated']:,}"
    )
    if item["error"]:
        message.append(f"Error: {item['error']}")
    message.append("")

if failed_details:
    message.append("FAILED ACTIVITY DETAILS")
    message.append("-" * 32)
    for item in failed_details:
        message.append(
            f"{item['layer']} | {item['task']} | "
            f"File: {item['file']} | Table: {item['table']}"
        )
        message.append(f"Error: {item['error'] or 'No error_message recorded'}")
        message.append("")

email_body = "\n".join(message)
print(email_body)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Send one consolidated notification for this pipeline run.
# Keep the shared function signature unchanged.
send_mail_fn(subject, email_body)
print("Consolidated pipeline notification sent.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df = spark.table("control.etl_run_log")

# # Display all column names, data types, and nullable settings
# for field in df.schema.fields:
#     print(
#         f"Column: {field.name}, "
#         f"Data Type: {field.dataType.simpleString()}, "
#         f"Nullable: {field.nullable}"
#     )
# spark.sql("SHOW SCHEMAS").show(truncate=False)

# spark.sql("SHOW TABLES IN control").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
