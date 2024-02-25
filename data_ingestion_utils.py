# Databricks notebook source
def file_exists_dbfs(file_path):
    try:
        dbutils.fs.ls(file_path)
        return True
    except Exception as e:
        return False

# COMMAND ----------

def load_data_to_delta(file_path, file_type, load_type, database_name, table_name):

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    
    spark.catalog.setCurrentDatabase(database_name)

    if not file_exists_dbfs(file_path):
        raise FileNotFoundError(f"No file found at {file_path}")

    if load_type == "incremental":
        pass  # Implement incremental load logic here
    else:
        if file_type == "csv":
            df = spark.read.option("header", "true").csv(file_path)
        elif file_type == "excel":
            df = spark.read.format("com.crealytics.spark.excel").option("header", "true").load(file_path)
        elif file_type == "json":
            df = spark.read.option("multiline", "true").json(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

        for col in df.columns:
            new_col = col.replace(" ", "_").replace(";", "").replace("{", "").replace("}", "").replace("(", "").replace(")", "").replace("\n", "").replace("\t", "").replace("=", "").replace("-", "_").lower()
            df = df.withColumnRenamed(col, new_col)

        # Write to Delta table
        df.write.format("delta").mode("overwrite").saveAsTable(f"{database_name}.{table_name}")


