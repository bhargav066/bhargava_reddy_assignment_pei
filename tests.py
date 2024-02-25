# Databricks notebook source
import unittest
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

# MAGIC %run "./data_ingestion_utils"

# COMMAND ----------



class TestDataIngestion(unittest.TestCase):

    def setUp(self):
        # Create a temporary file for testing
        dbutils.fs.put("/tmp/test_file.csv", "id,name\n1,Test", True)

    def tearDown(self):
        # Delete the temporary file after testing
        dbutils.fs.rm("/tmp/test_file.csv")

    def test_file_existence(self):
        result = file_exists_dbfs("/tmp/test_file.csv")
        self.assertTrue(result)

    # Additional test methods can go here


# COMMAND ----------

class TestLoadDataToDeltaCSV(unittest.TestCase):
    
    def setUp(self):
        # Define expected schema and data for the test CSV
        self.test_db = "test_db"
        self.test_table = "test_table"
        self.expected_schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True)
        ])
        self.expected_data = [['1', "Test"]]
        self.expected_df = spark.createDataFrame(self.expected_data, schema=self.expected_schema)

        # Create a temporary CSV file in DBFS for testing
        self.temp_file_path = "/tmp/test_data.csv"
        self.expected_df.write.csv(self.temp_file_path, header=True, mode="overwrite")

    def tearDown(self):
        # Delete the temporary CSV file after testing
        dbutils.fs.rm(self.temp_file_path, recurse=True)
        spark.sql(f"drop table {self.test_db}.{self.test_table}")
        spark.sql(f"drop database {self.test_db}")

    def test_load_data_csv(self):
        load_data_to_delta(self.temp_file_path, "csv", "full", self.test_db, self.test_table)
        loaded_df = spark.table(f"{self.test_db}.{self.test_table}")
        # Convert DataFrames to Pandas for easier comparison
        loaded_data = [list(row) for row in loaded_df.collect()]
        self.assertEqual(loaded_data, self.expected_data, "Loaded data does not match expected data")



# COMMAND ----------

class TestLoadDataToDeltaJSON(unittest.TestCase):
    
    def setUp(self):
        # Define expected schema and data for the test CSV
        self.test_db = "test_db"
        self.test_table = "test_table"
        self.expected_schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True)
        ])
        self.expected_data = [['1', "Test"]]
        self.expected_df = spark.createDataFrame(self.expected_data, schema=self.expected_schema)

        # Create a temporary CSV file in DBFS for testing
        self.temp_file_path = "/tmp/test_data.json"
        self.expected_df.write.json(self.temp_file_path, mode="overwrite")

    def tearDown(self):
        # Delete the temporary CSV file after testing
        dbutils.fs.rm(self.temp_file_path, recurse=True)
        spark.sql(f"drop table {self.test_db}.{self.test_table}")
        spark.sql(f"drop database {self.test_db}")

    def test_load_data_json(self):
        load_data_to_delta(self.temp_file_path, "json", "full", self.test_db, self.test_table)
        loaded_df = spark.table(f"{self.test_db}.{self.test_table}")
        # Convert DataFrames to Pandas for easier comparison
        loaded_data = [list(row) for row in loaded_df.collect()]
        self.assertEqual(loaded_data, self.expected_data, "Loaded data does not match expected data")



# COMMAND ----------

class TestLoadDataToDeltaExcel(unittest.TestCase):
    
    def setUp(self):
        # Define expected schema and data for the test CSV
        self.test_db = "test_db"
        self.test_table = "test_table"
        self.expected_schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True)
        ])
        self.expected_data = [['1', "Test"]]
        self.expected_df = spark.createDataFrame(self.expected_data, schema=self.expected_schema)

        # Create a temporary CSV file in DBFS for testing
        self.temp_file_path = "/tmp/test_data.xlsx"
        self.expected_df.write.format("com.crealytics.spark.excel").option("header", "true").option("dataAddress", "'Sheet1'!A1").mode("overwrite").save(self.temp_file_path)

    def tearDown(self):
        # Delete the temporary CSV file after testing
        dbutils.fs.rm(self.temp_file_path, recurse=True)
        spark.sql(f"drop table {self.test_db}.{self.test_table}")
        spark.sql(f"drop database {self.test_db}")

    def test_load_data_excel(self):
        load_data_to_delta(self.temp_file_path, "excel", "full", self.test_db, self.test_table)
        loaded_df = spark.table(f"{self.test_db}.{self.test_table}")
        # Convert DataFrames to Pandas for easier comparison
        loaded_data = [list(row) for row in loaded_df.collect()]
        self.assertEqual(loaded_data, self.expected_data, "Loaded data does not match expected data")



# COMMAND ----------

suite = unittest.TestSuite()

# Load test cases from each test class
suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestDataIngestion))
suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestLoadDataToDeltaCSV))
suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestLoadDataToDeltaJSON))
suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestLoadDataToDeltaExcel))

runner = unittest.TextTestRunner()

runner.run(suite)

# COMMAND ----------


