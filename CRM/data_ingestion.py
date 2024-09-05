import logging
from pyspark.sql import SparkSession, DataFrame
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Initialize Spark Session
# Initialize Spark session
spark = SparkSession.builder \
    .appName("Interaction Analysis") \
    .getOrCreate()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/data_validation.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger()

def print_green(text: str):
    """
    Print text in green to the console and log it.
    """
    logger.info(f"\033[92m{text}\033[0m")

def validate_ids(df1: DataFrame, df2: DataFrame, df1_col: str, df2_col: str, id_name: str):
    """
    Validate the presence of IDs from df1 in df2 and display any missing IDs.
    """
    missing_ids = df1.join(df2, df1[df1_col] == df2[df2_col], "left_anti") \
                     .select(df1[df1_col].alias(f"Missing_{id_name}"))
    
    if missing_ids.count() > 0:
        print_green(f"{id_name}s missing in {df2_col.split('_')[0]}.csv from {df1_col.split('_')[0]}.csv:")
        missing_ids.show()
        logger.info(f"{id_name}s missing in {df2_col.split('_')[0]}.csv from {df1_col.split('_')[0]}.csv:")
    else:
        logger.info(f"All {id_name}s from {df1_col.split('_')[0]}.csv are present in {df2_col.split('_')[0]}.csv.")

def initialize_spark_session(app_name: str) -> SparkSession:
    """
    Initialize and return a Spark session.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_data_files(file_paths: dict) -> dict:
    """
    Load CSV files into Spark DataFrames.
    """
    return {name: spark.read.csv(file, header=True, inferSchema=True) for name, file in file_paths.items()}

def display_dataframes(dfs: dict):
    """
    Display the first 5 records for each DataFrame.
    """
    for name, df in dfs.items():
        print_green(f"{name.capitalize()} DataFrame:")
        df.show(5, truncate=False)
        logger.info(f"Displayed first 5 records for {name.capitalize()} DataFrame.")

def validate_data(dfs: dict, validations: list):
    """
    Validate data between DataFrames based on the provided validation rules.
    """
    for df1_name, df2_name, id_name in validations:
        print_green(f"Verifying {id_name} matches between {df1_name}.csv and {df2_name}.csv...")
        logger.info(f"Starting validation of {id_name} between {df1_name}.csv and {df2_name}.csv.")
        validate_ids(dfs[df1_name], dfs[df2_name], id_name, id_name, id_name)

def main():
    # Load data from CSV files into DataFrames
    data_files = {
        "customers": "Dataset/customers.csv",
        "products": "Dataset/products.csv",
        "transactions": "Dataset/transactions.csv",
        "interactions": "Dataset/interactions.csv",
        "sales_team": "Dataset/sales_team.csv"
    }
    
    dfs = load_data_files(data_files)
    
    # Display initial records for each DataFrame
    display_dataframes(dfs)
    
    # Verify Data Accuracy
    validations = [
        ("transactions", "customers", "Customer_ID"),
        ("interactions", "customers", "Customer_ID"),
        ("transactions", "products", "Product_ID"),
        ("transactions", "sales_team", "Sales_Rep_ID")
    ]
    
    validate_data(dfs, validations)
    
    logger.info("Data validation completed.")

if __name__ == "__main__":
    main()
