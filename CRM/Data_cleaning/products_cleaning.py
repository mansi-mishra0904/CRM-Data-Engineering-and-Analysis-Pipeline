from pyspark.sql.functions import col, when
import logging
import os
from common_utility import *

spark = initialize_spark_session("Products Cleaning")

log_file_path = 'logs/products_cleaning.log'
logger = initialize_logger(log_file_path)

# Step 1: Load data from CSV files into DataFrames using PySpark
logger.info("Step 1: Loading data from CSV files into DataFrames...")

# Define a list of tuples mapping file names to DataFrame variables
file_names = [
    ("products_df", "Dataset/products.csv"),
]

# Load all DataFrames using a loop
dfs = {}
for df_var, file in file_names:
    dfs[df_var] = spark.read.csv(file, header=True, inferSchema=True)

# Assign DataFrames to their respective variables
products_df = dfs["products_df"]

# Step 2: Initial Validation - Display records for each DataFrame
logger.info("Step 2: Displaying initial records for validation...")

# Loop through the DataFrames to display the first 5 records
for df_var, _ in file_names:
    logger.info(f"{df_var.replace('_df', '').capitalize()} DataFrame:")
    dfs[df_var].show(5, truncate=False)

# Step 1: Count missing (null) values for each column before filling them
logger.info("Step 1: Counting missing values in each column before filling them...")
missing_values_before = count_missing_values(products_df)
missing_values_before.show()

# Step 2: Count duplicates before dropping them
logger.info("Step 2: Checking for duplicates in each column before dropping them...")
duplicate_count = count_duplicates_per_column(products_df)
duplicate_count.show()

# Step 3: Drop duplicates based on 'Customer_ID' if any are found
logger.info("Step 3: Checking and removing duplicate records based on 'Customer_ID'...")
cleaned_products_df = drop_duplicates(products_df, "Product_ID")

# Step 4: Fill missing values in 'Email' and 'Phone' columns
logger.info("Step 4: Filling missing values in 'Email' and 'Phone' columns...")
cleaned_products_df = fill_missing_values(cleaned_products_df, {"Category": "Uncategorized"})
cleaned_products_df.show(5, truncate=False)

# Step 5: Capitalize the first letter of the first and last names in the 'Name' and 'Country' columns
logger.info("Step 5: Capitalizing the first letter of each word in the 'Product_Name' and 'Category' columns...")
cleaned_products_df = capitalize_columns(cleaned_products_df, ["Product_Name", "Category"])
logger.info("Completed.")

# Step 6: Cross-validation - Count missing values again after filling them
logger.info("Step 6: Counting missing values in each column after filling them...")
missing_values_after = count_missing_values(cleaned_products_df)
missing_values_after.show()

# Step 7: Count duplicates after dropping them
logger.info("Step 7: Checking for duplicates in each column after dropping them...")
duplicate_count_after = count_duplicates_per_column(cleaned_products_df)
duplicate_count_after.show()

# Step 8: Handle negative or zero prices by replacing with average price
logger.info("Step 8: Replacing negative or zero prices with the average price...")
avg_price = products_df.agg({"Price": "avg"}).collect()[0][0]
cleaned_products_df = cleaned_products_df.withColumn("Price", when(col("Price") <= 0, avg_price).otherwise(col("Price")))
logger.info("completed.")

# Step 9: Export the cleaned data to a CSV file
logger.info("Step 9: Exporting the cleaned data to 'cleaned_products.csv'...")
# save_df_to_csv(cleaned_customers_df, "Cleaned_data/cleaned_products.csv")

# show duplicates
# get_duplicate_data_per_column(cleaned_products_df)

# Display the count of records after phone number processing
record_count_after_cleaning = cleaned_products_df.count()
logger.info(f"Number of records after cleaning: {record_count_after_cleaning}")

logger.info("Data cleaning and export completed successfully.")