import logging
import os
from common_utility import *

spark = initialize_spark_session("Customers Cleaning")

log_file_path = 'logs/customer_cleaning.log'
logger = initialize_logger(log_file_path)

logger.info("Logger initialized with dynamic path!")

# Step 1: Load data from CSV files into DataFrames using PySpark
logger.info("Step 1: Loading data from CSV files into DataFrames...")

# Define a list of tuples mapping file names to DataFrame variables
file_names = [
    ("customers_df", "Dataset/customers.csv")
]

# Load DataFrames using a loop
dfs = {}
for df_var, file in file_names:
    dfs[df_var] = spark.read.csv(file, header=True, inferSchema=True)

# Assign DataFrames to their respective variables
customers_df = dfs["customers_df"]

# Step 2: Initial Validation - Display records for each DataFrame
logger.info("Step 2: Displaying initial records for validation...")

# Loop through the DataFrames to display the first 5 records
for df_var, _ in file_names:
    logger.info(f"{df_var.replace('_df', '').capitalize()} DataFrame:")
    dfs[df_var].show(5, truncate=False)

# Step 1: Count missing (null) values for each column before filling them
logger.info("Step 1: Counting missing values in each column before filling them...")
missing_values_before = count_missing_values(customers_df)
missing_values_before.show()

# Step 2: Count duplicates before dropping them
logger.info("Step 2: Checking for duplicates in each column before dropping them...")
duplicate_count = count_duplicates_per_column(customers_df)
duplicate_count.show()

# Step 3: Format and clean phone numbers
logger.info("Step 3: Formatting and cleaning phone numbers...")
cleaned_customers_df = process_phone_numbers(customers_df, "Dataset/countries.csv")
logger.info("Completed.")

# Step 4: Drop duplicates based on 'Customer_ID' if any are found
logger.info("Step 4: Checking and removing duplicate records based on 'Customer_ID'...")
cleaned_customers_df = drop_duplicates(cleaned_customers_df, "Customer_ID")

# Step 5: Format and clean Email
logger.info("Step 5: Formatting and cleaning Email...")
cleaned_customers_df = validate_emails(cleaned_customers_df, "Email")
logger.info("Completed.")

# Step 5: Fill missing values in 'Email' and 'Phone' columns
logger.info("Step 5: Filling missing values in 'Email' and 'Phone' columns...")
cleaned_customers_df = fill_missing_values(cleaned_customers_df, {'Email': 'unknown', 'Phone': 'unknown'})
cleaned_customers_df.show(5, truncate=False)

# Step 6: Capitalize the first letter of the first and last names in the 'Name' and 'Country' columns
logger.info("Step 6: Capitalizing the first letter of each word in the 'Name' and 'Country' columns...")
cleaned_customers_df = capitalize_columns(cleaned_customers_df, ["Name", "Country"])
logger.info("Completed.")

# Step 7: Cross-validation - Count missing values again after filling them
logger.info("Step 7: Counting missing values in each column after filling them...")
missing_values_after = count_missing_values(cleaned_customers_df)
missing_values_after.show()

# Step 8: Count duplicates after dropping them
logger.info("Step 8: Checking for duplicates in each column after dropping them...")
duplicate_count_after = count_duplicates_per_column(cleaned_customers_df)
duplicate_count_after.show()

# Step 9: Export the cleaned data to a CSV file
logger.info("Step 9: Exporting the cleaned data to 'cleaned_customers.csv'...")
# save_df_to_csv(cleaned_customers_df, "Cleaned_data/cleaned_customers.csv")

# Display the count of records after phone number processing
record_count_after_cleaning = cleaned_customers_df.count()
logger.info(f"Number of records after cleaning: {record_count_after_cleaning}")

logger.info("Data cleaning and export completed successfully.")