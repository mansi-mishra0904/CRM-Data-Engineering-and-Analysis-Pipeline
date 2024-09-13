from pyspark.sql.functions import col, when, mean, date_format
import logging
from common_utility import *
import os

spark = initialize_spark_session("Transactions Cleaning")

log_file_path = 'logs/transactions_cleaning.log'
logger = initialize_logger(log_file_path)

# Step 1: Load data from CSV files into DataFrames using PySpark
logger.info("Step 1: Loading data from CSV files into DataFrames...")

# Define a list of tuples mapping file names to DataFrame variables
file_names = [
    ("transactions_df", "Dataset/transactions.csv"),
]

# Load all DataFrames using a loop
dfs = {}
for df_var, file in file_names:
    dfs[df_var] = spark.read.csv(file, header=True, inferSchema=True)

# Assign DataFrames to their respective variables
transactions_df = dfs["transactions_df"]

# Step 2: Initial Validation - Display records for each DataFrame
logger.info("Step 2: Displaying initial records for validation...")

# Loop through the DataFrames to display the first 5 records
for df_var, _ in file_names:
    logger.info(f"{df_var.replace('_df', '').capitalize()} DataFrame:")
    dfs[df_var].show(5, truncate=False)

# Step 1: Count missing values in each column before filling them
logger.info("Step 1: Counting missing values in each column before filling them...")
missing_values_before = count_missing_values(transactions_df)
missing_values_before.show()

# Step 2: Calculate the mean value of the 'Amount' column
logger.info("Step 2: Calculating the mean value of the 'Amount' column...")
mean_amount_row = transactions_df.agg(mean("Amount")).collect()[0]
mean_amount = mean_amount_row[0]
logger.info(f"Mean value calculated for 'Amount': {mean_amount:.2f}")

# Step 3: Fill missing values in the 'Amount' column with the rounded mean value
logger.info("Step 3: Filling missing values in the 'Amount' column with the mean value...")
cleaned_transactions_df = fill_missing_values(transactions_df, {"Amount": round(mean_amount, 2)})

# Step 4: Check for duplicate records based on 'Transaction_ID'
logger.info("Step 4: Checking for duplicate records based on 'Transaction_ID'...")
cleaned_transactions_df = drop_duplicates(cleaned_transactions_df, "Transaction_ID")

# Step 5: Ensure 'Date' is in YYYY-MM-DD format
logger.info("Step 5: Ensuring 'Date' column is in YYYY-MM-DD format...")
cleaned_transactions_df = cleaned_transactions_df.withColumn(
    "Date", date_format(col("Date"), "yyyy-MM-dd")
)

# Step 6: date validation
logger.info("Step 6: Validating dates present in Date column...")
cleaned_transactions_df = date_validation(cleaned_transactions_df,"Date")

# Step 7: Cross-verification after filling missing values in each column
logger.info("Step 7: Counting missing values in each column after filling them...")
missing_values_after = count_missing_values(cleaned_transactions_df)
missing_values_after.show()

# Step 8: Correct inaccurate data (e.g., non-positive 'Amount')
logger.info("Step 8: Correcting non-positive values in the 'Amount' column...")
corrected_transactions_df = cleaned_transactions_df.withColumn(
    "Amount",
    when(col("Amount") <= 0, mean_amount).otherwise(col("Amount"))
)

# Step 9: Display the cleaned DataFrame
logger.info("Step 9: Displaying the cleaned 'transactions_df' DataFrame...")
corrected_transactions_df.show(truncate=False)

# Step 10: Save the cleaned data to a new CSV
logger.info("Step 10: Saving the cleaned data to 'cleaned_transactions.csv'...")
# save_df_to_csv(corrected_transactions_df, "Cleaned_data/cleaned_transactions.csv")

# Display the count of records after phone number processing
record_count_after_cleaning = cleaned_transactions_df.count()
logger.info(f"Number of records after cleaning: {record_count_after_cleaning}")
logger.info("Data cleaning and export completed successfully.")