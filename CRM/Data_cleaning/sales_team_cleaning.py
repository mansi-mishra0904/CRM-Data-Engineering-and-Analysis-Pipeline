import logging
import os
from common_utility import *

class CustomFormatter(logging.Formatter):
    # ANSI escape codes for colors
    GREEN = "\033[92m"
    RESET = "\033[0m"

    def format(self, record):
        # Add green color to INFO level messages
        if record.levelno == logging.INFO:
            record.msg = f"{self.GREEN}{record.msg}{self.RESET}"
        return super().format(record)

# Ensure the logs directory exists
os.makedirs('logs', exist_ok=True)

# Initialize the logger
logger = logging.getLogger('DataCleaningLogger')
logger.setLevel(logging.INFO)

# Remove all existing handlers
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Create a stream handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(CustomFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

# Create a file handler
fh = logging.FileHandler('logs/data_cleaning.log')
fh.setLevel(logging.INFO)
fh.setFormatter(CustomFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

# Add handlers to the logger
logger.addHandler(ch)
logger.addHandler(fh)

# Step 1: Load data from CSV files into DataFrames using PySpark
logger.info("Step 1: Loading data from CSV files into DataFrames...")

# Define a list of tuples mapping file names to DataFrame variables
file_names = [
    ("interactions_df", "Dataset/interactions.csv"),
]

# Load all DataFrames using a loop
dfs = {}
for df_var, file in file_names:
    dfs[df_var] = spark.read.csv(file, header=True, inferSchema=True)

# Assign DataFrames to their respective variables
interactions_df = dfs["interactions_df"]

# Step 2: Initial Validation - Display records for each DataFrame
logger.info("Step 2: Displaying initial records for validation...")

# Loop through the DataFrames to display the first 5 records
for df_var, _ in file_names:
    logger.info(f"{df_var.replace('_df', '').capitalize()} DataFrame:")
    dfs[df_var].show(5, truncate=False)


# Step 1: Identify Missing Values
logger.info("Step 1: Identifying missing values in each column before filling them...")
missing_values_before = count_missing_values(sales_team_df)
missing_values_before.show()

# Step 2: Handle Missing Values
logger.info("Step 2: Handling missing values by filling with averages...")
# Calculate average Sales_Achieved
avg_sales_achieved = sales_team_df.select(avg(col("Sales_Achieved"))).first()[0]
logger.info(f"Average value of sales achieved column is {avg_sales_achieved}")

# Fill missing Sales_Achieved with the average
sales_team_cleaned_df = fill_missing_values(sales_team_df,{"Sales_Achieved": round(avg_sales_achieved,2)})

# Step 3: Check for Duplicate Values
logger.info("Step 3: Checking for duplicate records based on 'Sales_Rep_ID'...")
duplicate_count_before = count_duplicates_per_column(sales_team_cleaned_df)

# Step 4: Drop Duplicates if they exist
sales_team_cleaned_df = drop_duplicates(sales_team_cleaned_df, "Sales_Rep_ID")

# Step 6: Standardize Formats
logger.info("Step 6: Standardizing the format of 'Name' and 'Region' columns...")
sales_team_cleaned_df = capitalize_columns(sales_team_cleaned_df, [ "Name", "Region"])
logger.info("Completed.")

# Step 7: Cross-verification of missing values in each column after filling them
logger.info("Step 7: Identifying missing values in each column after filling them...")
missing_values_after = count_missing_values(sales_team_cleaned_df)
missing_values_after.show()

# Step 8: Display the cleaned 'sales_team_df' DataFrame
logger.info("Step 8: Displaying the cleaned 'sales_team_df' DataFrame...")
sales_team_cleaned_df.show(truncate=False)

# Step 9: Save the cleaned data to a new CSV
logger.info("Step 9: Saving the cleaned data to 'cleaned_sales_team.csv'...")
# save_df_to_csv(sales_team_cleaned_df, "Cleaned_data/cleaned_sales_team.csv")

# Display the count of records after phone number processing
record_count_after_cleaning = sales_team_cleaned_df.count()
logger.info(f"Number of records after cleaning: {record_count_after_cleaning}")
logger.info("Data cleaning and export completed successfully.")