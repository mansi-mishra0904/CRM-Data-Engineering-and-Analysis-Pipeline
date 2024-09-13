import logging
import os
from common_utility import *

spark = initialize_spark_session("Interactions Cleaning")

log_file_path = 'logs/interactions_cleaning.log'
logger = initialize_logger(log_file_path)

logger.info("Logger initialized with dynamic path!")

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


# Step 1: Identify missing values in each column before filling them
logger.info("Step 1: Identifying missing values in each column before filling them...")
missing_values_before = count_missing_values(interactions_df)
missing_values_before.show()

# Step 2: Count occurrences of each 'Interaction_Type' and identify the most occurring type
logger.info("Step 2: Counting occurrences of each 'Interaction_Type' to find the most occurring type...")
type_counts = interactions_df.groupBy("Interaction_Type").count() \
    .orderBy(col("count").desc()) \
    .first()

# Extract the most occurring 'Interaction_Type'
most_occurring_type = type_counts["Interaction_Type"] if type_counts else None
logger.info(f"The most occurring 'Interaction_Type' is: {most_occurring_type}")

# Step 3: Replace only null values in 'Interaction_Type' with the most occurring type
logger.info("Step 3: Replacing null values in 'Interaction_Type' with the most occurring type...")
cleaned_interactions_df = interactions_df.withColumn(
    "Interaction_Type",
    when(col("Interaction_Type").isNull(), lit(most_occurring_type))
    .otherwise(col("Interaction_Type"))
)

# Step 4: Check for duplicate records based on 'Interaction_ID'
logger.info("Step 4: Checking for duplicate records based on 'Interaction_ID'...")
cleaned_interactions_df = drop_duplicates(cleaned_interactions_df, "Interaction_ID")

# Step 5: Capitalize the first letter of the values in the 'Issue_Resolved' and 'Interaction_Type' columns
logger.info("Step 5: Capitalizing the first letter of each word in the and 'Interaction_Type' columns...")
cleaned_interactions_df = capitalize_columns(cleaned_interactions_df, [ "Interaction_Type"])
logger.info("Completed.")

# Step 6: date validation
logger.info("Step 6: Validating dates present in Interaction_Date column...")
cleaned_interactions_df = date_validation(cleaned_interactions_df,"Interaction_Date")

# Step 7: boolean validation
logger.info("Step 7: Validating booleans present in Issue_Resolved column...")
cleaned_interactions_df = validate_boolean_values(cleaned_interactions_df,"Issue_Resolved")

# Step 8: Cross-verification of missing values in each column after filling them
logger.info("Step 8: Identifying missing values in each column after filling them...")
missing_values_after = count_missing_values(cleaned_interactions_df)
missing_values_after.show()

# Step 9: Display the cleaned 'interactions_df' DataFrame
logger.info("Step 9: Displaying the cleaned 'interactions_df' DataFrame...")
cleaned_interactions_df.show(truncate=False)

# Step 10: Save the cleaned data to a new CSV
logger.info("Step 10: Saving the cleaned data to 'cleaned_interactions.csv'...")
# save_df_to_csv(cleaned_interactions_df, "Cleaned_data/cleaned_interactions.csv")

# Display the count of records after phone number processing
record_count_after_cleaning = cleaned_interactions_df.count()
logger.info(f"Number of records after cleaning: {record_count_after_cleaning}")
logger.info("Data cleaning and export completed successfully.")