from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum, lit, regexp_replace, initcap, when, mean, date_format, avg, concat,max as spark_max, current_date,udf,regexp_extract
import logging
import re
from email_validator import validate_email, EmailNotValidError
from pyspark.sql.types import StringType
import os
import warnings
warnings.filterwarnings('ignore')

# Step 1: Initialize Spark Session
spark = SparkSession.builder.appName("CRM_Data_Validation").getOrCreate()

import logging
import os

class CustomFormatter(logging.Formatter):
    # ANSI escape codes for colors
    GREEN = "\033[92m"
    RESET = "\033[0m"

    def format(self, record):
        # Add green color to INFO level messages
        if record.levelno == logging.INFO:
            record.msg = f"{self.GREEN}{record.msg}{self.RESET}"
        return super().format(record)

def initialize_logger(log_path):
    # Ensure the logs directory exists
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    # Initialize the logger
    logger = logging.getLogger('logger')
    logger.setLevel(logging.INFO)

    # Remove all existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create a stream handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(CustomFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    # Create a file handler with the dynamic path
    fh = logging.FileHandler(log_path)
    fh.setLevel(logging.INFO)
    fh.setFormatter(CustomFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    # Prevent logging from propagating to the root logger
    logger.propagate = False

    # Add handlers to the logger
    logger.addHandler(ch)
    logger.addHandler(fh)

    return logger

# Example usage
log_file_path = 'logs/customers_data_cleaning.log'
logger = initialize_logger(log_file_path)

def count_duplicates_per_column(df):
    """
    count_duplicates_per_column function counts the number of duplicate values for each column in the DataFrame
    and returns a DataFrame summarizing these counts.

    This function groups the DataFrame by each column and counts occurrences of each value. It then filters
    to count only those values that appear more than once, resulting in a count of duplicate values per column.

    Parameters:
    df (DataFrame): The input Spark DataFrame containing potential duplicate values in its columns.

    Returns:
    DataFrame: A Spark DataFrame where each row represents a column from the original DataFrame and the count of
               duplicate values in that column.

    Example:
    ----------
    Count duplicates in the customers_df DataFrame
    duplicate_counts_df = count_duplicates_per_column(customers_df)
    duplicate_counts_df.show()

    Output:
    ----------
    +-------------+-----------------+
    | Column      | Duplicate_Count |
    +-------------+-----------------+
    | Customer_ID | 5               |
    | Email       | 3               |
    +-------------+-----------------+
    """
    duplicate_counts = {}
    for column in df.columns:
        # Count the number of duplicate values in each column
        duplicate_count = df.groupBy(col(column)).count().filter(col("count") > 1).count()
        duplicate_counts[column] = duplicate_count
    # Convert the dictionary to a DataFrame for easier visualization
    duplicate_counts_df = spark.createDataFrame(list(duplicate_counts.items()), ["Column", "Duplicate_Count"])
    return duplicate_counts_df

def get_duplicate_data_per_column(df):
    """
    get_duplicate_data_per_column function identifies and retrieves rows with duplicate values based on each column
    in the DataFrame and returns a dictionary of DataFrames containing the duplicate rows for each column.

    This function groups the DataFrame by each column and identifies duplicates. It then joins the original DataFrame
    with these duplicate groups to extract the full rows that have duplicate values.

    Parameters:
    df (DataFrame): The input Spark DataFrame from which duplicate rows are to be identified.

    Returns:
    dict: A dictionary where the keys are column names and the values are DataFrames containing rows with duplicate
          values for that specific column.

    Example:
    ----------
    Retrieve and display duplicate rows based on each column in cleaned_customers_df
    duplicate_data = get_duplicate_data_per_column(cleaned_customers_df)
    for column, duplicates in duplicate_data.items():
        logger.info(f"Duplicate data based on column: {column}")
        duplicates.show(truncate=False)

    Output:
    ----------
    Duplicate data based on column: Customer_ID
    +-----------+-----------+---------------------+
    | Customer_ID | Name      | Email               |
    +-----------+-----------+---------------------+
    | 123       | Alice     | alice@example.com   |
    | 123       | Alice     | alice@example.com   |
    +-----------+-----------+---------------------+
    """
    duplicate_data = {}

    for column in df.columns:
        # Find duplicate rows based on the specific column
        duplicate_rows = df.groupBy(col(column)).count().filter(col("count") > 1)

        # Join the duplicate rows with the original DataFrame to get full duplicate data
        duplicates = df.join(duplicate_rows, on=column, how='inner')

        # Store the duplicates in the dictionary
        duplicate_data[column] = duplicates

        # Show duplicate data for each column
    for column, duplicates in duplicate_data.items():
        logger.info(f"Duplicate data based on column: {column}")
        duplicates.show(5,truncate=False)



def drop_duplicates(df: DataFrame, key_column: str) -> DataFrame:
    """
    This function checks for duplicate records in a specified key column,
    drops duplicates if any are found, and returns the cleaned DataFrame.

    Parameters:
    ----------
    df : DataFrame
        The input DataFrame to check for duplicates.
    key_column : str
        The column name based on which duplicates should be identified and removed.

    Returns:
    ----------
    DataFrame
        The cleaned DataFrame with duplicates removed, if any were found.


    Sample Input:
    ----------
    Input DataFrame:
    +---+-----+----+
    | ID| Name| Age|
    +---+-----+----+
    |  1| John|  25|
    |  2| Jane|  30|
    |  1| John|  25|
    |  3| Mike|  35|
    +---+-----+----+

    key_column: 'ID'

    Sample Output:
    ----------
    Cleaned DataFrame:
    +---+-----+----+
    | ID| Name| Age|
    +---+-----+----+
    |  1| John|  25|
    |  2| Jane|  30|
    |  3| Mike|  35|
    +---+-----+----+
    """
    # Count duplicates before dropping
    duplicate_count_before = df.groupBy(key_column).count().filter("count > 1").count()
    logger.info(f"Number of duplicate records before dropping: {duplicate_count_before}")

    # Drop duplicates if any are found
    if duplicate_count_before > 0:
        cleaned_df = df.dropDuplicates([key_column])
        duplicate_count_after = cleaned_df.groupBy(key_column).count().filter("count > 1").count()
        logger.info(f"Number of duplicate records after dropping: {duplicate_count_after}")
    else:
        cleaned_df = df
        logger.info("No duplicates found.")

    return cleaned_df



def validate_emails(df: DataFrame, email_column: str = "Email") -> DataFrame:
    """
    Validates the email addresses in the provided DataFrame using a regex pattern.

    Parameters:
    df (DataFrame): A DataFrame containing customer data, including an 'Email' column.
    email_column (str): The column name for the email addresses. Default is 'Email'.

    Returns:
    DataFrame: The original DataFrame with validation results logger.infoed to the console.
    """

    if email_column not in df.columns:
        raise ValueError(f"DataFrame must contain an '{email_column}' column")

    # Regex pattern for validating email format
    email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

    # UDF to validate email using regex
    def validate_email_safely(email):
        if re.match(email_regex, email):
            return None  # Valid email
        else:
            return email  # Invalid email

    validate_email_udf = udf(validate_email_safely, StringType())

    # Apply the UDF to filter invalid emails
    non_null_emails_df = df.filter(col(email_column).isNotNull())
    invalid_emails_df = non_null_emails_df.withColumn('Invalid_Email', validate_email_udf(col(email_column)))
    invalid_emails = invalid_emails_df.filter(col('Invalid_Email').isNotNull())

    if invalid_emails.count() == 0:
        logger.info("All non-null emails are valid.")
    else:
        logger.info("There are invalid emails in the dataset:")
        invalid_emails.select('Invalid_Email').show(truncate=False)
        logger.info(f"Total number of invalid emails: {invalid_emails.count()}")

    return df

def capitalize_columns(df: DataFrame, columns: list) -> DataFrame:
    """
    Capitalizes the first letter of each word in the specified columns of a DataFrame.

    Parameters:
    ----------
    df : DataFrame
        The input DataFrame whose columns need to be capitalized.
    columns : list
        A list of column names to apply the capitalization.

    Returns:
    ----------
    DataFrame
        The DataFrame with the specified columns capitalized.


    Sample Input:
    ----------
    Input DataFrame:
    +---+------------+-------------+
    | ID|  Name      |   City      |
    +---+------------+-------------+
    |  1| john doe   | new york    |
    |  2| mike brown | chicago     |
    +---+------------+-------------+

    columns: ['Name', 'City']

    Sample Output:
    ----------
    Output DataFrame:
    +---+-----------+-------------+
    | ID|    Name   |     City    |
    +---+-----------+-------------+
    |  1| John Doe  | New York    |
    |  2| Mike Brown| Chicago     |
    +---+-----------+-------------+

    """
    for column in columns:
        df = df.withColumn(column, initcap(col(column)))
    return df

def count_missing_values(df: DataFrame) -> DataFrame:
    """
    Counts the number of missing (null) values in each column of the DataFrame.

    Parameters:
    ----------
    df : DataFrame
        The input DataFrame for which missing values need to be counted.

    Returns:
    ----------
    DataFrame
        A DataFrame containing the count of missing values for each column.

    Sample Input:
    ----------
    Input DataFrame:
    +---+------+-----+
    | ID| Name | Age |
    +---+------+-----+
    |  1| John |  25 |
    |  2| null |  30 |
    |  3| Mike | null|
    |  4| null | null|
    +---+------+-----+

    Sample Output:
    ----------
    Output DataFrame:
    +---+----+---+
    | ID|Name|Age|
    +---+----+---+
    |  0|   2|  2|
    +---+----+---+

    """
    missing_values_df = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
    return missing_values_df

def fill_missing_values(df: DataFrame, fill_values: dict) -> DataFrame:
    """
    Fills missing (null) values in the specified columns with the provided default values.

    Parameters:
    ----------
    df : DataFrame
        The input DataFrame in which missing values need to be filled.
    fill_values : dict
        A dictionary where keys are column names and values are the default values
        to fill in for missing data in those columns.

    Returns:
    ----------
    DataFrame
        The DataFrame with missing values filled according to the specified default values.

    Sample Input:
    ----------
    Input DataFrame:
    +---+------+-----+
    | ID| Name | Age |
    +---+------+-----+
    |  1| John |  25 |
    |  2| null |  30 |
    |  3| Mike | null|
    |  4| null | null|
    +---+------+-----+

    fill_values: {'Name': 'Unknown', 'Age': 0}

    Sample Output:
    ----------
    Output DataFrame:
    +---+--------+---+
    | ID|  Name  |Age|
    +---+--------+---+
    |  1|  John  | 25|
    |  2| Unknown| 30|
    |  3|  Mike  |  0|
    |  4| Unknown|  0|
    +---+--------+---+
    """
    filled_df = df.fillna(fill_values)
    return filled_df

def process_phone_numbers(customers_df, country_codes_path):
    """
    Processes customer phone numbers by cleaning and formatting them, and appends the appropriate country code.

    Parameters:
    - customers_df: Spark DataFrame containing customer data with columns including 'Phone' and 'Country'.
    - country_codes_path: Path to the CSV file containing country codes with columns 'Country' and 'Country_Code'.

    Returns:
    - Spark DataFrame with cleaned phone numbers and appended country codes.


    Sample Input:
    ----------
    customers_df:
    +----------+------------+----------------+
    |CustomerID|   Country   |   Phone       |
    +----------+------------+----------------+
    |    1     |   USA       |(123) 456-7890 |
    |    2     |   UK        | 123-456-7890  |
    |    3     |   India     | +91-9876543210|
    +----------+------------+----------------+

    country_codes.csv:
    +---------+--------------+
    | Country | Country_Code |
    +---------+--------------+
    |  USA    |   +1         |
    |  UK     |   +44        |
    |  India  |   +91        |
    +---------+--------------+

    Sample Output:
    ----------
    +----------+------------+---------------+
    |CustomerID|   Country  |   Phone       |
    +----------+------------+---------------+
    |    1     |   USA      |+1-1234567890  |
    |    2     |   UK       |+44-1234567890 |
    |    3     |   India    |+91-9876543210 |
    +----------+------------+---------------+
    """

    # Step 1: Load country codes data from CSV
    country_codes_df = spark.read.csv(country_codes_path, header=True, inferSchema=True)

    # Step 2: Identify and remove the '+1-' prefix from phone numbers
    cleaned_df = customers_df.withColumn(
        "Processed_Phone",
        when(col("Phone").startswith("+1-"), regexp_replace(col("Phone"), r"^\+1-", ""))
        .otherwise(col("Phone"))
    )

    # Step 3: Remove everything after 'x' or similar extensions
    cleaned_df = cleaned_df.withColumn(
        "Processed_Phone",
        regexp_replace(col("Processed_Phone"), r"x.*", "")
    )

    # Step 4: Remove non-digit characters to retain only numeric part of the phone number
    final_cleaned_df = cleaned_df.withColumn(
        "Final_Phone_Number",
        regexp_replace(col("Processed_Phone"), r"[^0-9]", "")
    )

    # Step 5: Join with the Country Codes DataFrame to add the Country Code
    final_df = final_cleaned_df.join(country_codes_df, "Country", "left")

    # Step 6: Append the country code to the cleaned phone number
    final_df = final_df.withColumn(
        "Phone",
        when(col("Country_Code").isNotNull() & col("Final_Phone_Number").isNotNull(),
             concat(col("Country_Code"), lit("-"), col("Final_Phone_Number"))
        ).otherwise(col("Phone"))
    )

    # Select the final columns to include in the output DataFrame
    final_df = final_df.select(
        "Customer_ID",
        "Name",
        "Email",
        "Phone",
        "Country"
    )

    # Return the final DataFrame with the cleaned and formatted phone numbers
    return final_df



def date_validation(df: DataFrame,  column: str) -> DataFrame:
    """
    This function identifies and corrects future dates in the 'Date' column of a DataFrame.
    If future dates are found, they are replaced with the most recent valid past date in the dataset.

    Parameters:
    df (DataFrame): The input DataFrame containing the 'Date' column.

    Returns:
    DataFrame: The DataFrame with future dates corrected.

    Sample Input:
    ----------
    Input DataFrame:
    +---+----------+
    | ID|   Date   |
    +---+----------+
    |  1|2024-08-25|
    |  2|2025-01-01|  # Future date
    |  3|2023-12-15|
    |  4|2024-07-10|
    +---+----------+

    Sample Output:
    ----------
    Output DataFrame:
    +---+----------+
    | ID|   Date   |
    +---+----------+
    |  1|2024-08-25|
    |  2|2024-08-25|  # Corrected to most recent past date
    |  3|2023-12-15|
    |  4|2024-07-10|
    +---+----------+

    """

    # Step 1: Identify rows where 'Interaction_Date' is in the future
    logger.info("Step 1: Identifying future dates in 'Interaction_Date'...")
    future_dates_df = df.filter(col(column) > current_date())

    # Check if there are any future dates
    if future_dates_df.count() > 0:
        logger.info(f"Found {future_dates_df.count()} records with future dates.")

        # Step 2: Determine the most recent valid date in the dataset
        most_recent_past_date = df.filter(col(column) <= current_date()) \
                                  .agg(spark_max(column)) \
                                  .collect()[0][0]

        logger.info(f"The most recent valid past date is: {most_recent_past_date}")

        # Step 3: Replace future dates with the most recent valid past date
        logger.info("Step 3: Replacing future dates with the most recent valid past date...")
        df = df.withColumn(
            column,
            when(col(column) > current_date(), most_recent_past_date)
            .otherwise(col(column))
        )

        logger.info("Future dates have been corrected.")
    else:
        logger.info("No future dates found.")

    return df

def validate_boolean_values(df: DataFrame, column: str) -> DataFrame:
    """
    This function validates the 'boolean' column to ensure it contains only boolean values.
    Non-boolean values will be corrected to False by default.

    Parameters:
    df (DataFrame): The input DataFrame containing the 'Issue_Resolved' column.

    Returns:
    DataFrame: The DataFrame with invalid 'Issue_Resolved' values corrected.

    Sample Input:
    ----------
    Input DataFrame:
    +---+--------------+
    | ID|Issue_Resolved|
    +---+--------------+
    |  1|     True     |
    |  2|     False    |
    |  3|     Yes      |  # Non-boolean value
    |  4|     1        |  # Non-boolean value
    +---+--------------+

    Sample Output:
    ----------
    Output DataFrame:
    +---+--------------+
    | ID|Issue_Resolved|
    +---+--------------+
    |  1|     True     |
    |  2|     False    |
    |  3|     False    |  # Corrected to False
    |  4|     False    |  # Corrected to False
    +---+--------------+
    """

    # Step 1: Identify non-boolean values in 'Issue_Resolved'
    logger.info("Step 1: Identifying non-boolean values.. ")

    # Assuming the column should contain only True or False values
    invalid_values_df = df.filter(~col(column).isin(True, False))

    if invalid_values_df.count() > 0:
        logger.info(f"Found {invalid_values_df.count()} records with non-boolean values in 'Issue_Resolved'.")

        # Step 2: Correct non-boolean values to False
        logger.info("Step 2: Correcting non-boolean values to False...")
        df = df.withColumn(
            column,
            when(col(column).isin(True, False), col(column))
            .otherwise(lit(False))
        )

        logger.info("Non-boolean values have been corrected to False.")
    else:
        logger.info("All values in 'Issue_Resolved' are valid booleans.")

    return df


def save_df_to_csv(df, file_path):
    """
    Saves a Spark DataFrame to a CSV file.

    Parameters:
    - df: Spark DataFrame to be saved.
    - file_path: Path where the CSV file will be saved.

    Returns:
    - None
    """
    # Save DataFrame to CSV
    df.toPandas().to_csv(file_path, index=False)