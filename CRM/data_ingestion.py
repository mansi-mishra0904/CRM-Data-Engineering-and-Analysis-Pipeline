import logging
from pyspark.sql import SparkSession, DataFrame

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

# Initialize Spark Session
spark = SparkSession.builder.appName("CRM_Data_Validation").getOrCreate()

# Load data from CSV files into DataFrames
data_files = {
    "customers": "Dataset/customers.csv",
    "products": "Dataset/products.csv",
    "transactions": "Dataset/transactions.csv",
    "interactions": "Dataset/interactions.csv",
    "sales_team": "Dataset/sales_team.csv"
}

dfs = {name: spark.read.csv(file, header=True, inferSchema=True) for name, file in data_files.items()}

# Display initial records for each DataFrame
for name, df in dfs.items():
    print_green(f"{name.capitalize()} DataFrame:")
    df.show(5, truncate=False)
    logger.info(f"Displayed first 5 records for {name.capitalize()} DataFrame.")

# Verify Data Accuracy
validations = [
    ("transactions", "customers", "Customer_ID"),
    ("interactions", "customers", "Customer_ID"),
    ("transactions", "products", "Product_ID"),
    ("transactions", "sales_team", "Sales_Rep_ID")
]

for df1_name, df2_name, id_name in validations:
    print_green(f"Verifying {id_name} matches between {df1_name}.csv and {df2_name}.csv...")
    logger.info(f"Starting validation of {id_name} between {df1_name}.csv and {df2_name}.csv.")
    validate_ids(dfs[df1_name], dfs[df2_name], id_name, id_name, id_name)

print_green("Data validation completed.")
logger.info("Data validation completed.")
