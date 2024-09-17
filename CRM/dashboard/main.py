import streamlit as st
from pyspark.sql import SparkSession, DataFrame
from sprint2 import *
from sprint3 import *
from pyspark.sql.functions import col, sum, lit, regexp_replace, initcap, when, mean, date_format, avg, concat,max as spark_max, current_date
spark = SparkSession.builder.appName("CRM_Data_Validation").getOrCreate()
import warnings
warnings.filterwarnings('ignore')
# Function to display outputs from the PySpark script


def run_pyspark_script():
    # Function to print in green (used in the provided script)
    def print_green(text: str):
        st.write(f"<span style='color:green'>{text}</span>", unsafe_allow_html=True)

    # Step 1: Initialize Spark Session
    print_green("Step 1: Initializing Spark Session...")
    spark = SparkSession.builder.appName("CRM_Data_Validation").getOrCreate()

    # Step 2: Load data from all files into DataFrames using PySpark
    print_green("Step 2: Loading data from CSV files into DataFrames...")
    customers_df = spark.read.csv("/spark-data/CRM/rough/customers.csv", header=True, inferSchema=True)
    products_df = spark.read.csv("/spark-data/CRM/rough/products.csv", header=True, inferSchema=True)
    transactions_df = spark.read.csv("/spark-data/CRM/rough/transactions.csv", header=True, inferSchema=True)
    interactions_df = spark.read.csv("/spark-data/CRM/rough/interactions.csv", header=True, inferSchema=True)
    sales_team_df = spark.read.csv("/spark-data/CRM/rough/sales_team.csv", header=True, inferSchema=True)

    # Step 3: Initial Validation - Display records for each DataFrame
    print_green("Step 3: Displaying initial records for validation...")

    print_green("Customers DataFrame:")
    st.dataframe(customers_df.limit(5).toPandas())

    print_green("Products DataFrame:")
    st.dataframe(products_df.limit(5).toPandas())

    print_green("Transactions DataFrame:")
    st.dataframe(transactions_df.limit(5).toPandas())

    print_green("Interactions DataFrame:")
    st.dataframe(interactions_df.limit(5).toPandas())

    print_green("Sales Team DataFrame:")
    st.dataframe(sales_team_df.limit(5).toPandas())

    # Step 4: Verifying Data Accuracy

    # 4.1 Verify that Customer_ID in transactions.csv and interactions.csv matches with customers.csv
    print_green("Step 4.1: Verifying Customer_ID matches across transactions and interactions with customers...")
    validate_ids(transactions_df, customers_df, "Customer_ID", "Customer_ID", "Customer_ID")
    validate_ids(interactions_df, customers_df, "Customer_ID", "Customer_ID", "Customer_ID")

    # 4.2 Check that Product_ID in transactions.csv is valid according to products.csv
    print_green("Step 4.2: Verifying Product_ID matches between transactions.csv and products.csv...")
    validate_ids(transactions_df, products_df, "Product_ID", "Product_ID", "Product_ID")

    # 4.3 Ensure that Sales_Rep_ID in transactions.csv matches entries in sales_team.csv
    print_green("Step 4.3: Verifying Sales_Rep_ID matches between transactions.csv and sales_team.csv...")
    validate_ids(transactions_df, sales_team_df, "Sales_Rep_ID", "Sales_Rep_ID", "Sales_Rep_ID")

    print_green("Data validation completed.")

# Function to validate IDs (used in the provided script)
def validate_ids(df1, df2, df1_col, df2_col, id_name):
    missing_ids = df1.join(df2, df1[df1_col] == df2[df2_col], "left_anti") \
                     .select(df1[df1_col].alias(f"Missing_{id_name}"))

    st.write(f"<span style='color:green'>{id_name}s missing in {df2_col.split('_')[0]}.csv from {df1_col.split('_')[0]}.csv:</span>", unsafe_allow_html=True)
    st.dataframe(missing_ids.toPandas())


# Sidebar navigation with 6 different titles (including Home)
st.sidebar.title("Select a Task to View Analysis")
options = ["Home", 
           "Data Ingestion", 
           "Data Cleaning", 
           "Customer Purchase Behaviour Analysis", 
           "Product Sales Performance Evaluation", 
           "Sales Team Performance Analysis", 
           "Customer Interaction Effectiveness"]
selected_option = st.sidebar.radio("Select a task", options)

# Display page based on the selected task
if selected_option == "Home":
    st.title("CRM Data Engineering and Analysis Pipeline")
    
    # Add an image or logo
    st.image("/spark-data/CRM/rough/images/logo.png", caption="Sales Data Analysis Project", use_column_width=True)

    # Add project description or introduction
    st.write("""
        This project focuses on analyzing customer behavior, product sales performance, 
        sales team performance, and the effectiveness of customer interactions. 
        Select any task from the sidebar to view the corresponding analysis.
    """)

    st.subheader("ER Diagram")
    st.image("/spark-data/CRM/rough/images/ER.png")

    st.subheader("Architecture Diagram")
    st.image("/spark-data/CRM/rough/images/Architecture.png")
    # Add download buttons for Problem Statement PPT and Documentation


    # # Display the PDF (after converting DOCX to PDF)
    # st.markdown("### CRM Data Engineering and Analysis Pipeline Documentation")

    # # Use Google Docs viewer to preview PDF
    # st.markdown(
    #     f'<iframe src="https://docs.google.com/document/d/1IeErapsQKlD-sSZpr1cOJrAnB87WMTyK/edit?usp=sharing&ouid=104866895261391064800&rtpof=true&sd=true" '
    #     f'width="800" height="500"></iframe>',
    #     unsafe_allow_html=True
    # )

    # st.markdown("### CRM Data Engineering and Analysis Pipeline Presentation")

    #  # Use Google Docs viewer to preview PDF
    # st.markdown(
    #     f'<iframe src="https://docs.google.com/presentation/d/1K57eHjqDoWFBpkGemtfufJ51ZoHOI23X/edit?usp=sharing&ouid=104866895261391064800&rtpof=true&sd=true" '
    #     f'width="800" height="500"></iframe>',
    #     unsafe_allow_html=True
    # )


elif selected_option == "Data Ingestion":
    st.title("Data Ingestion and Initial Validation")
    run_pyspark_script()

elif selected_option == "Data Cleaning":
    st.header("Data Cleaning")
    sprint2()

elif selected_option == "Customer Purchase Behaviour Analysis":
    st.header("Customer Purchase Behaviour Analysis")
    customer_purchase()

elif selected_option == "Product Sales Performance Evaluation":
    st.header("Product Sales Performance Evaluation")
    product_sales()

elif selected_option == "Sales Team Performance Analysis":
    st.header("Sales Team Performance Analysis")
    sales_team_performance()

elif selected_option == "Customer Interaction Effectiveness":
    st.header("Customer Interaction Effectiveness Analysis")
    sprint3()
