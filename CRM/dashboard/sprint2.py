import streamlit as st

from pyspark.sql import functions as F
from scipy.stats import linregress
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, sum as _sum, avg, count, round, to_date, date_format
import pandas as pd
from bar_plot_with_line import *
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from common_utility import *
from matplotlib.cm import get_cmap
import matplotlib.patches as patches
import warnings
warnings.filterwarnings('ignore')

from pyspark.sql.functions import col, lit, when, mean, date_format, avg
spark = SparkSession.builder.appName("CRM_Data_Validation").getOrCreate()

def print_green(text: str):
    st.write(f"<h3><b><span style='color:green'>{text}</span></b></h3>", unsafe_allow_html=True)

def sprint2():
    # Load data from all files into DataFrames using PySpark
    st.subheader("Data preprocessing of Customers Table ")
    customers_df = spark.read.csv("/spark-data/CRM/rough/customers.csv", header=True, inferSchema=True)
    
    # Data Cleaning for Customers Table
    print_green("Step 1: Counting missing values in each column before filling them...")
    missing_values_before = count_missing_values(customers_df)
    st.write("Missing Values Before Cleaning:")
    st.dataframe(missing_values_before.toPandas())

    print_green("Step 2: Checking for duplicates in each column before dropping them...")
    duplicate_count = count_duplicates_per_column(customers_df)
    st.write("Duplicate Counts Before Cleaning:")
    st.dataframe(duplicate_count.toPandas())

    print_green("Step 3: Formatting and cleaning phone numbers...")
    cleaned_customers_df = process_phone_numbers(customers_df, "/spark-data/CRM/rough/countries.csv")
    st.write("Cleaned Customers Data:")
    st.dataframe(cleaned_customers_df.limit(5).toPandas())  # Show a few rows for checking

    print_green("Step 4: Checking and removing duplicate records based on 'Customer_ID'...")
    cleaned_customers_df = drop_duplicates(cleaned_customers_df, "Customer_ID")

    print_green("Step 5: Filling missing values in 'Email' and 'Phone' columns...")
    cleaned_customers_df = fill_missing_values(cleaned_customers_df, {'Email': 'unknown', 'Phone': 'unknown'})
    st.write("Customers Data After Filling Missing Values:")
    st.dataframe(cleaned_customers_df.limit(5).toPandas())  # Show a few rows for checking

    print_green("Step 6: Capitalizing the first letter of each word in the 'Name' and 'Country' columns...")
    cleaned_customers_df = capitalize_columns(cleaned_customers_df, ["Name", "Country"])

    print_green("Step 7: Counting missing values in each column after filling them...")
    missing_values_after = count_missing_values(cleaned_customers_df)
    st.write("Missing Values After Cleaning:")
    st.dataframe(missing_values_after.toPandas())

    print_green("Step 8: Checking for duplicates in each column after dropping them...")
    duplicate_count_after = count_duplicates_per_column(cleaned_customers_df)
    st.write("Duplicate Counts After Cleaning:")
    st.dataframe(duplicate_count_after.toPandas())

    print_green("Step 9: Exporting the cleaned data to 'cleaned_customers.csv'...")
    # save_df_to_csv(cleaned_customers_df, "Cleaned_data/cleaned_customers.csv")

    # Display the count of records after phone number processing
    record_count_after_cleaning = cleaned_customers_df.count()
    st.write(f"Number of records after cleaning: {record_count_after_cleaning}")

    print_green("Data cleaning and export completed successfully.")



    # Load data from all files into DataFrames using PySpark
    st.subheader("Data preprocessing of Products Table")
    products_df = spark.read.csv("products.csv", header=True, inferSchema=True)
    
    # Data Cleaning for Products Table
    print_green("Step 1: Counting missing values in each column before filling them...")
    missing_values_before = count_missing_values(products_df)
    st.write("Missing Values Before Cleaning:")
    st.dataframe(missing_values_before.toPandas())

    print_green("Step 2: Checking for duplicates in each column before dropping them...")
    duplicate_count = count_duplicates_per_column(products_df)
    st.write("Duplicate Counts Before Cleaning:")
    st.dataframe(duplicate_count.toPandas())

    print_green("Step 3: Checking and removing duplicate records based on 'Product_ID'...")
    cleaned_products_df = drop_duplicates(products_df, "Product_ID")

    print_green("Step 4: Filling missing values in 'Category' column...")
    cleaned_products_df = fill_missing_values(cleaned_products_df, {"Category": "Uncategorized"})
    st.write("Products Data After Filling Missing Values:")
    st.dataframe(cleaned_products_df.limit(5).toPandas())  # Show a few rows for checking

    print_green("Step 5: Capitalizing the first letter of each word in the 'Product_Name' and 'Category' columns...")
    cleaned_products_df = capitalize_columns(cleaned_products_df, ["Product_Name", "Category"])

    print_green("Step 6: Counting missing values in each column after filling them...")
    missing_values_after = count_missing_values(cleaned_products_df)
    st.write("Missing Values After Cleaning:")
    st.dataframe(missing_values_after.toPandas())

    print_green("Step 7: Checking for duplicates in each column after dropping them...")
    duplicate_count_after = count_duplicates_per_column(cleaned_products_df)
    st.write("Duplicate Counts After Cleaning:")
    st.dataframe(duplicate_count_after.toPandas())

    print_green("Step 8: Replacing negative or zero prices with the average price...")
    avg_price = products_df.agg({"Price": "avg"}).collect()[0][0]
    cleaned_products_df = cleaned_products_df.withColumn("Price", when(col("Price") <= 0, avg_price).otherwise(col("Price")))
    st.write("Products Data After Price Adjustment:")
    st.dataframe(cleaned_products_df.limit(5).toPandas())  # Show a few rows for checking

    print_green("Step 9: Exporting the cleaned data to 'cleaned_products.csv'...")
    # save_df_to_csv(cleaned_products_df, "Cleaned_data/cleaned_products.csv")

    # Display the count of records after cleaning
    record_count_after_cleaning = cleaned_products_df.count()
    st.write(f"Number of records after cleaning: {record_count_after_cleaning}")

    print_green("Data cleaning and export completed successfully.")


     # Load data from transactions CSV file
    st.subheader("Data Preprocessing for transactions table..")
    transactions_df = spark.read.csv("transactions.csv", header=True, inferSchema=True)
    
    # Data Cleaning for Transactions Table
    print_green("Step 1: Counting missing values in each column before filling them...")
    missing_values_before = count_missing_values(transactions_df)
    st.write("Missing Values Before Cleaning:")
    st.dataframe(missing_values_before.toPandas())

    print_green("Step 2: Calculating the mean value of the 'Amount' column...")
    mean_amount_row = transactions_df.agg(mean("Amount")).collect()[0]
    mean_amount = mean_amount_row[0]
    st.write(f"Mean value calculated for 'Amount': {mean_amount:.2f}")

    print_green("Step 3: Filling missing values in the 'Amount' column with the mean value...")
    cleaned_transactions_df = fill_missing_values(transactions_df, {"Amount": mean_amount})
    st.write("Transactions Data After Filling Missing Values:")
    st.dataframe(cleaned_transactions_df.limit(5).toPandas())

    print_green("Step 4: Checking for duplicate records based on 'Transaction_ID'...")
    cleaned_transactions_df = drop_duplicates(cleaned_transactions_df, "Transaction_ID")

    print_green("Step 5: Ensuring 'Date' column is in YYYY-MM-DD format...")
    cleaned_transactions_df = cleaned_transactions_df.withColumn(
        "Date", date_format(col("Date"), "yyyy-MM-dd")
    )

    print_green("Step 6: Validating dates present in 'Date' column...")
    cleaned_transactions_df = date_validation(cleaned_transactions_df, "Date")
    st.write("Transactions Data After Date Validation:")
    st.dataframe(cleaned_transactions_df.limit(5).toPandas())

    print_green("Step 7: Counting missing values in each column after filling them...")
    missing_values_after = count_missing_values(cleaned_transactions_df)
    st.write("Missing Values After Cleaning:")
    st.dataframe(missing_values_after.toPandas())

    print_green("Step 8: Correcting non-positive values in the 'Amount' column...")
    corrected_transactions_df = cleaned_transactions_df.withColumn(
        "Amount",
        when(col("Amount") <= 0, mean_amount).otherwise(col("Amount"))
    )
    st.write("Transactions Data After Correcting 'Amount' Column:")
    st.dataframe(corrected_transactions_df.limit(5).toPandas())

    print_green("Step 9: Displaying the cleaned 'transactions_df' DataFrame...")
    st.write("Final Cleaned Transactions Data:")
    st.dataframe(corrected_transactions_df.limit(10).toPandas())

    print_green("Step 10: Saving the cleaned data to 'cleaned_transactions.csv'...")
    # save_df_to_csv(corrected_transactions_df, "Cleaned_data/cleaned_transactions.csv")

    # Display the count of records after cleaning
    record_count_after_cleaning = cleaned_transactions_df.count()
    st.write(f"Number of records after cleaning: {record_count_after_cleaning}")

    print_green("Data cleaning and export completed successfully.")



    # Load data from interactions CSV file
    st.subheader("Data Preprocessing of Interactions Table")
    interactions_df = spark.read.csv("interactions.csv", header=True, inferSchema=True)
    
    # Data Cleaning for Interactions Table
    print_green("Step 1: Identifying missing values in each column before filling them...")
    missing_values_before = count_missing_values(interactions_df)
    st.write("Missing Values Before Cleaning:")
    st.dataframe(missing_values_before.toPandas())

    print_green("Step 2: Counting occurrences of each 'Interaction_Type' to find the most occurring type...")
    type_counts = interactions_df.groupBy("Interaction_Type").count().orderBy(col("count").desc()).first()

    most_occurring_type = type_counts["Interaction_Type"] if type_counts else None
    st.write(f"The most occurring 'Interaction_Type' is: {most_occurring_type}")

    print_green("Step 3: Replacing null values in 'Interaction_Type' with the most occurring type...")
    cleaned_interactions_df = interactions_df.withColumn(
        "Interaction_Type",
        when(col("Interaction_Type").isNull(), lit(most_occurring_type))
        .otherwise(col("Interaction_Type"))
    )
    st.write("Interactions Data After Replacing Nulls in 'Interaction_Type':")
    st.dataframe(cleaned_interactions_df.limit(5).toPandas())

    print_green("Step 4: Checking for duplicate records based on 'Interaction_ID'...")
    cleaned_interactions_df = drop_duplicates(cleaned_interactions_df, "Interaction_ID")

    print_green("Step 5: Capitalizing the first letter of the values in the 'Interaction_Type' columns...")
    cleaned_interactions_df = capitalize_columns(cleaned_interactions_df, ["Interaction_Type"])
    st.write("Interactions Data After Capitalizing 'Interaction_Type':")
    st.dataframe(cleaned_interactions_df.limit(5).toPandas())

    print_green("Step 6: Validating dates present in 'Interaction_Date' column...")
    cleaned_interactions_df = date_validation(cleaned_interactions_df, "Interaction_Date")
    st.write("Interactions Data After Date Validation:")
    st.dataframe(cleaned_interactions_df.limit(5).toPandas())

    print_green("Step 7: Validating booleans present in 'Issue_Resolved' column...")
    cleaned_interactions_df = validate_boolean_values(cleaned_interactions_df, "Issue_Resolved")
    st.write("Interactions Data After Boolean Validation:")
    st.dataframe(cleaned_interactions_df.limit(5).toPandas())

    print_green("Step 8: Identifying missing values in each column after filling them...")
    missing_values_after = count_missing_values(cleaned_interactions_df)
    st.write("Missing Values After Cleaning:")
    st.dataframe(missing_values_after.toPandas())

    print_green("Step 9: Displaying the cleaned 'interactions_df' DataFrame...")
    st.write("Final Cleaned Interactions Data:")
    st.dataframe(cleaned_interactions_df.limit(10).toPandas())

    print_green("Step 10: Saving the cleaned data to 'cleaned_interactions.csv'...")
    # save_df_to_csv(cleaned_interactions_df, "Cleaned_data/cleaned_interactions.csv")

    # Display the count of records after cleaning
    record_count_after_cleaning = cleaned_interactions_df.count()
    st.write(f"Number of records after cleaning: {record_count_after_cleaning}")

    print_green("Data cleaning and export completed successfully.")

    # Load data from sales_team CSV file
    st.subheader("Data Preprocessing of Sales Team Table")
    sales_team_df = spark.read.csv("sales_team.csv", header=True, inferSchema=True)
    
    # Data Cleaning for Sales Team Table
    print_green("Step 1: Identifying missing values in each column before filling them...")
    missing_values_before = count_missing_values(sales_team_df)
    st.write("Missing Values Before Cleaning:")
    st.dataframe(missing_values_before.toPandas())

    print_green("Step 2: Handling missing values by filling with averages...")
    avg_sales_achieved = sales_team_df.select(avg(col("Sales_Achieved"))).first()[0]
    st.write(f"Average value of 'Sales_Achieved' column: {avg_sales_achieved:.2f}")

    sales_team_cleaned_df = fill_missing_values(sales_team_df, {"Sales_Achieved": avg_sales_achieved})
    st.write("Sales Team Data After Filling Missing 'Sales_Achieved':")
    st.dataframe(sales_team_cleaned_df.limit(5).toPandas())

    print_green("Step 3: Checking for duplicate records based on 'Sales_Rep_ID'...")
    duplicate_count_before = count_duplicates_per_column(sales_team_cleaned_df)
    st.write("Duplicate Count Before Dropping:")
    st.dataframe(duplicate_count_before.toPandas())

    print_green("Step 4: Dropping duplicates based on 'Sales_Rep_ID' if they exist...")
    sales_team_cleaned_df = drop_duplicates(sales_team_cleaned_df, "Sales_Rep_ID")

    print_green("Step 5: Standardizing the format of 'Name' and 'Region' columns...")
    sales_team_cleaned_df = capitalize_columns(sales_team_cleaned_df, ["Name", "Region"])
    st.write("Sales Team Data After Standardizing 'Name' and 'Region':")
    st.dataframe(sales_team_cleaned_df.limit(5).toPandas())

    print_green("Step 6: Identifying missing values in each column after filling them...")
    missing_values_after = count_missing_values(sales_team_cleaned_df)
    st.write("Missing Values After Cleaning:")
    st.dataframe(missing_values_after.toPandas())

    print_green("Step 7: Displaying the cleaned 'sales_team_df' DataFrame...")
    st.write("Final Cleaned Sales Team Data:")
    st.dataframe(sales_team_cleaned_df.limit(10).toPandas())

    print_green("Step 8: Saving the cleaned data to 'cleaned_sales_team.csv'...")
    # save_df_to_csv(sales_team_cleaned_df, "Cleaned_data/cleaned_sales_team.csv")

    # Display the count of records after cleaning
    record_count_after_cleaning = sales_team_cleaned_df.count()
    st.write(f"Number of records after cleaning: {record_count_after_cleaning}")

    print_green("Data cleaning and export completed successfully.")


def customer_purchase():
# Load data into Spark DataFrames
    customers_df = spark.read.csv("Cleaned_data/cleaned_customers.csv", header=True, inferSchema=True)
    transactions_df = spark.read.csv("Cleaned_data/cleaned_transactions.csv", header=True, inferSchema=True)
    products_df = spark.read.csv("Cleaned_data/cleaned_products.csv", header=True, inferSchema=True)

    # Validate the data
    customers_df.show(5,truncate=False)
    transactions_df.show(5,truncate=False)
    products_df.show(5,truncate=False)
    ## 1. Total Spending, Average Transaction Amount, and Purchase Frequency
    # Calculate total spending, average transaction amount, and purchase frequency per customer

    # Aggregate transaction data to compute key metrics for each customer
    customer_spending_df = transactions_df.groupBy("Customer_ID") \
    .agg(
    _sum("Amount").alias("Total_Spending"),  # Calculate the total spending by summing up the 'Amount' column
    round(avg("Amount"), 2).alias("Average_Transaction_Amount"),  # Compute the average transaction amount, rounded to 2 decimal points for precision
    count("Transaction_ID").alias("Purchase_Frequency")  # Count the number of transactions to determine purchase frequency
    )

    # Combine customer spending metrics with customer details to facilitate country-based segmentation and analysis
    customer_behavior_df = customer_spending_df.join(customers_df, on="Customer_ID", how="inner")

    # Select relevant columns to display the combined customer spending metrics and segmentation data
    print_green("Total Spending, Average Transaction Amount, and Purchase Frequency of Each Customer")
    st.dataframe(customer_behavior_df.select(["Customer_ID", "Name", "Total_Spending", "Average_Transaction_Amount", "Purchase_Frequency", "Country"]).limit(5).toPandas())

    ## 1. Top 20 most active customers based on purchase frequency
    # Join the customer spending data with the customer details to get names and countries
    # This step merges customer spending behavior data with customer details to enrich the dataset with customer names and countries.
    customer_details_df = customers_df.select("Customer_ID", "Name", "Country")
    customer_behavior_df = customer_spending_df.join(customer_details_df, on="Customer_ID", how="inner")

    # Identify top 10 most active customers based on purchase frequency
    # Sorting the merged dataset by purchase frequency to identify the top 10 customers who have made the most purchases.
    top_active_customers_df = customer_behavior_df.orderBy(col("Purchase_Frequency").desc()).limit(20)
    top_active_customers_pd_df = top_active_customers_df.toPandas()
    print_green("Top Active Customers Based on Their Purchase Frequency")
    st.dataframe(top_active_customers_pd_df.head(5))

    # Define a colormap for shades of blue and reverse it
    # Generate a color map for visual differentiation based on country, using shades of blue to represent different countries.
    cmap = get_cmap('Greens')
    unique_countries = top_active_customers_pd_df['Country'].unique()
    colors = cmap(np.linspace(0, 1, len(unique_countries)))[::-1]  # Reverse the colormap to ensure the darkest shade is used first
    color_dict = dict(zip(unique_countries, colors))

    # Map the colors to the DataFrame based on country
    # Add a new column to the DataFrame to assign a color to each customer based on their country for visualization purposes.
    top_active_customers_pd_df['Color'] = top_active_customers_pd_df['Country'].map(color_dict)

    # Create the bar chart with line plot for Purchase Frequency
    # Initialize the figure and axis for plotting.
    fig, ax1 = plt.subplots(figsize=(14, 5))

    # Bar plot for Purchase Frequency
    # Plot a bar chart to visualize the purchase frequency of the top 10 customers, with colors representing their countries.
    bars = ax1.bar(
    top_active_customers_pd_df['Name'],
    top_active_customers_pd_df['Purchase_Frequency'],
    color=top_active_customers_pd_df['Color'],
    label='Purchase Frequency',
    edgecolor='black'
    )

    # Create a second y-axis for Average Transaction Amount
    # Add a second y-axis to the same plot to display the average transaction amount alongside the purchase frequency.
    ax2 = ax1.twinx()
    lines = ax2.plot(
    top_active_customers_pd_df['Name'],
    top_active_customers_pd_df['Average_Transaction_Amount'],
    color='orange',
    marker='o',
    linestyle='-',
    linewidth=2,
    markersize=8,
    label='Avg Transaction Amount'
    )

    # Add labels on top of bars with integer values
    # Annotate each bar with its height value to provide clear information on the purchase frequency for each customer.
    for bar in bars:
        yval = bar.get_height()
        ax1.text(
        bar.get_x() + bar.get_width() / 2.0,
        yval + 0.05 * max(top_active_customers_pd_df['Purchase_Frequency']),
        f'{int(yval)}',  # Display the purchase frequency as an integer
        ha='center',
        va='bottom'
        )

    # Customize the layout
    # Set labels, titles, and gridlines to enhance the readability and presentation of the chart.
    ax1.set_xlabel('Customer Name')
    ax1.set_ylabel('Purchase Frequency')
    ax1.set_title('Top 20 Frequent Customers by Purchase Behavior', fontweight='bold')
    ax1.set_xticklabels(top_active_customers_pd_df['Name'], rotation=45, ha='right')
    ax1.grid(axis='y', linestyle='--', alpha=0.7)
    ax2.set_ylabel('Average Transaction Amount', color='orange')

    # Create a custom legend
    # Add a legend to the plot to identify the colors representing different countries.
    legend_patches = [patches.Patch(color=color_dict[country], label=country) for country in unique_countries]
    ax1.legend(
    handles=legend_patches,
    title='Country',
    bbox_to_anchor=(1.05, 1),  # Move legend to the right side of the plot
    loc='upper left',
    title_fontsize='13',
    fontsize='10'
    )
    ax2.legend(loc='upper right')

    # Adjust layout to make space for the legend
    # Ensure the plot is adjusted to accommodate the legend and labels without overlapping.
    plt.tight_layout(rect=[0, 0, 0.8, 1])  # Adjust right space for legend
    plt.show()
    st.pyplot(fig)

    # Conclusion and suggestions
    st.subheader("Conclusion and Suggestions")
    st.write(
        """
        **Conclusion:** 
        The analysis of the top 20 most active customers shows a diverse range of purchase frequencies and average transaction amounts. Customers from different countries display varying spending patterns.

        **Suggestions:** 
        - **Targeted Marketing:** Focus marketing efforts on the top active customers to increase their purchase frequency. Consider personalized promotions for customers from specific countries to boost engagement.
        - **Loyalty Programs:** Implement or enhance loyalty programs to reward frequent purchasers, potentially increasing their average transaction amount.
        - **Regional Strategies:** Develop region-specific strategies based on the spending behavior and preferences observed in different countries.
        """
    )

    ## 2. Top 20 spending customers with Average Transaction Amount
    # Aggregate spending data to calculate total spending and average transaction amount per customer
    customer_spending_df = transactions_df.groupBy("Customer_ID") \
    .agg(
    _sum("Amount").alias("Total_Spending"),
    avg("Amount").alias("Average_Transaction_Amount")
    )

    # Join the aggregated spending data with customer details to enrich the dataset
    customer_details_df = customers_df.select("Customer_ID", "Name", "Country")
    customer_spending_with_details_df = customer_spending_df.join(
    customer_details_df,
    on="Customer_ID",
    how="inner"
    )

    # Identify top 20 customers based on total spending
    top_customers_df = customer_spending_with_details_df.orderBy(col("Total_Spending").desc()).limit(20)
    top_customers_pd_df = top_customers_df.toPandas()
    print_green( "Top Customers Based on Their Total Spending" )
    st.dataframe(top_customers_pd_df.head(5))


    # Define a colormap for shades of blue and reverse it
    cmap = get_cmap('Greens')
    unique_countries = top_customers_pd_df['Country'].unique()
    colors = cmap(np.linspace(0, 1, len(unique_countries)))[::-1]  # Reverse the colormap for better contrast
    color_dict = dict(zip(unique_countries, colors))

    # Map the colors to the DataFrame based on country
    top_customers_pd_df['Color'] = top_customers_pd_df['Country'].map(color_dict)

    # Create the bar chart with line plot for Average Transaction Amount
    fig, ax1 = plt.subplots(figsize=(14, 5))

    # Bar plot for Total Spending
    bars = ax1.bar(
    top_customers_pd_df['Name'],
    top_customers_pd_df['Total_Spending'],
    color=top_customers_pd_df['Color'],
    label='Total Spending',
    edgecolor='black'
    )

    # Create a second y-axis for Average Transaction Amount
    ax2 = ax1.twinx()
    lines = ax2.plot(
    top_customers_pd_df['Name'],
    top_customers_pd_df['Average_Transaction_Amount'],
    color='orange',
    marker='o', 
    linestyle='-',
    linewidth=2,
    markersize=8,
    label='Avg Transaction Amount'
    )

    # Add labels on top of bars with a slight tilt
    for bar in bars:
        yval = bar.get_height()
        ax1.text(
        bar.get_x() + bar.get_width() / 2.0,
        yval + 0.05 * max(top_customers_pd_df['Total_Spending']),
        f'{yval:.2f}',
        ha='center',
        va='bottom',
        fontsize=10,
        rotation=45  # Tilt the label by 45 degrees
        )

    # Customize the layout
    ax1.set_xlabel('Customer Name')
    ax1.set_ylabel('Total Spending')
    ax1.set_title('Top 20 Customers by Their Total Spending', fontweight='bold')
    ax1.set_xticklabels(top_customers_pd_df['Name'], rotation=45, ha='right')
    ax1.grid(axis='y', linestyle='--', alpha=0.7)

    ax2.set_ylabel('Average Transaction Amount', color='orange')

    # Create a custom legend
    legend_patches = [patches.Patch(color=color_dict[country], label=country) for country in unique_countries]
    ax1.legend(
    handles=legend_patches,
    title='Country',
    bbox_to_anchor=(1.05, 1),  # Move legend to the right side of the plot
    loc='upper left',
    title_fontsize='13',
    fontsize='10'
    )
    ax2.legend(loc='upper right')

    # Adjust layout to make space for the legend
    plt.tight_layout(rect=[0, 0, 0.8, 1])  # Adjust right space for legend

    # Show the plot
    plt.show()
    st.pyplot(fig)

     # Conclusion and suggestions
    st.subheader("Conclusion and Suggestions")
    st.write(
        """
        **Conclusion:** 
        The top 20 customers based on total spending reveal significant spending patterns. There is also a variance in average transaction amounts among these top spenders.

        **Suggestions:** 
        - **Exclusive Offers:** Offer exclusive deals or products to the top spending customers to further increase their engagement and spending.
        - **Segmentation:** Analyze further to segment these high spenders into different categories based on their transaction behaviors.
        - **Personalized Communication:** Develop personalized communication strategies for these high-value customers to enhance retention and drive repeat purchases.
        """
    )

    ## 3. Spending Distribution by Country
    # Aggregate spending by country and round both the total spending and average transaction amount to 2 decimal points
    # This step summarizes the spending data by country, providing rounded figures for easier interpretation and consistency.
    country_spending_df = customer_behavior_df.groupBy("Country") \
    .agg(
    round(_sum("Total_Spending"), 2).alias("Total_Spending_By_Country"),  # Calculate and round total spending per country
    round(avg("Average_Transaction_Amount"), 2).alias("Avg_Transaction_Amount_By_Country"),  # Calculate and round average transaction amount per country
    count("Customer_ID").alias("Customer_Count_By_Country")  # Count the number of customers per country
    )

    # Display the results
    # Order by total spending in descending order to see which countries have the highest total spending.
    # The results are then shown to provide a clear view of spending distribution by country.
    print_green("Aggregated Spending Data by Country: Total Spending, Avg Transaction Amount, Customer Count")
    country_spending_df = country_spending_df.orderBy(col("Total_Spending_By_Country").desc())
    country_spending_df.orderBy(col("Customer_Count_By_Country").desc()).show()

    # 4. Top 20 Countries by Number of Customers, Total Spending, and Avg Transaction Amount
    # Convert Spark DataFrame to Pandas DataFrame for plotting
    # Converting to Pandas DataFrame for ease of use with Matplotlib for visualization.
    country_spending_pd_df = country_spending_df.orderBy(col("Customer_Count_By_Country").desc()).toPandas()
    st.dataframe(country_spending_pd_df.head(5))

    # Sort by Customer_Count_By_Country in descending order and get the top 20 countries
    # Sorting data to focus on the top 20 countries with the highest customer counts for detailed analysis.
    top_countries_df = country_spending_pd_df.sort_values(by="Customer_Count_By_Country", ascending=False).head(20)

    # Define a colormap for shades of blue
    # Setting up a colormap to visually differentiate countries using a gradient of blue shades.
    cmap = get_cmap('Greens')
    # Create a color map for countries
    num_countries = len(top_countries_df)
    colors = cmap(np.linspace(0, 1, num_countries))[::-1]  # Use a range of shades for better visual distinction
    color_dict = dict(zip(top_countries_df['Country'], colors))

    # Plot the top 20 countries with the maximum number of customers
    fig, ax1 = plt.subplots(figsize=(14, 8))

    # Bar plot for Customer Count
    # Creating a bar plot to show the number of customers for each country. Bars are colored based on the colormap.
    bars = ax1.bar(
    top_countries_df['Country'],
    top_countries_df['Customer_Count_By_Country'],
    color=[color_dict[country] for country in top_countries_df['Country']],
    edgecolor='black'
    )

    # Add labels on top of bars
    # Adding data labels on top of the bars to provide exact customer counts for clarity.
    for bar in bars:
        yval = bar.get_height()
        ax1.text(
        bar.get_x() + bar.get_width() / 2.0,
        yval + 0.05 * max(top_countries_df['Customer_Count_By_Country']),
        f'{yval:.0f}',
        ha='center',
        va='bottom'
        )

    # Create a second y-axis for Total Spending
    # Adding a secondary y-axis to plot total spending data alongside customer counts.
    ax2 = ax1.twinx()
    lines_total_spending = ax2.plot(
    top_countries_df['Country'],
    top_countries_df['Total_Spending_By_Country'],
    color='orange',
    marker='o',
    linestyle='-',
    linewidth=2,
    markersize=8,
    label='Total Spending'
    )

    # Create a third y-axis for Average Transaction Amount
    # Adding a third y-axis to plot the average transaction amount data. This axis is shifted outward for clarity.
    ax3 = ax1.twinx()
    ax3.spines['right'].set_position(('outward', 60))  # Adjust position to avoid overlap with other axes
    lines_avg_transaction = ax3.plot(
    top_countries_df['Country'],
    top_countries_df['Avg_Transaction_Amount_By_Country'],
    color='green',
    marker='s',
    linestyle='--',
    linewidth=2,
    markersize=8,
    label='Avg Transaction Amount'
    )

    # Customize the layout
    # Setting axis labels, title, and grid for the bar plot. Ensuring clear presentation of all plotted data.
    ax1.set_xlabel('Country')
    ax1.set_ylabel('Number of Customers')
    ax1.set_title('Top 20 Countries by Number of Customers, Total Spending, and Avg Transaction Amount', fontweight='bold')
    ax1.set_xticklabels(top_countries_df['Country'], rotation=45, ha='right')
    ax1.grid(axis='y', linestyle='--', alpha=0.7)

    # Set labels for the secondary y-axes
    ax2.set_ylabel('Total Spending', color='orange')
    ax3.set_ylabel('Avg Transaction Amount', color='green')

    # Add custom legends
    # Creating and positioning legends for the bar plot, total spending line plot, and average transaction amount line plot.
    ax1.legend(handles=[bars[0]], title='Customer Count', loc='upper left')
    ax2.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), title='Metrics')
    ax3.legend(loc='upper right')

    # Adjust layout to make space for the legends
    # Ensuring that the plot layout is adjusted to accommodate the legends and make the visualization clear.
    plt.tight_layout()
    plt.show()
    st.pyplot(fig)


    # Conclusion and suggestions
    st.subheader("Conclusion and Suggestions")
    st.write(
        """
        **Conclusion:** 
        The distribution of total spending by country highlights significant differences in spending behavior across regions.

        **Suggestions:** 
        - **Regional Promotions:** Design region-specific promotions and offers to cater to different spending capabilities and preferences.
        - **Market Expansion:** Consider expanding market efforts in countries with higher total spending to capitalize on existing high-value customer bases.
        - **Localized Strategies:** Implement localized strategies based on spending patterns to optimize marketing and sales efforts in various countries.
        """
    )

    print_green("Customer Segmentation Based on Their Spending Behaviour")
# Step 1: Join transactions with customers to get customer details
    transactions_customers_df = transactions_df.join(customers_df, on="Customer_ID", how="inner")

    # Step 2: Aggregate data to calculate total spending and average transaction amount per customer
    customer_aggregation_df = transactions_customers_df.groupBy("Customer_ID", "Name", "Country") \
        .agg(
            F.sum("Amount").alias("Total_Spending"),
            F.avg("Amount").alias("Average_Transaction_Amount")
        )

    # Step 3: Create `customer_spending_with_details_df` by joining with the original customer details
    customer_spending_with_details_df = customer_aggregation_df.join(customers_df.select("Customer_ID", "Email", "Phone"), on="Customer_ID", how="inner")

    # Step 4: Calculate Total Spending, Average Transaction Amount, and Customer Count by Country
    country_aggregation_df = customer_spending_with_details_df.groupBy("Country") \
        .agg(
            F.sum("Total_Spending").alias("Total_Spending_By_Country"),
            F.avg("Average_Transaction_Amount").alias("Avg_Transaction_Amount_By_Country"),
            F.count("Customer_ID").alias("Customer_Count_By_Country")
        )

    # Step 5: Manually Define Spending Clusters
    def assign_spending_cluster(total_spending):
        if total_spending < 1500:
            return "Low Spending"
        elif 1500 <= total_spending < 3000:
            return "Moderate Spending"
        else:
            return "High Spending"

    assign_spending_cluster_udf = F.udf(assign_spending_cluster, F.StringType())

    country_clustered_df = country_aggregation_df.withColumn(
        "Cluster_Name", assign_spending_cluster_udf(F.col("Total_Spending_By_Country"))
    )

    # Calculate the number of customers in each cluster
    customer_count_by_cluster = country_clustered_df.groupBy("Cluster_Name") \
        .agg(F.sum("Customer_Count_By_Country").alias("Total_Customers")) \
        .toPandas()

    # Display the customer count by cluster
    st.subheader("Customer Count by Cluster")
    st.write(customer_count_by_cluster)

    # Define a colormap for shades of green
    cmap = get_cmap('Greens')
    num_clusters = len(customer_count_by_cluster)
    colors = cmap(np.linspace(0.5, 1, num_clusters))  # Create different shades of green

    # Map colors to clusters
    color_dict = dict(zip(customer_count_by_cluster['Cluster_Name'], colors))

    # Plot the clusters
    plt.figure(figsize=(14, 6))
    sns.scatterplot(
        x='Total_Spending_By_Country', 
        y='Avg_Transaction_Amount_By_Country', 
        hue='Cluster_Name', 
        palette=color_dict,  # Use shades of green
        data=country_clustered_df.toPandas(),
        s=100
    )

    # Customize the plot
    plt.title('Customer Segmentation by Spending Behaviour')
    plt.xlabel('Total Spending by Country')
    plt.ylabel('Average Transaction Amount by Country')
    plt.legend(title='Cluster')

    # Display the plot in Streamlit
    st.pyplot(plt.gcf())

    # Conclusion and suggestions
    st.subheader("Conclusion and Suggestions")
    st.write(
        """
        **Conclusion:** 
        The customer segmentation based on spending behavior reveals distinct clusters of spending patterns. Countries are categorized into low, moderate, and high spending clusters, showing variations in total spending and average transaction amounts.

        **Suggestions:**
        - **Targeted Marketing:** Develop marketing strategies tailored to each spending cluster. For instance, offer premium products and exclusive promotions to high-spending clusters while providing value-oriented offers to low-spending clusters.
        - **Customer Engagement:** Increase engagement efforts for moderate and high-spending clusters to convert them into loyal customers. Consider personalized communication and loyalty programs.
        - **Resource Allocation:** Allocate marketing resources more efficiently by focusing on regions with high-spending clusters. Analyze low-spending regions for potential growth opportunities.
        """
    )
    # Print the overall title
    print_green("Quarterly, Monthly, and Weekly Trends of Purchasing Behavior")

    # Convert 'Date' column to date format
    transactions_df = transactions_df.withColumn('Date', to_date(col('Date'), 'yyyy-MM-dd'))

    # Monthly aggregation
    transactions_df = transactions_df.withColumn('Year', F.year(col('Date')))
    transactions_df = transactions_df.withColumn('Month', F.month(col('Date')))
    transactions_df = transactions_df.withColumn('Month_Name', F.date_format(col('Date'), 'MMMM'))

    monthly_spending_df = transactions_df.groupBy("Year", "Month", "Month_Name").agg(
        F.round(F.sum("Amount"), 2).alias("Total_Spending")
    ).orderBy("Year", "Month")

    # Convert to Pandas DataFrame for plotting
    monthly_spending_pd_df = monthly_spending_df.toPandas()
    monthly_spending_pd_df['Date'] = pd.to_datetime(monthly_spending_pd_df[['Year', 'Month']].assign(Day=1))

    # Quarterly aggregation
    transactions_df = transactions_df.withColumn('Quarter', F.quarter(col('Date')))
    quarterly_spending_df = transactions_df.groupBy("Year", "Quarter").agg(
        F.round(F.sum("Amount"), 2).alias("Total_Spending")
    ).orderBy("Year", "Quarter")

    # Convert to Pandas DataFrame for plotting
    quarterly_spending_pd_df = quarterly_spending_df.toPandas()
    quarterly_spending_pd_df['Date'] = pd.to_datetime(quarterly_spending_pd_df[['Year']].assign(
        Month=quarterly_spending_pd_df['Quarter'] * 3 - 2, Day=1))

    # Weekly aggregation
    transactions_df = transactions_df.withColumn('Day_Of_Week', date_format(col('Date'), 'EEEE'))
    weekly_spending_df = transactions_df.groupBy("Day_Of_Week").agg(
        F.round(F.sum("Amount"), 2).alias("Total_Spending")
    ).orderBy(F.expr("case when Day_Of_Week = 'Monday' then 1 "
                    "when Day_Of_Week = 'Tuesday' then 2 "
                    "when Day_Of_Week = 'Wednesday' then 3 "
                    "when Day_Of_Week = 'Thursday' then 4 "
                    "when Day_Of_Week = 'Friday' then 5 "
                    "when Day_Of_Week = 'Saturday' then 6 "
                    "when Day_Of_Week = 'Sunday' then 7 "
                    "else 8 end"))

    # Convert to Pandas DataFrame for plotting
    weekly_spending_pd_df = weekly_spending_df.toPandas()


    plot_trend(
        quarterly_spending_pd_df,
        x_col='Date',
        y_col='Total_Spending',
        x_ticks=quarterly_spending_pd_df['Date'],
        x_labels=[f'Q{q}' for q in quarterly_spending_pd_df['Quarter']],
        title='Quarterly Spending Trends',
        xlabel='Quarter',
        ylabel='Total Spending'
    )

    plot_trend(
        monthly_spending_pd_df,
        x_col='Date',
        y_col='Total_Spending',
        x_ticks=monthly_spending_pd_df['Date'],
        x_labels=monthly_spending_pd_df['Month_Name'],
        title='Monthly Spending Trends',
        xlabel='Month',
        ylabel='Total Spending'
    )

    plot_trend(
        weekly_spending_pd_df,
        x_col='Day_Of_Week',
        y_col='Total_Spending',
        title='Weekly Spending Trends by Day of the Week',
        xlabel='Day of the Week',
        ylabel='Total Spending',
        x_ticks=None,  # No specific ticks needed for day of the week
        x_labels=None  # Default labels are fine
    )
    # Conclusion
    st.write("### Conclusion")
    st.write("""
    The purchasing behavior analysis reveals the following insights:
    - **Monthly trends**: There is a consistent rise in spending during certain months, which can be attributed to factors such as holidays or seasonal sales.
    - **Quarterly trends**: The quarterly data shows consistent growth patterns, indicating that the business is maintaining a stable growth trajectory across quarters.
    - **Weekly trends**: The weekly trends highlight significant purchasing activities during weekends, suggesting that consumer behavior is influenced by leisure time or weekend promotions.

    These trends provide valuable insights for optimizing sales strategies and targeting customers during peak spending periods.
    """)


def product_sales():

    # Load data into Spark DataFrames
    customers_df = spark.read.csv("Cleaned_data/cleaned_customers.csv", header=True, inferSchema=True)
    transactions_df = spark.read.csv("Cleaned_data/cleaned_transactions.csv", header=True, inferSchema=True)
    products_df = spark.read.csv("Cleaned_data/cleaned_products.csv", header=True, inferSchema=True)

    # Data processing and visualization functions

    def load_and_process_data():
        # Join the transactions DataFrame with the products DataFrame on Product_ID
        merged_df = transactions_df.join(products_df, on='Product_ID')
        
        # Calculate total revenue and sales volume for each product
        product_sales_df = merged_df.groupBy("Product_ID", "Product_Name", "Category", "Price").agg(
            round(_sum("Amount"), 2).alias("Total_Revenue"),
            count("Transaction_ID").alias("Sales_Volume")
        )
        
        return product_sales_df, merged_df

    def plot_revenue_by_category(product_sales_df):
        # Convert Spark DataFrame to Pandas DataFrame for plotting
        product_sales_df_pd = product_sales_df.toPandas()
        
        # Sort the DataFrame by 'Category' and then by 'Total_Revenue' within each category
        product_sales_df_pd = product_sales_df_pd.sort_values(by=['Category', 'Total_Revenue'], ascending=[True, False])
        product_sales_df_pd['Product_Label'] = product_sales_df_pd['Product_Name'] + ' (' + product_sales_df_pd['Category'] + ')'

        # Set up the plotting space
        fig, ax = plt.subplots(figsize=(12, 10))

        # Get unique categories for color coding
        categories = product_sales_df_pd['Category'].unique()

        # Define a colormap for shades of blue
        cmap = get_cmap('Greens')
        palette = [cmap(i / len(categories)) for i in range(len(categories))][::-1]

        # Plot each category
        for i, category in enumerate(categories):
            category_data = product_sales_df_pd[product_sales_df_pd['Category'] == category]
            bars = ax.barh(category_data['Product_Label'], category_data['Total_Revenue'], 
                        label=category, color=palette[i], edgecolor='black')

            # Add exact revenue labels on the bars
            for bar in bars:
                width = bar.get_width()
                ax.text(width, bar.get_y() + bar.get_height()/2, f' {width:.2f}', 
                        va='center', ha='left', fontsize=10, color='black')

        # Set labels and title
        ax.set_xlabel('Total Revenue')
        ax.set_title('Total Revenue of Each Product by Category')
        ax.legend(title='Category', bbox_to_anchor=(1.05, 1), loc='upper left')

        # Adjust layout
        plt.tight_layout()

        st.pyplot(fig)

    def plot_sales_volume_by_category(product_sales_df):
        # Convert Spark DataFrame to Pandas DataFrame for plotting
        product_sales_df_pd = product_sales_df.toPandas()
        
        # Sort the DataFrame by 'Category' and then by 'Sales_Volume' within each category
        product_sales_df_pd = product_sales_df_pd.sort_values(by=['Category', 'Sales_Volume'], ascending=[True, False])
        product_sales_df_pd['Product_Label'] = product_sales_df_pd['Product_Name'] + ' (' + product_sales_df_pd['Category'] + ')'

        # Set up the plotting space
        fig, ax = plt.subplots(figsize=(12, 10))

        # Get unique categories for color coding
        categories = product_sales_df_pd['Category'].unique()

        # Define a colormap for shades of blue
        cmap = get_cmap('Greens')
        palette = [cmap(i / len(categories)) for i in range(len(categories))][::-1]

        # Plot each category
        for i, category in enumerate(categories):
            category_data = product_sales_df_pd[product_sales_df_pd['Category'] == category]
            bars = ax.barh(category_data['Product_Label'], category_data['Sales_Volume'], 
                        label=category, color=palette[i], edgecolor='black')

            # Add exact sales volume labels on the bars
            for bar in bars:
                width = bar.get_width()
                ax.text(width, bar.get_y() + bar.get_height()/2, f'{width}', 
                        va='center', ha='left', fontsize=10, color='black')

        # Set labels and title
        ax.set_xlabel('Sales Volume')
        ax.set_title('Sales Volume of Each Product by Category')
        ax.legend(title='Category', bbox_to_anchor=(1.05, 1), loc='upper left')

        # Adjust layout
        plt.tight_layout()

        st.pyplot(fig)

    def plot_category_performance(product_sales_df):
        # Convert Spark DataFrame to Pandas DataFrame for plotting
        product_sales_df_pd = product_sales_df.toPandas()
        
        # Evaluate Category Performance by Total Revenue
        category_performance = product_sales_df_pd.groupby('Category').agg({
            'Total_Revenue': 'sum',
            'Sales_Volume': 'sum'
        }).reset_index()

        # Define a colormap for shades of green
        cmap = get_cmap('Greens')
        colors = cmap(np.linspace(0, 1, len(category_performance)))[::-1]

        # Set up the figure and axis
        fig, ax = plt.subplots(figsize=(6, 6))

        # Bar plot for Category Performance by Total Revenue
        bars = ax.bar(category_performance['Category'], category_performance['Total_Revenue'], color=colors, edgecolor='black')

        # Customize the title and labels
        ax.set_title('Category Performance by Total Revenue', fontsize=16, fontweight='bold')
        ax.set_xlabel('Category', fontsize=14)
        ax.set_ylabel('Total Revenue', fontsize=14)
        ax.grid(axis='y', linestyle='--', alpha=0.7)

        # Add labels on the bars
        for bar in bars:
            height = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                height + 0.01 * max(category_performance['Total_Revenue']),
                f'{height:,.2f}',
                ha='center',
                va='bottom',
                fontsize=10
            )

        # Adjust layout
        plt.tight_layout()

        return fig
    


    def plot_total_revenue_by_category(merged_df):
        # Aggregate total spending by category
        merged_df = merged_df.toPandas()
        category_spending = merged_df.groupby(['Category']).agg({'Amount': 'sum'}).reset_index()
        
        # Define a colormap for shades of green
        cmap = plt.get_cmap('Greens')
        colors = cmap(np.linspace(0, 1, len(category_spending)))[::-1]

        # Create a pie chart
        fig, ax = plt.subplots(figsize=(6, 6))
        ax.pie(
            category_spending['Amount'], 
            labels=category_spending['Category'], 
            colors=colors, 
            autopct='%1.1f%%', 
            startangle=140,
            wedgeprops={'edgecolor': 'black'}
        )

        # Add a title
        ax.set_title('Total Revenue by Product Category', fontsize=16, fontweight='bold')

        return fig


    def plot_top_products_by_revenue(product_sales_df):
        # Convert Spark DataFrame to Pandas DataFrame for plotting
        product_sales_df_pd = product_sales_df.toPandas()
        
        # Sort by Total_Revenue to get the top 10 products
        top_products_revenue = product_sales_df_pd.sort_values(by='Total_Revenue', ascending=False).head(20)

        # Define a colormap for shades of blue
        cmap = get_cmap('Greens')
        colors = cmap(np.linspace(0, 1, len(top_products_revenue)))[::-1]

        # Set up the figure and axis
        fig, ax = plt.subplots(figsize=(12, 6))

        # Bar plot for top 10 products by total revenue
        bars = ax.bar(top_products_revenue['Product_Name'], top_products_revenue['Total_Revenue'], color=colors, edgecolor='black')
        ax.set_title('Top 20 Products by Total Revenue', fontsize=16, fontweight='bold')
        ax.set_xlabel('Product Name', fontsize=14)
        ax.set_ylabel('Total Revenue', fontsize=14)
        ax.grid(axis='y', linestyle='--', alpha=0.7)

        # Add labels on the bars
        for bar in bars:
            height = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                height + 0.01 * max(top_products_revenue['Total_Revenue']),
                f'{height:,.2f}',
                ha='center',
                va='bottom',
                fontsize=10,
                rotation=45
            )

        # Rotate x-axis labels for better readability
        plt.xticks(rotation=45, ha='right')

        # Adjust layout
        plt.tight_layout()

        st.pyplot(fig)

    def plot_top_products_by_volume(product_sales_df):
        # Convert Spark DataFrame to Pandas DataFrame for plotting
        product_sales_df_pd = product_sales_df.toPandas()
        
        # Sort by Sales_Volume to get the top 10 products
        top_products = product_sales_df_pd.sort_values(by='Sales_Volume', ascending=False).head(20)

        # Define a colormap for shades of blue
        cmap = get_cmap('Greens')
        colors = cmap(np.linspace(0, 1, len(top_products)))[::-1]

        # Set up the figure and axis
        fig, ax = plt.subplots(figsize=(12, 6))

        # Bar plot for top 10 products by sales volume
        bars = ax.bar(top_products['Product_Name'], top_products['Sales_Volume'], color=colors, edgecolor='black')
        ax.set_title('Top 20 Products by Sales Volume', fontsize=16, fontweight='bold')
        ax.set_xlabel('Product Name', fontsize=14)
        ax.set_ylabel('Sales Volume', fontsize=14)
        ax.grid(axis='y', linestyle='--', alpha=0.7)

        # Add labels on the bars
        for bar in bars:
            height = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                height + 0.01 * max(top_products['Sales_Volume']),
                f'{height:.0f}',
                ha='center',
                va='bottom',
                fontsize=10
            )

        # Rotate x-axis labels for better readability
        plt.xticks(rotation=45, ha='right')

        # Adjust layout
        plt.tight_layout()

        st.pyplot(fig)

    def plot_pricing_vs_sales(product_sales_df):
        # Convert Spark DataFrame to Pandas DataFrame for plotting
        product_sales_df_pd = product_sales_df.toPandas()
        
        # Scatter plot to visualize the relationship between price and sales volume
        fig, ax = plt.subplots(figsize=(8, 4))
        sns.scatterplot(data=product_sales_df_pd, x='Price', y='Sales_Volume', hue='Category', palette='viridis', s=100, ax=ax)

        # Fit a linear regression line to see the trend
        for category in product_sales_df_pd['Category'].unique():
            cat_data = product_sales_df_pd[product_sales_df_pd['Category'] == category]
            slope, intercept, r_value, p_value, std_err = linregress(cat_data['Price'], cat_data['Sales_Volume'])
            ax.plot(cat_data['Price'], intercept + slope * cat_data['Price'])

        # Title and labels
        ax.set_title('Effect of Pricing on Sales Volume by Category', fontweight='bold')
        ax.set_xlabel('Price')
        ax.set_ylabel('Sales Volume')
        ax.legend(title='Category')
        ax.grid(True)

        st.pyplot(fig)


    # Streamlit app
    def main():

        # Load data into Spark DataFrames
        customers_df = spark.read.csv("Cleaned_data/cleaned_customers.csv", header=True, inferSchema=True)
        transactions_df = spark.read.csv("Cleaned_data/cleaned_transactions.csv", header=True, inferSchema=True)
        products_df = spark.read.csv("Cleaned_data/cleaned_products.csv", header=True, inferSchema=True)
         # Data processing
        product_sales_df, merged_df = load_and_process_data()
        # Display charts and conclusions
        print_green('Total Revenue of Each Product by Category')
        plot_revenue_by_category(product_sales_df)
        st.write("### Conclusion:")
        st.write("This chart provides insights into the total revenue generated by each product within its respective category. It highlights high-revenue products and those contributing less to overall revenue.")
        st.write("Products in categories with higher revenues can be identified for promotional efforts, while those with lower revenues might need more targeted marketing strategies.")
        st.write("### Suggestion:")
        st.write("Focus on optimizing the product offerings in high-revenue categories. Consider introducing promotional campaigns or discounts for lower-performing products to boost their revenue.")
        st.write("Additionally, review pricing strategies to ensure competitive positioning in high-revenue categories.")

        print_green('Sales Volume of Each Product by Category')
        plot_sales_volume_by_category(product_sales_df)
        st.write("### Conclusion:")
        st.write("This chart illustrates the sales volume of each product by category, which helps in understanding which products are sold the most frequently. High-volume products may have high turnover but not necessarily high revenue.")
        st.write("Understanding sales volume can help in inventory planning and identifying popular products that drive overall sales.")
        st.write("### Suggestion:")
        st.write("Review inventory management strategies to ensure that popular products are always in stock. Explore opportunities to increase sales volume for lower-volume products through promotions or discounts.")
        st.write("Consider bundling popular high-volume products with complementary items to boost sales.")

        print_green('Top 20 Products by Total Revenue')
        plot_top_products_by_revenue(product_sales_df)
        st.write("### Conclusion:")
        st.write("This chart showcases the top 20 products generating the highest total revenue. Identifying these top performers allows for a focused approach to marketing and sales strategies.")
        st.write("Products that generate the most revenue are crucial for the business's profitability and should be prioritized in marketing efforts.")
        st.write("### Suggestion:")
        st.write("Use the insights from top revenue-generating products to craft targeted marketing campaigns. Consider increasing production or availability of these products to capitalize on their success.")
        st.write("Review customer feedback for these top products to enhance their appeal and address any potential issues.")

        print_green('Top 20 Products by Sales Volume')
        plot_top_products_by_volume(product_sales_df)
        st.write("### Conclusion:")
        st.write("This chart highlights the top 20 products by sales volume, showing which products are sold the most. High sales volume indicates strong customer demand and frequent purchases.")
        st.write("High sales volume products are essential for maintaining customer engagement and driving overall sales.")
        st.write("### Suggestion:")
        st.write("Promote high-volume products more aggressively to further increase their sales. Analyze trends to identify reasons for high sales volume and replicate successful strategies with other products.")
        st.write("Explore opportunities to introduce similar products that could benefit from the same level of customer interest.")

        print_green('Effect of Pricing on Sales Volume by Category')
        plot_pricing_vs_sales(product_sales_df)
        st.write("### Conclusion:")
        st.write("This scatter plot visualizes the relationship between product pricing and sales volume across categories. It helps in understanding how price adjustments affect sales performance.")
        st.write("Identifying pricing strategies that maximize sales volume can provide insights into pricing optimization and customer behavior.")
        st.write("### Suggestion:")
        st.write("Adjust pricing strategies based on the observed trends to optimize sales volume. Consider offering discounts or special pricing for products with high sales potential.")
        st.write("Conduct further analysis to determine the impact of different pricing strategies on sales volume and overall profitability.")

        print_green('Category Performance by Total Revenue and Total Revenue by Product Category')

        # Create columns
        col1, col2 = st.columns(2)

        with col1:
            fig1 = plot_category_performance(product_sales_df)
            st.pyplot(fig1)
            st.write("### Conclusion:")
            st.write("This bar chart illustrates the performance of each category based on total revenue. It highlights which categories are performing well and which may need attention.")
            st.write("Categories with higher revenue are crucial to business success, and those with lower revenue may require more focus and improvement.")
            st.write("### Suggestion:")
            st.write("Invest more resources into high-revenue categories to enhance their performance. For lower-revenue categories, explore new marketing strategies or product enhancements to boost their performance.")

        with col2:
            fig2 = plot_total_revenue_by_category(merged_df)
            st.pyplot(fig2)
            st.write("### Conclusion:")
            st.write("This pie chart shows the distribution of total revenue across different product categories. It helps in understanding which categories contribute the most to overall revenue.")
            st.write("Categories with a larger share of total revenue are key drivers of business profitability and should be closely monitored.")
            st.write("### Suggestion:")
            st.write("Focus on expanding the product offerings in high-revenue categories. Additionally, consider evaluating and improving the performance of lower-revenue categories to increase their contribution to total revenue.")

    main()