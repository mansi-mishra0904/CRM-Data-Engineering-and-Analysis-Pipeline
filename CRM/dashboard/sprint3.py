from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, sum as spark_sum
from common_utility import *
from bar_plot_with_line import *
from world_map_plot import *

import streamlit as st
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, when, round, expr
import plotly.express as px

# Initialize Spark session
spark = initialize_spark_session("Interaction Analysis")
def print_green(text: str):
    st.write(f"<h3><b><span style='color:green'>{text}</span></b></h3>", unsafe_allow_html=True)

def sprint3():
    # Load the datasets
    interactions_file_path = 'Cleaned_data/cleaned_interactions.csv'
    customers_file_path = 'Cleaned_data/cleaned_customers.csv'

    interactions = load_data_files(interactions_file_path)
    customers = load_data_files(customers_file_path)

    # Merge interactions with customers on Customer_ID
    interactions_with_customers = interactions.join(customers, on='Customer_ID')

    # Section 1: Analyzing Resolution Rates of Different Interaction Types

    print_green("Interaction Type Counts and Resolution Rates")

    # Step 1: Calculate counts of interactions by type
    interaction_counts = interactions_with_customers.groupBy('Interaction_Type').count()

    # Step 2: Calculate resolution rates by interaction type (as percentages)
    resolution_rates = interactions_with_customers.groupBy('Interaction_Type').agg(
        (F.mean(F.col('Issue_Resolved').cast('double')) * 100).alias('Resolution Rate (%)')
    )

    # Step 3: Combine counts and resolution rates into a single DataFrame
    data = interaction_counts.join(resolution_rates, on='Interaction_Type')

    # Step 4: Convert to Pandas DataFrame for plotting
    data_pd = data.toPandas()

    # Plot Interaction Type Counts and Resolution Rates
    plot_dual_axis_bar_line(
        data_pd=data_pd,
        x_labels='Interaction_Type',
        bar_col='count',
        line_col='Resolution Rate (%)',
        bar_label='Count',
        line_label='Resolution Rate (%)',
        title='Interaction Type Counts and Resolution Rates',
        x_title='Interaction Type',
        bar_y_title='Count',
        line_y_title='Resolution Rate (%)'
    )

    # Suggestions and Conclusion
    st.write("**Suggestions:**")
    st.write("1. Focus more on improving the interaction channel, which shows a lower resolution rate.")
    st.write("2. Continue investing in Chat support and Email support as it has the lower resolution rate.")

    st.write("**Conclusion:** Chat has the highest interaction count, but Phone shows the best resolution rate. This suggests that while customers use Email or chat more often, Phone is more effective in resolving issues.")

    # Section 2: Top 20 Countries Based on Interaction Type Count
    print_green("Top 20 Countries Based on Interaction Type Count")

    # Convert 'Issue_Resolved' to boolean
    interactions = interactions.withColumn('Issue_Resolved', col('Issue_Resolved').cast('boolean'))

    # Merge interactions with customers data
    interactions_with_customers = interactions.join(customers, on='Customer_ID')

    # Step 2: Count total interactions by country
    total_interactions_by_country = interactions_with_customers.groupBy('Country').count()
    total_interactions_by_country = total_interactions_by_country.withColumnRenamed('count', 'Total_Interactions')

    # Step 3: Sort and get top 20 countries
    top_20_countries = total_interactions_by_country.orderBy(col('Total_Interactions').desc()).limit(20)
    top_20_countries_list = [row['Country'] for row in top_20_countries.collect()]

    # Step 4: Filter interactions for top 20 countries
    interaction_counts_top_20 = interactions_with_customers.filter(col('Country').isin(top_20_countries_list))

    # Step 5: Count interactions by country and interaction type
    interaction_counts = interaction_counts_top_20.groupBy('Country', 'Interaction_Type').count()
    interaction_counts_pivot = interaction_counts.groupBy('Country').pivot('Interaction_Type').sum('count').fillna(0)

    # Step 6: Convert to Pandas DataFrame for plotting
    data = interaction_counts_pivot.toPandas()

    # Step 7: Set up the data for plotting
    data.set_index('Country', inplace=True)

    # Plot Interaction Counts by Country
    plot_interaction_counts_by_country(data)

    # Suggestions and Conclusion
    st.write("**Suggestions:**")
    st.write("1. Identify the top countries in terms of interactions shows the most active customers also countries having maximum issues.")

    st.write("**Conclusion:** The majority of interactions come from the top 20 countries, highlighting where the customer base is most active.")

    # Section 3: Preferred Method of Interactions by Country
    print_green("Preferred Method of Interactions by Country")

    # Merge interactions with customers
    merged_data = interactions.join(customers, on='Customer_ID')
    interaction_counts = merged_data.groupBy('Country', 'Interaction_Type').count()

    pivoted_data = interaction_counts.groupBy('Country').pivot('Interaction_Type').sum('count').fillna(0)

    # Define interaction types
    interaction_types = ['Chat', 'Email', 'Phone']

    # Create a new column for the preferred interaction method
    preferred_method = pivoted_data.withColumn(
        "Preferred_Interaction_Method", 
        expr(f"CASE " + " ".join([f"WHEN {col} = greatest({', '.join(interaction_types)}) THEN '{col}'" for col in interaction_types]) + " END")
    )

    # Select necessary columns
    preferred_method = preferred_method.select('Country', 'Preferred_Interaction_Method')

    # Plot Preferred Method by Country
    preferred_method_pd = preferred_method.toPandas()

    plot_world_map(
        preferred_method_pd,
        locations_col="Country",
        color_col="Preferred_Interaction_Method",
        plot_title="Preferred Interaction Methods by Demographics",
        hover_data=None,
        is_discrete=True,
        color_discrete_sequence=px.colors.sequential.Greens_r
    )

    # Suggestions and Conclusion
    st.write("**Suggestions:**")
    st.write("1. Consider expanding Chat services in countries where it is the most preferred method.")
    st.write("2. Optimize Email and Phone support in regions where they are highly preferred.")

    st.write("**Conclusion:** Different countries have distinct preferences for interaction methods, with Chat being a common choice in many regions.")

    # Section 4: Resolution Rate by Country
    print_green("Resolution Rate by Country")

    # Group by country and interaction type to get total and resolved issues
    country_resolution_counts = interactions_with_customers.groupBy("Country").agg(
        count(when(col("Issue_Resolved") == "Yes", 1)).alias("Resolved_Issues"),
        count("*").alias("Total_Issues")
    )

    # Calculate resolution rate for each country
    country_resolution_rates = country_resolution_counts.withColumn(
        "Resolution_Rate", round(col("Resolved_Issues") / col("Total_Issues") * 100, 2)
    )

    # Convert Spark DataFrame to Pandas DataFrame
    country_resolution_rates_pd = country_resolution_rates.toPandas()

    # Plot Resolution Rate by Country
    plot_world_map(
        country_resolution_rates_pd,
        locations_col="Country",
        color_col="Resolution_Rate",
        plot_title="Resolution Rate by Country",
        hover_data=["Resolved_Issues", "Total_Issues"],
        is_discrete=False,
        color_scale=px.colors.sequential.Greens_r
    )
    
    # Suggestions and Conclusion
    st.write("**Suggestions:**")
    st.write("1. Analyze countries with lower resolution rates to identify challenges.")
    st.write("2. Implement targeted support strategies in countries with lower performance.")

    st.write("**Conclusion:** The resolution rates vary significantly across countries. Identifying trends can help prioritize resources for improving support in key areas.")



def sales_team_performance():
    # Function to print titles in green
    def print_green(text):
        st.markdown(f"<h3 style='color:green'>{text}</h3>", unsafe_allow_html=True)

    # Streamlit app main function
    def main():
        # Initialize Spark session
        spark = SparkSession.builder.appName("Sales Team Performance Assessment").getOrCreate()

        # Load the sales_team.csv file into a DataFrame
        sales_team_df = spark.read.csv("Cleaned_data/cleaned_sales_team.csv", header=True, inferSchema=True)

        # Calculate the Performance Ratio (Sales_Achieved / Sales_Target)
        sales_team_df = sales_team_df.withColumn("Performance_Ratio", col("Sales_Achieved") / col("Sales_Target"))

        # Classify Performance Status (High Performer or Underperformer)
        sales_team_df = sales_team_df.withColumn(
            "Performance_Status",
            when(col("Performance_Ratio") >= 1, "High Performer").otherwise("Underperformer")
        )

        # Calculate Sales Percentage
        sales_team_df = sales_team_df.withColumn('Sales_Percentage', (col('Sales_Achieved') / col('Sales_Target')) * 100)

        # Sales Target vs Sales Achieved for Each Representative
        print_green("Sales Target vs Sales Achieved for Each Representative")
        sales_team_pd_df = sales_team_df.toPandas()
        plot_dual_axis_bar_line(
            data_pd=sales_team_pd_df,
            x_labels='Name',
            bar_col='Sales_Target',
            line_col='Sales_Achieved',
            bar_label='Sales Target',
            line_label='Sales Achieved',
            title='Sales Target vs Sales Achieved for Each Representative',
            x_title='Sales Representatives',
            bar_y_title='Sales Target',
            line_y_title='Sales Achieved',
            rotation=45
        )

        # Conclusion and Suggestion
        st.write("**Conclusion:** The graph shows how well each representative is performing relative to their target. Some representatives have exceeded their targets, while others are lagging.")
        st.write("**Suggestion:** For those who are underperforming, consider implementing additional training or mentorship programs to help them improve their performance.")

        # Identifying Top Performer and Underperformer
        print_green("Identifying Top Performer and Underperformer")
        top_performers_df = sales_team_df.filter(col("Performance_Ratio") >= 1).toPandas()
        underperformers_df = sales_team_df.filter(col("Performance_Ratio") < 1).toPandas()

        plot_performers_bar_chart(
            df=top_performers_df,
            title='High Performers (Performance Ratio >= 1)',
            xlabel='Sales Representatives',
            ylabel='Performance Ratio',
            cmap='Greens',
            color_inversion=True
        )

        plot_performers_bar_chart(
            df=underperformers_df,
            title='Underperformers (Performance Ratio < 1)',
            xlabel='Sales Representatives',
            ylabel='Performance Ratio',
            cmap='Greens',
            color_inversion=False
        )

        # Conclusion and Suggestion
        st.write("**Conclusion:** High performers are consistently achieving or exceeding their sales targets, while underperformers are falling short.")
        st.write("**Suggestion:** Continue to support high performers and provide targeted assistance to underperformers, such as goal-setting, tracking performance metrics, and offering incentives.")

        # Sales Team Performance by Region
        print_green("Sales Team Performance by Region")
        region_performance_df = sales_team_df.groupBy("Region").agg(
            spark_sum("Sales_Achieved").alias("Total_Sales_Achieved"),
            spark_sum("Sales_Target").alias("Total_Sales_Target")
        )

        region_performance_df = region_performance_df.withColumn(
            "Performance_Ratio", col("Total_Sales_Achieved") / col("Total_Sales_Target")
        )

        region_performance_df = region_performance_df.withColumn(
            "Performance_Status",
            when(col("Performance_Ratio") >= 1, "High Performer").otherwise("Underperformer")
        )

        plot_region_performance(region_performance_df)

        # Conclusion and Suggestion
        st.write("**Conclusion:** Regions that consistently meet or exceed targets demonstrate strong market performance, while others may need to reassess their strategies.")
        st.write("**Suggestion:** Focus on regions that are underperforming by analyzing local market conditions and adjusting sales strategies accordingly.")

        # Performance Report Highlighting Top 20 Sales Representatives
        print_green("Performance Report Highlighting Top 20 Sales Representatives")
        sales_team_pd_df['Achievement_Rate (%)'] = (sales_team_pd_df['Sales_Achieved'] / sales_team_pd_df['Sales_Target']) * 100
        top_20_performers = sales_team_pd_df.sort_values(by='Achievement_Rate (%)', ascending=False).head(20)
        plot_dual_axis_bar_line(
            data_pd=top_20_performers,
            x_labels='Name',
            bar_col='Sales_Target',
            line_col='Sales_Achieved',
            bar_label='Sales Target',
            line_label='Sales Achieved',
            title='Top 20 Performers: Sales Target vs Sales Achieved',
            x_title='Sales Representative',
            bar_y_title='Sales Target',
            line_y_title='Sales Achieved',
            cmap='Greens',
            line_color='orange'
        )

        # Conclusion and Suggestion
        st.write("**Conclusion:** The top 20 performers are excelling in achieving their targets, with some achieving much higher than expected.")
        st.write("**Suggestion:** Consider recognizing and rewarding these top performers to maintain high levels of motivation and encourage continuous success.")

    main()

