from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when,sum as spark_sum
import warnings
warnings.filterwarnings('ignore')

"""# Problem Statement 5: Sales Team Performance Assessment

**Objective**: Analyse sales team performance to identify high performers and areas needing improvement.

**Description:**
1.	Assess sales_team.csv to evaluate performance metrics such as sales achieved versus sales targets for each sales representative.
2.	Identify top performers and those who are underperforming relative to their targets.

**Expected Deliverables:**
- A performance report highlighting top sales representatives and those needing additional support or training.
- Actionable recommendations for improving overall sales performance.
"""

# Commented out IPython magic to ensure Python compatibility.
# %run utilities/bar_plot_with_line.ipynb

"""## Performance Ratio, Sales Percentage and Performance Status of Each Sales Representative"""

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
# Calculate the percentage of sales achieved relative to the target for each sales rep
sales_team_df = sales_team_df.withColumn('Sales_Percentage', (col('Sales_Achieved') / col('Sales_Target')) * 100)
# Display the updated DataFrame with Performance Ratio and Status
sales_team_df.show(5)

"""# Sales Target vs Sales Achieved for Each Representative"""

# Convert to Pandas for plotting
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

"""# Idenytifying Top Performer and Under Performer"""

# Separate top performers and underperformers
top_performers_df = sales_team_df.filter(col("Performance_Ratio") >= 1).toPandas()
underperformers_df = sales_team_df.filter(col("Performance_Ratio") < 1).toPandas()

# Sort by Performance Ratio in descending order
top_performers_df = top_performers_df.sort_values(by='Performance_Ratio', ascending=False)
underperformers_df = underperformers_df.sort_values(by='Performance_Ratio', ascending=False)

plot_performers_bar_chart(
    df=top_performers_df,
    title='High Performers (Performance Ratio >= 1)',
    xlabel='Sales Representatives',
    ylabel='Performance Ratio',
    cmap='Blues',
    color_inversion=True  # Invert colors for top performers
)

plot_performers_bar_chart(
    df=underperformers_df,
    title='Underperformers (Performance Ratio < 1)',
    xlabel='Sales Representatives',
    ylabel='Performance Ratio',
    cmap='Blues',
    color_inversion=False
)

"""# Sales Team Performance by Region"""

# Aggregate Sales_Achieved and Sales_Target by Region
region_performance_df = sales_team_df.groupBy("Region").agg(
    spark_sum("Sales_Achieved").alias("Total_Sales_Achieved"),
    spark_sum("Sales_Target").alias("Total_Sales_Target")
)

# Calculate the Performance Ratio (Total_Sales_Achieved / Total_Sales_Target) for each region
region_performance_df = region_performance_df.withColumn(
    "Performance_Ratio", col("Total_Sales_Achieved") / col("Total_Sales_Target")
)

# Classify regions as High Performer or Underperformer
region_performance_df = region_performance_df.withColumn(
    "Performance_Status",
    when(col("Performance_Ratio") >= 1, "High Performer").otherwise("Underperformer")
)
region_performance_df.show(5)

plot_region_performance(region_performance_df)

"""# Performance Report Highlighting Top 20 Sales Representatives"""

# Assuming 'sales_team_df' is a Spark DataFrame, first convert it to a Pandas DataFrame for further processing
sales_team_pd_df = sales_team_df.toPandas()

# Calculate the Sales Achievement Rate
sales_team_pd_df['Achievement_Rate (%)'] = (sales_team_pd_df['Sales_Achieved'] / sales_team_pd_df['Sales_Target']) * 100

# Sort by achievement rate to highlight top performers and select the top 20
top_20_performers = sales_team_pd_df.sort_values(by='Achievement_Rate (%)', ascending=False).head(20)
plot_dual_axis_bar_line(
    data_pd=top_20_performers,             # DataFrame with top 20 performers
    x_labels='Name',                       # Column for y-axis (sales rep names)
    bar_col='Sales_Target',                # Column for bar plot (Sales Target)
    line_col='Sales_Achieved',             # Column for line plot (Sales Achieved)
    bar_label='Sales Target',              # Label for the bar plot
    line_label='Sales Achieved',           # Label for the line plot
    title='Top 20 Performers: Sales Target vs Sales Achieved',  # Plot title
    x_title='Sales Representative',        # X-axis label
    bar_y_title='Sales Target',            # Y-axis label for the bar plot
    line_y_title='Sales Achieved',         # Y-axis label for the line plot
    cmap='Blues',                          # Colormap for bars
    line_color='orange'                    # Color for the line plot
)