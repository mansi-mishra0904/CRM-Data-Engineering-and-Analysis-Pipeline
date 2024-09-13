import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.cm import get_cmap
from pyspark.sql import functions as F
import seaborn as sns
from pyspark.sql import DataFrame

def plot_dual_axis_bar_line(data_pd, x_labels, bar_col, line_col, bar_label, line_label, title, x_title, bar_y_title, line_y_title, cmap='Greens', line_color='orange', figsize=(14, 6), rotation=45):
    """
    A reusable function to create a dual-axis plot with bars and a line plot.

    Parameters:
        data_pd (pd.DataFrame): The input data as a Pandas DataFrame.
        x_labels (str): Column name for x-axis labels.
        bar_col (str): Column name for bar plot values.
        line_col (str): Column name for line plot values.
        bar_label (str): Label for the bar plot.
        line_label (str): Label for the line plot.
        title (str): Title of the plot.
        x_title (str): Label for the x-axis.
        bar_y_title (str): Label for the y-axis of the bar plot.
        line_y_title (str): Label for the y-axis of the line plot.
        cmap (str): Colormap for bar plot (default is 'Greens').
        line_color (str): Color for the line plot (default is 'orange').
        figsize (tuple): Size of the figure (default is (14, 6)).
        rotation (int): Rotation for x-axis labels (default is 45).
    """
    # Create an index for x-axis labels
    x = np.arange(len(data_pd[x_labels]))

    # Define colormap for bars
    cmap = get_cmap(cmap)
    colors = cmap(np.linspace(0, 1, len(data_pd)))[::-1]  # Reverse shades for better contrast

    # Create figure and first axis for bar plot
    fig, ax1 = plt.subplots(figsize=figsize)

    # Bar plot for bar_col values
    bars = ax1.bar(
        x,
        data_pd[bar_col],
        color=colors,
        width=0.5,
        label=bar_label,
        edgecolor='black'
    )

    # Annotate bars with values
    for bar in bars:
        yval = bar.get_height()
        ax1.text(
            bar.get_x() + bar.get_width() / 2.0,
            yval + 0.05 * max(data_pd[bar_col]),
            f'{int(yval)}',  # Display as integer
            ha='center',
            va='bottom',
            fontsize=10
        )

    # Create second axis for line plot
    ax2 = ax1.twinx()

    # Line plot for line_col values
    lines = ax2.plot(
        x,
        data_pd[line_col],
        color=line_color,
        marker='o',
        linestyle='-',
        linewidth=2,
        markersize=8,
        label=line_label
    )
    
    # Set axis titles
    ax1.set_xlabel(x_title, fontsize=14)
    ax1.set_ylabel(bar_y_title, color='blue', fontsize=14)
    ax2.set_ylabel(line_y_title, color=line_color, fontsize=14)

    # Set title
    ax1.set_title(title, fontweight='bold', fontsize=16)

    # Customize x-axis labels
    ax1.set_xticks(x)
    ax1.set_xticklabels(data_pd[x_labels], rotation=rotation, ha='right', fontsize=10)

    # Grid for the bar plot axis (y-axis)
    ax1.grid(axis='y', linestyle='--', alpha=0.7)

    # Create a custom legend
    legend_patches = [patches.Patch(color=colors[0], label=bar_label), patches.Patch(color=line_color, label=line_label)]
    ax1.legend(
        handles=legend_patches,
        loc='upper left',
        title='Legend',
        fontsize=10,
        title_fontsize=12
    )
    ax2.legend(loc='upper right')
    

    # Adjust layout to prevent overlap
    plt.tight_layout()

    # Show the plot
    plt.show()

def plot_triple_axis_bar_line(data_pd, x_labels, bar_col, line1_col, line2_col, 
                              bar_label, line1_label, line2_label, 
                              title, x_title, bar_y_title, line1_y_title, line2_y_title, 
                              cmap='Greens', line1_color='orange', line2_color='green', 
                              figsize=(14, 6), rotation=45):
    """
    Extended reusable function to create a triple-axis plot with bars and two line plots.

    Parameters:
        data_pd (pd.DataFrame): Input data as a Pandas DataFrame.
        x_labels (str): Column name for x-axis labels.
        bar_col (str): Column name for bar plot values.
        line1_col (str): Column name for the first line plot.
        line2_col (str): Column name for the second line plot.
        bar_label (str): Label for the bar plot.
        line1_label (str): Label for the first line plot.
        line2_label (str): Label for the second line plot.
        title (str): Title of the plot.
        x_title (str): Label for the x-axis.
        bar_y_title (str): Label for the y-axis of the bar plot.
        line1_y_title (str): Label for the first y-axis of the line plot.
        line2_y_title (str): Label for the second y-axis of the line plot.
        cmap (str): Colormap for bar plot (default is 'Greens').
        line1_color (str): Color for the first line plot.
        line2_color (str): Color for the second line plot.
        figsize (tuple): Size of the figure (default is (14, 6)).
        rotation (int): Rotation for x-axis labels (default is 45).
    """
    # Create an index for x-axis labels
    x = np.arange(len(data_pd[x_labels]))

    # Define colormap for bars
    cmap = get_cmap(cmap)
    colors = cmap(np.linspace(0, 1, len(data_pd)))[::-1]  # Reverse shades for better contrast

    # Create figure and first axis for bar plot
    fig, ax1 = plt.subplots(figsize=figsize)

    # Bar plot for bar_col values
    bars = ax1.bar(
        x,
        data_pd[bar_col],
        color=colors,
        width=0.5,
        label=bar_label,
        edgecolor='black'
    )

    # Annotate bars with values
    for bar in bars:
        yval = bar.get_height()
        ax1.text(
            bar.get_x() + bar.get_width() / 2.0,
            yval + 0.05 * max(data_pd[bar_col]),
            f'{int(yval)}',  # Display as integer
            ha='center',
            va='bottom',
            fontsize=10
        )

    # Create second axis for the first line plot
    ax2 = ax1.twinx()

    # Line plot for line1_col values
    lines1 = ax2.plot(
        x,
        data_pd[line1_col],
        color=line1_color,
        marker='o',
        linestyle='-',
        linewidth=2,
        markersize=8,
        label=line1_label
    )
    
    # Create third axis for the second line plot
    ax3 = ax1.twinx()
    ax3.spines["right"].set_position(("outward", 60))  # Adjust position to avoid overlap
    
    # Line plot for line2_col values
    lines2 = ax3.plot(
        x,
        data_pd[line2_col],
        color=line2_color,
        marker='s',
        linestyle='--',
        linewidth=2,
        markersize=8,
        label=line2_label
    )

    # Set axis titles
    ax1.set_xlabel(x_title, fontsize=14)
    ax1.set_ylabel(bar_y_title, fontsize=14)
    ax2.set_ylabel(line1_y_title, color=line1_color, fontsize=14)
    ax3.set_ylabel(line2_y_title, color=line2_color, fontsize=14)

    # Set title
    ax1.set_title(title, fontweight='bold', fontsize=16)

    # Customize x-axis labels
    ax1.set_xticks(x)
    ax1.set_xticklabels(data_pd[x_labels], rotation=rotation, ha='right', fontsize=10)

    # Grid for the bar plot axis (y-axis)
    ax1.grid(axis='y', linestyle='--', alpha=0.7)

    # Add custom legends
    bar_patch = patches.Patch(color=colors[0], label=bar_label)
    line1_patch = patches.Patch(color=line1_color, label=line1_label)
    line2_patch = patches.Patch(color=line2_color, label=line2_label)
    
    ax1.legend(handles=[bar_patch], loc='upper left', title='Customer Count', fontsize=10)
    ax2.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), title='Total Spending', fontsize=10)
    ax3.legend(loc='upper right', title='Avg Transaction Amount', fontsize=10)

    # Adjust layout to prevent overlap
    plt.tight_layout()

    # Show the plot
    plt.show()
def plot_trend(df, x_col, y_col, x_ticks=None, x_labels=None, title='', xlabel='', ylabel='', marker='^', linestyle='-', color='green', figsize=(10, 4)):
    """
    Plots a trend line for a given DataFrame and customization parameters.

    Parameters:
        df (pd.DataFrame): DataFrame with the data to plot.
        x_col (str): Name of the column to be used for the x-axis.
        y_col (str): Name of the column to be used for the y-axis.
        x_ticks (list, optional): List of tick locations for the x-axis. Default is None.
        x_labels (list, optional): List of tick labels for the x-axis. Default is None.
        title (str): Title of the plot. Default is ''.
        xlabel (str): Label for the x-axis. Default is ''.
        ylabel (str): Label for the y-axis. Default is ''.
        marker (str): Marker style for the plot. Default is '^'.
        linestyle (str): Line style for the plot. Default is '-'.
        color (str): Color for the plot line. Default is 'blue'.
        figsize (tuple): Size of the figure. Default is (12, 6).
    """
    plt.figure(figsize=figsize)
    
    plt.plot(
        df[x_col],
        df[y_col],
        marker=marker,  
        linestyle=linestyle, 
        color=color,
        linewidth=2
    )
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True)
    
    if x_ticks is not None:
        plt.xticks(x_ticks, x_labels, rotation=45, ha='right')
    
    # Add labels on top of dots
    for i in range(len(df)):
        plt.text(
            df[x_col].iloc[i], 
            df[y_col].iloc[i], 
            f'{df[y_col].iloc[i]:.2f}', 
            ha='center', 
            va='bottom'
        )
    
    plt.tight_layout()
    plt.show()


def customer_segmentation_by_spending_behavior(transactions_df: DataFrame, customers_df: DataFrame):
    """
    Function to perform customer segmentation based on spending behavior and visualize the results.

    Parameters:
        transactions_df (DataFrame): Spark DataFrame containing transactions data.
        customers_df (DataFrame): Spark DataFrame containing customer details.
    
    Steps:
        1. Join transactions with customers to get detailed information.
        2. Aggregate data to calculate total spending and average transaction amount per customer.
        3. Join aggregated data with the original customer details.
        4. Calculate spending, average transaction, and customer count by country.
        5. Assign spending clusters based on total spending.
        6. Visualize customer segmentation using a scatter plot.
    """

    # Step 1: Join transactions with customers to get customer details
    transactions_customers_df = transactions_df.join(customers_df, on="Customer_ID", how="inner")

    # Step 2: Aggregate data to calculate total spending and average transaction amount per customer
    customer_aggregation_df = transactions_customers_df.groupBy("Customer_ID", "Name", "Country") \
        .agg(
            F.sum("Amount").alias("Total_Spending"),
            F.avg("Amount").alias("Average_Transaction_Amount")
        )

    # Step 3: Create `customer_spending_with_details_df` by joining with customer contact details
    customer_spending_with_details_df = customer_aggregation_df.join(
        customers_df.select("Customer_ID", "Email", "Phone"),
        on="Customer_ID",
        how="inner"
    )

    # Step 4: Calculate Total Spending, Average Transaction Amount, and Customer Count by Country
    country_aggregation_df = customer_spending_with_details_df.groupBy("Country") \
        .agg(
            F.sum("Total_Spending").alias("Total_Spending_By_Country"),
            F.avg("Average_Transaction_Amount").alias("Avg_Transaction_Amount_By_Country"),
            F.count("Customer_ID").alias("Customer_Count_By_Country")
        )

    # Step 5: Define Spending Clusters
    def assign_spending_cluster(total_spending):
        if total_spending < 1500:
            return "Low Spending"
        elif 1500 <= total_spending < 3000:
            return "Moderate Spending"
        else:
            return "High Spending"

    # Register UDF for clustering
    assign_spending_cluster_udf = F.udf(assign_spending_cluster, F.StringType())

    # Add cluster names based on total spending
    country_clustered_df = country_aggregation_df.withColumn(
        "Cluster_Name", assign_spending_cluster_udf(F.col("Total_Spending_By_Country"))
    )

    # Step 6: Calculate the number of customers in each cluster
    customer_count_by_cluster = country_clustered_df.groupBy("Cluster_Name") \
        .agg(F.sum("Customer_Count_By_Country").alias("Total_Customers")) \
        .toPandas()

   

    # Step 7: Define a colormap for the clusters
    cmap = get_cmap('Greens')
    num_clusters = len(customer_count_by_cluster)
    colors = cmap(np.linspace(0.5, 1, num_clusters))  # Create different shades

    # Map colors to clusters
    color_dict = dict(zip(customer_count_by_cluster['Cluster_Name'], colors))

    # Step 8: Plot the clusters using a scatter plot
    plt.figure(figsize=(10, 6))
    sns.scatterplot(
        x='Total_Spending_By_Country', 
        y='Avg_Transaction_Amount_By_Country', 
        hue='Cluster_Name', 
        palette=color_dict,  # Use colormap for clusters
        data=country_clustered_df.toPandas(),
        s=100
    )

    # Customize the plot
    plt.title('Customer Segmentation by Spending Behaviour')
    plt.xlabel('Total Spending by Country')
    plt.ylabel('Average Transaction Amount by Country')
    plt.legend(title='Cluster')

    # Show the plot
    plt.show()



def plot_horizontal_barh(df, category_col, label_col, value_col, title='Total Revenue by Category'):
    """
    Reusable plotting function for creating horizontal bar charts.

    Parameters:
    - df: Pandas DataFrame containing the data to plot.
    - category_col: Column name containing the category (used for color grouping).
    - label_col: Column name for the labels (y-axis).
    - value_col: Column name for the values (x-axis).
    - title: Optional. The title of the plot. Default is 'Total Revenue by Category'.
    """
     # Sort the DataFrame by category and value
    df = df.sort_values(by=[category_col, value_col], ascending=[True, False])

    # Set up the plotting space
    fig, ax = plt.subplots(figsize=(10, 8))

    # Get unique categories for color coding
    categories = df[category_col].unique()

    # Define a colormap for shades of blue
    cmap = get_cmap('Greens')

    # Generate colors for each category
    palette = [cmap(i / len(categories)) for i in range(len(categories))][::-1]

    # Plot each category
    for i, category in enumerate(categories):
        category_data = df[df[category_col] == category]
        bars = ax.barh(category_data[label_col], category_data[value_col], 
                       label=category, color=palette[i], edgecolor='black')

        # Add exact value labels on the bars
        for bar in bars:
            width = bar.get_width()
            ax.text(width, bar.get_y() + bar.get_height()/2, f'{width:.2f}', 
                    va='center', ha='left', fontsize=10, color='black')

    # Set labels and title
    ax.set_xlabel('Total Revenue')
    ax.set_title(title)
    ax.legend(title='Category', bbox_to_anchor=(1.05, 1), loc='upper left')

    # Adjust layout
    plt.tight_layout()
    plt.show()

def plot_vertical_bar(df, label_col, value_col, title='Top Products by Total Revenue'):
    """
    Reusable plotting function for creating vertical bar charts.

    Parameters:
    - df: Pandas DataFrame containing the data to plot.
    - label_col: Column name for the labels (x-axis).
    - value_col: Column name for the values (y-axis).
    - title: Optional. The title of the plot. Default is 'Top Products by Total Revenue'.
    """
    # Sort the DataFrame by value column
    df = df.sort_values(by=value_col, ascending=False)

    # Define a colormap for shades of blue
    cmap = get_cmap('Greens')

    # Generate colors for each bar
    colors = cmap(np.linspace(0, 1, len(df)))[::-1]
    
    # Set up the figure and axes
    fig, ax = plt.subplots(figsize=(12, 6))

    # Plot vertical bars
    bars = ax.bar(df[label_col], df[value_col], color=colors, edgecolor='black')

    # Add labels on the bars
    for bar in bars:
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            height + 0.01 * df[value_col].max(),  # Add a small offset above the bar
            f'{height:,.2f}',  # Format the label
            ha='center',
            va='bottom',
            fontsize=10,
            rotation=45
        )

    # Set labels and title
    ax.set_title(title, fontsize=16, fontweight='bold')
    ax.set_xlabel('Product Name', fontsize=14)
    ax.set_ylabel('Total Revenue', fontsize=14)
    ax.grid(axis='y', linestyle='--', alpha=0.7)

    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45, ha='right')

    # Adjust layout
    plt.tight_layout()
    plt.show()

def plot_performers_bar_chart(df, title, xlabel, ylabel, cmap='Greens', color_inversion=False):
    """
    Function to plot bar chart for sales team performance.
    
    Parameters:
    df: pd.DataFrame - DataFrame containing the sales performance data
    title: str - Title of the plot
    xlabel: str - Label for the x-axis
    ylabel: str - Label for the y-axis
    cmap: str - Color map for the bar chart
    color_inversion: bool - Whether to reverse the color map order (for descending order of performers)
    """
    # Sort by Performance Ratio
    df = df.sort_values(by='Performance_Ratio', ascending=not color_inversion)

    # Generate a color palette
    num_bars = len(df)
    cmap = get_cmap(cmap)
    colors = cmap(np.linspace(0, 1, num_bars))
    
    # Reverse colors if color inversion is needed
    if color_inversion:
        colors = colors[::-1]

    # Set up the figure and plot the bars
    plt.figure(figsize=(12, 6))
    bars = plt.bar(
        df['Name'], 
        df['Performance_Ratio'], 
        color=colors,
        edgecolor='black'
    )

    # Set labels and title
    plt.xlabel(xlabel, fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    plt.title(title, fontsize=15)
    plt.xticks(rotation=45, ha='right')

    # Adding labels on top of the bars
    for bar in bars:
        yval = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2, 
            yval + 0.01, 
            f'{round(yval, 3)}',  # Show values up to 3 decimal places
            va='bottom', 
            ha='center', 
            fontsize=10,
            rotation=45  # Tilt the label by 45 degrees
        )

    plt.tight_layout()
    plt.show()

def plot_region_performance(region_performance_df):
    """
    Function to aggregate sales data by region, calculate performance ratio, and plot the performance by region.
    
    Parameters:
    sales_team_df: Spark DataFrame - DataFrame containing sales performance data
    """

    # Step 1: Convert Spark DataFrame to Pandas for plotting
    region_performance_pd_df = region_performance_df.toPandas()

    # Step 2: Sort by Performance Ratio in descending order
    region_performance_pd_df = region_performance_pd_df.sort_values(by='Performance_Ratio', ascending=False)

    # Step 3: Define a colormap for different shades of blue
    cmap = get_cmap('Greens')
    colors = cmap(np.linspace(0, 1, len(region_performance_pd_df)))[::-1]

    # Step 4: Plot the performance by region
    plt.figure(figsize=(12, 8))
    bars = plt.bar(
        region_performance_pd_df['Region'],
        region_performance_pd_df['Performance_Ratio'],
        color=colors,
        edgecolor='black'
    )

    # Step 5: Add performance ratio labels on top of the bars
    for bar in bars:
        yval = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2, 
            yval + 0.01, 
            round(yval, 3),
            va='bottom', 
            ha='center', 
            fontsize=10,
            rotation=45
        )

    # Step 6: Add a horizontal line at performance ratio of 1
    plt.axhline(y=1, color='red', linestyle='--', linewidth=2, label='Performance Target (Ratio = 1)')

    # Step 7: Set labels and title
    plt.xlabel('Region', fontsize=12)
    plt.ylabel('Performance Ratio', fontsize=12)
    plt.title('Sales Team Performance by Region', fontsize=15)
    plt.xticks(rotation=45, ha='right')

    # Step 8: Add a legend
    plt.legend()

    # Step 9: Adjust layout and show the plot
    plt.tight_layout()
    plt.show()

def plot_sales_rep_performance_with_profit(sales_rep_performance_pd_df):
    """
    Function to aggregate sales data by sales representative, calculate performance ratio, profit,
    and plot the performance while identifying the top performer based on profit.
    
    Parameters:
    sales_team_df: Spark DataFrame - DataFrame containing sales performance data
    """
   

    # Step 1: Define a colormap for different shades of blue
    cmap = get_cmap('Greens')
    colors = cmap(np.linspace(0, 1, len(sales_rep_performance_pd_df)))[::-1]

    # Step 2: Plotting the performance by sales representative
    plt.figure(figsize=(12, 8))
    bars = plt.bar(
        sales_rep_performance_pd_df['Name'],
        sales_rep_performance_pd_df['Performance_Ratio'],
        color=colors,
        edgecolor='black'
    )

    # Step 3: Add performance ratio labels on top of the bars
    for bar in bars:
        yval = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2, 
            yval + 0.01, 
            round(yval, 3),
            va='bottom', 
            ha='center', 
            fontsize=10,
            rotation=90
        )

    # Step 4: Add a horizontal line at performance ratio of 1
    plt.axhline(y=1, color='red', linestyle='--', linewidth=2, label='Performance Target (Ratio = 1)')

    # Step 5: Set labels and title
    plt.xlabel('Sales Representative', fontsize=12)
    plt.ylabel('Performance Ratio', fontsize=12)
    plt.title('Sales Performance by Representative (Sorted by Profit)', fontsize=15)
    plt.xticks(rotation=45, ha='right')

    # Step 6: Add a legend
    plt.legend()

    # Step 7: Adjust layout and show the plot
    plt.tight_layout()
    plt.show()


def plot_interaction_counts_by_country(data):
    """
    Function to plot interaction counts by country and interaction type for top countries.

    Parameters:
    data: Pandas DataFrame - DataFrame containing interaction counts with columns for each interaction type
    """
    # Define a colormap for shades of blue
    cmap = get_cmap('Greens')
    num_interactions = len(data.columns)  # Number of interaction types
    colors = cmap(np.linspace(0.4, 1, num_interactions))  # Generate shades of blue
    
    # Set up the figure and axis
    plt.figure(figsize=(14, 10))
    bar_width = 0.25  # Width of bars
    
    # Create a list of positions for bars on x-axis
    r1 = np.arange(len(data))
    positions = [r1 + i * bar_width for i in range(num_interactions)]
    
    # Plot bars for each interaction type with different shades of blue
    bars = []
    for i, interaction_type in enumerate(data.columns):
        bars.append(
            plt.bar(
                positions[i], 
                data[interaction_type], 
                color=colors[i], 
                width=bar_width, 
                edgecolor='grey', 
                label=interaction_type
            )
        )

    # Add counts on top of the bars
    for i in range(len(r1)):
        for j, interaction_type in enumerate(data.columns):
            plt.text(
                positions[j][i], 
                data[interaction_type][i] + 0.1, 
                str(int(data[interaction_type][i])), 
                ha='center', 
                fontsize=10, 
                color='black'
            )

    # Add titles and labels
    plt.title('Top 20 Countries by Interaction Type Count', fontsize=16)
    plt.ylabel('Number of Interactions', fontweight='bold')
    plt.xlabel('Country', fontweight='bold')

    # Set the position of the x ticks
    plt.xticks(
        [r + (bar_width * (num_interactions - 1) / 2) for r in r1], 
        data.index, 
        rotation=45, 
        ha='right'
    )

    # Display the legend
    plt.legend(title='Interaction Type', bbox_to_anchor=(1.05, 1), loc='upper left')

    # Adjust layout to fit labels and show the plot
    plt.tight_layout()
    plt.show()