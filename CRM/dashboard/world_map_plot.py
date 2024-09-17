import plotly.express as px
import pandas as pd
import plotly.express as px
import streamlit as st

def plot_world_map(df, locations_col, color_col, plot_title, hover_data=None, is_discrete=True, color_scale=None, color_discrete_sequence=None):
    """
    A function to plot a world map with customizable settings for both discrete and continuous color scales.
    
    Parameters:
        df (pd.DataFrame): The data to plot.
        locations_col (str): The column in df that contains country names.
        color_col (str): The column in df that determines the color of the map.
        plot_title (str): The title of the plot.
        hover_data (list, optional): Additional columns to display on hover. Defaults to None.
        is_discrete (bool, optional): Whether the color scale is discrete (True) or continuous (False). Defaults to True.
        color_scale (list, optional): Continuous color scale if using continuous values. Defaults to None.
        color_discrete_sequence (list, optional): Discrete color sequence if using categorical values. Defaults to None.
    
    Returns:
        fig: The generated plotly figure.
    """
    
    if is_discrete:
        fig = px.choropleth(df,
                            locations=locations_col,
                            locationmode="country names",
                            color=color_col,
                            title=plot_title,
                            color_discrete_sequence=color_discrete_sequence if color_discrete_sequence else px.colors.sequential.Greens_r,
                            hover_data=hover_data
                           )
    else:
        fig = px.choropleth(df,
                            locations=locations_col,
                            locationmode="country names",
                            color=color_col,
                            title=plot_title,
                            color_continuous_scale=color_scale if color_scale else px.colors.sequential.Greens,
                            hover_data=hover_data
                           )
    
    # Update layout for better visualization
    fig.update_layout(
        geo=dict(
            showframe=False,
            showcoastlines=True,
            projection_type='equirectangular',
            showland=True,  # Show land
            landcolor="lightgray",  # Color of the land
            showocean=True,  # Show ocean
            oceancolor="lightblue",  # Color of the ocean
        ),
        title=dict(
            text=plot_title,
            x=0.5,  # Center the title
            xanchor='center',
            font=dict(size=20)  # Increase title size
        ),
        autosize=False,
        width=1000,  # Set width for larger plot
        height=700   # Set height for larger plot
    )
    
    st.plotly_chart(fig, use_container_width=True)
