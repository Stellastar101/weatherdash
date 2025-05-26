from taipy import Config, Scope
import os
import pandas as pd
from datetime import datetime

# === Define file paths ===
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SITES_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'site_data.csv')
WEATHER_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'weather_data.csv')

# === Load data (these will be imported from other modules) ===
sites_df = pd.read_csv(SITES_DATA_PATH)


#print(sites_df)
weather_df = pd.read_csv(WEATHER_DATA_PATH)
weather_df['date'] = pd.to_datetime(weather_df[['Year', 'Month', 'Day', 'Hour', 'Minute']])
parameters = weather_df.columns.tolist()[10:]


# === Configure Data Nodes ===
# Use file paths rather than dataframes directly
sites_data_cfg = Config.configure_data_node(id="sites_data", default_data=SITES_DATA_PATH)
weather_data_cfg = Config.configure_data_node(id="weather_data", default_data=WEATHER_DATA_PATH)

site_names_cfg = Config.configure_data_node(id="site_names", default_data=None)
dates_cfg = Config.configure_data_node(id="dates", default_data=None)
selected_sites_cfg = Config.configure_data_node(id="selected_sites", default_data=None)
selected_aws_cfg = Config.configure_data_node(id="selected_aws", default_data=["6369"])  # Default to empty list
#parameters_cfg = Config.configure_data_node(id="parameters", default_data=["temperature_air_20_seconds"])
parameters_cfg = Config.configure_data_node(id="parameters", default_data=parameters[2])
filtered_data_cfg = Config.configure_data_node(id="filtered_data")
figure_cfg = Config.configure_data_node(id="figure")

# === Tasks ===
def preprocess_data_task(weather_data, sites_data):
    """Initial processing task to extract site names and date range"""
    if isinstance(weather_data, str):
        weather_data = pd.read_csv(weather_data)
        weather_data['date'] = pd.to_datetime(weather_data[['Year', 'Month', 'Day', 'Hour', 'Minute']])
    if isinstance(sites_data, str):
        sites_data = pd.read_csv(sites_data)
    
    return sites_data['Sites'].unique().tolist(), (weather_data['date'].min(), weather_data['date'].max())

preprocess_task_cfg = Config.configure_task(
    id="preprocess_data",
    function=preprocess_data_task,
    input=[weather_data_cfg, sites_data_cfg],
    output=[site_names_cfg, dates_cfg]
)

def filter_data_task(sites_data, weather_data, selected_sites, selected_aws, dates, parameters):
    """Filter weather data based on selections"""
    if isinstance(weather_data, str):
        weather_data = pd.read_csv(weather_data)
        weather_data['date'] = pd.to_datetime(weather_data[['Year', 'Month', 'Day', 'Hour', 'Minute']])
    if isinstance(sites_data, str):
        sites_data = pd.read_csv(sites_data)
    
    # Start with all data
    filtered = weather_data.copy()
    
    # Filter by sites if selected
    if selected_sites:
        aws_list = sites_data[sites_data['Sites'].isin(selected_sites)]['AWS ID'].tolist()
        filtered = filtered[filtered['AWS ID'].isin(aws_list)]
    
    # Filter by AWS if selected
    if selected_aws and len(selected_aws) > 0:
        filtered = filtered[filtered['AWS ID'].isin(selected_aws)]
    
    # Filter by date range
    if dates and len(dates) == 2:
        filtered = filtered[(filtered['date'] >= dates[0]) & (filtered['date'] <= dates[1])]
    
    # Ensure parameters exist in the dataframe
    valid_params = [p for p in parameters if p in filtered.columns]
    
    # Return filtered data with required columns
    return filtered[['date', 'AWS ID'] + valid_params]

filter_data_task_cfg = Config.configure_task(
    id="filter_data",
    function=filter_data_task,
    input=[sites_data_cfg, weather_data_cfg, selected_sites_cfg, selected_aws_cfg, dates_cfg, parameters_cfg],
    output=filtered_data_cfg,
    skippable=True
)

def create_figure_task(filtered_data, parameters):
    """Create a plotly figure from filtered data"""
    import plotly.graph_objects as go
    
    if filtered_data is None or (isinstance(filtered_data, pd.DataFrame) and filtered_data.empty):
        fig = go.Figure()
        fig.update_layout(title="No data available for selected filters")
        return fig
    
    if isinstance(filtered_data, str):
        filtered_data = pd.read_csv(filtered_data)
        if 'date' in filtered_data.columns:
            filtered_data['date'] = pd.to_datetime(filtered_data['date'])
    
    fig = go.Figure()
    
    # Ensure parameters exist in the dataframe
    valid_params = [p for p in parameters if p in filtered_data.columns]
    
    if not valid_params:
        fig.update_layout(title="No valid parameters selected")
        return fig
    
    for aws, group in filtered_data.groupby('AWS ID'):
        for param in valid_params:
            fig.add_trace(go.Scatter(x=group['date'], y=group[param], mode='lines', name=f"{aws} - {param}"))
    
    fig.update_layout(
        title="Weather Parameters Over Time", 
        xaxis_title="Date", 
        yaxis_title="Value", 
        legend_title="AWS & Parameter"
    )
    
    return fig

create_figure_task_cfg = Config.configure_task(
    id="create_figure",
    function=create_figure_task,
    input=[filtered_data_cfg, parameters_cfg],
    output=figure_cfg,
    skippable=True
)

# === Scenario Configuration ===
scenario_cfg = Config.configure_scenario(
    id="weather_scenario",
    task_configs=[preprocess_task_cfg, filter_data_task_cfg, create_figure_task_cfg]
)

#Config.configure_global_behavior(
#    pandas_format="pickle"  # Ensures better DataFrame serialization

Config.configure_global_app(
    pandas_format="pickle"  # Ensures better DataFrame serialization
)
# === Make DataFrames importable ===
__all__ = ['scenario_cfg', 'sites_df', 'weather_df']
