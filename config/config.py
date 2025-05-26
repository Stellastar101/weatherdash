from taipy import Config, Scope
from taipy.core.config.data_node_config import DataNodeConfig
from taipy.core.config.task_config import TaskConfig
from taipy.core.config.scenario_config import ScenarioConfig
import os
import pandas as pd
from datetime import datetime
from typing import List, Tuple, Union, Any, Optional # Added for type hinting

# === Column Name Constants ===
SITES_COLUMN: str = 'Sites'
SITE_AWS_DISPLAY_COLUMN: str = 'AWS' # Column in site_data.csv for AWS display name
AWS_ID_COLUMN: str = 'AWS ID'        # Column in site_data.csv and weather_data.csv for AWS unique identifier
DATE_COLUMN: str = 'date'            # Standard name for date columns after processing

# === Define file paths ===
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SITES_DATA_PATH: str = os.path.join(PROJECT_ROOT, 'data', 'site_data.csv')
WEATHER_DATA_PATH: str = os.path.join(PROJECT_ROOT, 'data', 'weather_data.csv')

# === Load data ===
sites_df: pd.DataFrame = pd.read_csv(SITES_DATA_PATH)

weather_df: pd.DataFrame = pd.read_csv(WEATHER_DATA_PATH)
weather_df[DATE_COLUMN] = pd.to_datetime(weather_df[['Year', 'Month', 'Day', 'Hour', 'Minute']])
parameters_all_available: List[str] = weather_df.columns.tolist()[10:]


# === Configure Data Nodes ===
sites_data_cfg: DataNodeConfig = Config.configure_data_node(id="sites_data", default_data=SITES_DATA_PATH, scope=Scope.GLOBAL)
weather_data_cfg: DataNodeConfig = Config.configure_data_node(id="weather_data", default_data=WEATHER_DATA_PATH, scope=Scope.GLOBAL)

site_names_cfg: DataNodeConfig = Config.configure_data_node(id="site_names", scope=Scope.SCENARIO) 
dates_cfg: DataNodeConfig = Config.configure_data_node(id="dates", scope=Scope.SCENARIO) 
selected_sites_cfg: DataNodeConfig = Config.configure_data_node(id="selected_sites", default_data=None, scope=Scope.SCENARIO) 
selected_aws_cfg: DataNodeConfig = Config.configure_data_node(id="selected_aws", default_data=[], scope=Scope.SCENARIO) 

parameters_cfg: DataNodeConfig = Config.configure_data_node(
    id="parameters", 
    default_data=[parameters_all_available[0]] if parameters_all_available else [], 
    scope=Scope.SCENARIO
)
all_parameters_list_cfg: DataNodeConfig = Config.configure_data_node(
    id="all_parameters_list", 
    default_data=parameters_all_available, 
    scope=Scope.SCENARIO 
)

filtered_data_cfg: DataNodeConfig = Config.configure_data_node(id="filtered_data", scope=Scope.SCENARIO) 
figure_cfg: DataNodeConfig = Config.configure_data_node(id="figure", scope=Scope.SCENARIO)

# === Tasks ===
def preprocess_data_task(
    weather_data: Union[str, pd.DataFrame], 
    sites_data: Union[str, pd.DataFrame]
) -> Tuple[List[str], Tuple[datetime, datetime]]:
    """
    Initial processing task to extract unique site names and the overall date range from weather data.

    Args:
        weather_data (Union[str, pd.DataFrame]): Path to weather data CSV or loaded DataFrame.
        sites_data (Union[str, pd.DataFrame]): Path to sites data CSV or loaded DataFrame.

    Returns:
        Tuple[List[str], Tuple[datetime, datetime]]: 
            - A list of unique site names.
            - A tuple with the minimum and maximum dates found in the weather data.
    """
    df_weather: pd.DataFrame
    if isinstance(weather_data, str):
        df_weather = pd.read_csv(weather_data)
        df_weather[DATE_COLUMN] = pd.to_datetime(df_weather[['Year', 'Month', 'Day', 'Hour', 'Minute']])
    else:
        df_weather = weather_data
        if DATE_COLUMN not in df_weather.columns or not pd.api.types.is_datetime64_any_dtype(df_weather[DATE_COLUMN]):
             df_weather[DATE_COLUMN] = pd.to_datetime(df_weather[['Year', 'Month', 'Day', 'Hour', 'Minute']])
    
    df_sites: pd.DataFrame
    if isinstance(sites_data, str):
        df_sites = pd.read_csv(sites_data)
    else:
        df_sites = sites_data
    
    unique_site_names: List[str] = df_sites[SITES_COLUMN].unique().tolist()
    date_range: Tuple[datetime, datetime] = (df_weather[DATE_COLUMN].min(), df_weather[DATE_COLUMN].max())
    
    return unique_site_names, date_range

preprocess_task_cfg: TaskConfig = Config.configure_task(
    id="preprocess_data",
    function=preprocess_data_task,
    input=[weather_data_cfg, sites_data_cfg],
    output=[site_names_cfg, dates_cfg]
)

def filter_data_task(
    sites_data: Union[str, pd.DataFrame], 
    weather_data: Union[str, pd.DataFrame], 
    selected_sites: Optional[List[str]], 
    selected_aws: Optional[List[str]], 
    dates: Optional[Tuple[Optional[datetime], Optional[datetime]]], 
    parameters: Optional[List[str]]
) -> pd.DataFrame:
    """
    Filters weather data based on user selections for sites, AWS, date range, and parameters.

    Args:
        sites_data (Union[str, pd.DataFrame]): Path to sites data CSV or loaded DataFrame.
        weather_data (Union[str, pd.DataFrame]): Path to weather data CSV or loaded DataFrame.
        selected_sites (Optional[List[str]]): List of selected site names.
        selected_aws (Optional[List[str]]): List of selected AWS IDs.
        dates (Optional[Tuple[Optional[datetime], Optional[datetime]]]): Tuple with start and end dates.
        parameters (Optional[List[str]]): List of selected parameters to include.

    Returns:
        pd.DataFrame: Filtered weather data.
    """
    df_weather: pd.DataFrame
    if isinstance(weather_data, str):
        df_weather = pd.read_csv(weather_data)
        df_weather[DATE_COLUMN] = pd.to_datetime(df_weather[['Year', 'Month', 'Day', 'Hour', 'Minute']])
    else:
        df_weather = weather_data
        if DATE_COLUMN not in df_weather.columns or not pd.api.types.is_datetime64_any_dtype(df_weather[DATE_COLUMN]):
             df_weather[DATE_COLUMN] = pd.to_datetime(df_weather[['Year', 'Month', 'Day', 'Hour', 'Minute']])

    df_sites: pd.DataFrame
    if isinstance(sites_data, str):
        df_sites = pd.read_csv(sites_data)
    else:
        df_sites = sites_data
    
    filtered_df: pd.DataFrame = df_weather.copy()
    
    if selected_sites: # Ensure selected_sites is not None and not empty
        aws_ids_from_sites: List[str] = df_sites[df_sites[SITES_COLUMN].isin(selected_sites)][AWS_ID_COLUMN].tolist()
        filtered_df = filtered_df[filtered_df[AWS_ID_COLUMN].isin(aws_ids_from_sites)]
    
    if selected_aws: # Ensure selected_aws is not None and not empty
        filtered_df = filtered_df[filtered_df[AWS_ID_COLUMN].isin(selected_aws)]
    
    if dates and dates[0] is not None and dates[1] is not None:
        filtered_df = filtered_df[(filtered_df[DATE_COLUMN] >= dates[0]) & (filtered_df[DATE_COLUMN] <= dates[1])]
    
    valid_params: List[str] = []
    if parameters: 
        valid_params = [p for p in parameters if p in filtered_df.columns]
    
    columns_to_return = [DATE_COLUMN, AWS_ID_COLUMN] + valid_params
    # Ensure only existing columns are selected to avoid KeyError
    columns_to_return = [col for col in columns_to_return if col in filtered_df.columns]
    return filtered_df[columns_to_return]

filter_data_task_cfg: TaskConfig = Config.configure_task(
    id="filter_data",
    function=filter_data_task,
    input=[sites_data_cfg, weather_data_cfg, selected_sites_cfg, selected_aws_cfg, dates_cfg, parameters_cfg],
    output=filtered_data_cfg,
    skippable=True
)

def create_figure_task(
    filtered_data: Optional[pd.DataFrame], 
    parameters: Optional[List[str]]
) -> Any: # Using Any for plotly.graph_objects.Figure for simplicity with worker
    """
    Creates a Plotly figure from the filtered weather data for the selected parameters.

    Args:
        filtered_data (Optional[pd.DataFrame]): DataFrame of filtered weather data.
        parameters (Optional[List[str]]): List of selected parameters to plot.

    Returns:
        Any: A Plotly Figure object. Empty figure with a message if no data/valid parameters.
    """
    import plotly.graph_objects as go 
    
    fig = go.Figure()

    if filtered_data is None or filtered_data.empty:
        fig.update_layout(title_text="No data available for selected filters") # Use title_text
        return fig
    
    if DATE_COLUMN in filtered_data.columns and not pd.api.types.is_datetime64_any_dtype(filtered_data[DATE_COLUMN]):
        filtered_data[DATE_COLUMN] = pd.to_datetime(filtered_data[DATE_COLUMN])
            
    valid_params: List[str] = []
    if parameters:
        valid_params = [p for p in parameters if p in filtered_data.columns]
    
    if not valid_params:
        fig.update_layout(title_text="No valid parameters selected or parameters not in data") # Use title_text
        return fig
        
    for aws_id_val, group in filtered_data.groupby(AWS_ID_COLUMN):
        for param in valid_params:
            fig.add_trace(go.Scatter(x=group[DATE_COLUMN], y=group[param], mode='lines', name=f"{aws_id_val} - {param}"))
    
    fig.update_layout(
        title_text="Weather Parameters Over Time", # Use title_text
        xaxis_title="Date", 
        yaxis_title="Value", 
        legend_title_text="AWS & Parameter" # Use legend_title_text
    )
    return fig

create_figure_task_cfg: TaskConfig = Config.configure_task(
    id="create_figure",
    function=create_figure_task,
    input=[filtered_data_cfg, parameters_cfg],
    output=figure_cfg,
    skippable=True
)

# === Scenario Configuration ===
scenario_cfg: ScenarioConfig = Config.configure_scenario(
    id="weather_scenario",
    task_configs=[preprocess_task_cfg, filter_data_task_cfg, create_figure_task_cfg],
    # Data nodes are automatically added to scenario if used as input/output of tasks.
    # Explicitly adding them via additional_data_node_configs is also possible if they are not part of any task.
    # For instance, all_parameters_list_cfg is used by GUI but not directly by a task here.
    additional_data_node_configs=[all_parameters_list_cfg] 
)

Config.configure_global_app(
    clean_entities_enabled=True, 
    pandas_format="pickle"
)

__all__ = ['scenario_cfg', 'sites_df', 'weather_df', 'parameters_all_available']
