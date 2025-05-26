from taipy import Config, create_scenario, run
import sys
import os
import pandas as pd
from datetime import datetime, timedelta

# Ensure the parent directory is in the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.config import scenario_cfg, SITES_DATA_PATH, WEATHER_DATA_PATH

def create_scenario_instance():
    """Creates and initializes a scenario instance with all required data nodes"""
    # Initialize Taipy Core
    run()
    
    # Create scenario (this will use the default file paths from config)
    scenario = create_scenario(scenario_cfg)
    
    # Manually load the data
    sites_df = pd.read_csv(SITES_DATA_PATH)
    weather_df = pd.read_csv(WEATHER_DATA_PATH)
    weather_df['date'] = pd.to_datetime(weather_df[['Year', 'Month', 'Day', 'Hour', 'Minute']])
    
    # Initialize other data nodes with default values
    # Initial site names from sites data
    site_names = sites_df['Sites'].unique().tolist()
    scenario.site_names.write(site_names)
    
    # Default date range (last 30 days)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    scenario.dates.write((start_date, end_date))
    
    # Default selections
    scenario.selected_sites.write(None)
    scenario.selected_aws.write([])
    scenario.parameters.write(["temperature_air_20_seconds"])
    
    # Execute the initial tasks to generate filtered_data and figure
    scenario.submit()
    
    return scenario
