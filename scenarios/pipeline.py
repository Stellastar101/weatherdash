from taipy import Config, create_scenario, run, Scenario
import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import List

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Imports from config.config
from config.config import (
    scenario_cfg, 
    SITES_DATA_PATH, 
    parameters_all_available, 
    SITES_COLUMN
)

def create_scenario_instance() -> Scenario:
    """
    Creates and initializes a Taipy Scenario instance with default data node values.

    This function initializes the Taipy Core, creates a scenario based on the 
    configuration defined in `config.config`, populates essential data nodes 
    (like site names, date ranges, and parameter lists) with default values, 
    and then submits the scenario to trigger initial task executions.

    Returns:
        Scenario: The initialized Taipy Scenario object.
    """
    run() # Initialize Taipy Core, ensuring it's ready.
    
    # Create a new scenario from the configured scenario_cfg
    scenario: Scenario = create_scenario(scenario_cfg)
    
    # Load sites data to populate initial site names list for the selector
    sites_df: pd.DataFrame = pd.read_csv(SITES_DATA_PATH)
    
    # Initialize data nodes with default values:
    # Populate 'site_names' data node (list of all site names for GUI selector's lov)
    site_names_list: List[str] = sites_df[SITES_COLUMN].unique().tolist()
    scenario.site_names.write(site_names_list)
    
    # Populate 'dates' data node (default date range for GUI date_range selector)
    end_date: datetime = datetime.now()
    start_date: datetime = end_date - timedelta(days=30)
    scenario.dates.write((start_date, end_date))
    
    # Populate 'selected_sites' data node (default value for site selector - initially no sites selected)
    scenario.selected_sites.write(None) 
    
    # Populate 'selected_aws' data node (default value for AWS selector - initially no AWS selected)
    scenario.selected_aws.write([])   
    
    # Populate 'all_parameters_list' data node (list of all available parameters for GUI parameter selector's lov)
    # parameters_all_available is imported from config.config
    scenario.all_parameters_list.write(parameters_all_available)
    
    # Populate 'parameters' data node (default value for parameter selector - currently selected parameters)
    # Defaults to a list containing the first available parameter, or an empty list.
    default_selected_parameters: List[str] = [parameters_all_available[0]] if parameters_all_available else []
    scenario.parameters.write(default_selected_parameters)
    
    # Submit the scenario. This executes tasks whose inputs are now available 
    # (e.g., preprocess_data if it depends only on global data nodes, 
    # then filter_data and create_figure based on the default selections just written).
    # This ensures that data nodes like 'figure' are populated for the initial GUI display.
    scenario.submit() 
    
    return scenario
