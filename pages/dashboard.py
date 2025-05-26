import taipy.gui.builder as tgb
from taipy.gui import State, Gui # Added State for type hinting callbacks, Gui for main
from taipy import Scenario # For type hinting scenario object
import pandas as pd
from datetime import datetime
import sys
import os
from typing import List, Dict, Tuple, Optional, Any # For type hinting

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import specific column name constants used in this file
from config.config import SITES_COLUMN, SITE_AWS_DISPLAY_COLUMN, AWS_ID_COLUMN
from scenarios.pipeline import create_scenario_instance

# --- Global State Variables Initialization ---
# scenario will hold the Taipy Scenario object, providing access to data nodes
scenario: Scenario = create_scenario_instance()

# Full list of available site names for the site selector's list of values (lov)
site_names_lov: List[str] = scenario.site_names.read()

# Selected date range [start_date, end_date] for the date_range selector
# Taipy GUI will bind the selector directly to the 'dates' global variable.
dates: Tuple[datetime, datetime] = scenario.dates.read()

# Currently selected parameters by the user (list of strings)
# Bound to the 'value' of the parameter selector.
parameters: List[str] = scenario.parameters.read() 

# Full list of available parameters for the parameter selector's lov
all_parameters_list: List[str] = scenario.all_parameters_list.read() 

# Plotly figure object for the chart display
figure: Any = scenario.figure.read() # Using Any for Plotly figure type

# --- Variables bound to GUI selectors and related internal state ---
# Holds the currently selected site names (list of strings) from the site selector. Bound to 'value'.
site_names: List[str] = [] 
# Holds the list of AWS display names for the AWS selector's lov, dependent on selected sites. Bound to 'lov'.
Aws: List[str] = [] 
# Holds the currently selected AWS display name(s) (list of strings) from the AWS selector. Bound to 'value'.
Aws_default: List[str] = [] 
# Holds the mapping from AWS display name (str) to AWS ID (str). Internal state.
aws_name_to_id_map: Dict[str, str] = {} 

# --- Initial UI setup based on scenario's default selections ---
# This block ensures the GUI reflects any default selections set in the scenario creation.
selected_sites_initial: Optional[List[str]] = scenario.selected_sites.read() 
if selected_sites_initial:
    site_names = selected_sites_initial # Update GUI state for site selector's 'value'

    sites_data_initial: pd.DataFrame = scenario.sites_data.read()
    relevant_sites_df_initial: pd.DataFrame = sites_data_initial[
        sites_data_initial[SITES_COLUMN].isin(selected_sites_initial)
    ]
    
    Aws = relevant_sites_df_initial[SITE_AWS_DISPLAY_COLUMN].unique().tolist() # Update 'lov' for AWS selector
    Aws_default = [Aws[0]] if Aws else [] # Update 'value' for AWS selector

    aws_name_to_id_map = pd.Series( # Update internal mapping state
        relevant_sites_df_initial[AWS_ID_COLUMN].values, 
        index=relevant_sites_df_initial[SITE_AWS_DISPLAY_COLUMN]
    ).to_dict()
    
    # If default AWS names were determined, translate to IDs and update scenario's 'selected_aws' data node
    initial_selected_aws_ids: List[str] = []
    for name in Aws_default:
        if name in aws_name_to_id_map:
            initial_selected_aws_ids.append(aws_name_to_id_map[name])
    
    if initial_selected_aws_ids: 
        scenario.selected_aws.write(initial_selected_aws_ids)
        scenario.submit() # Re-run scenario with these initial AWS selections
        figure = scenario.figure.read() # Refresh figure
        # Note: The 'parameters' global variable is already initialized from scenario.parameters.read() above.
        # If scenario.parameters was also affected by the submit, 'parameters' would need scenario.parameters.read() again.
        # However, selected_aws primarily affects 'filtered_data' and 'figure'.

# --- Callback Functions ---
def update_aws_state_and_scenario(state: State) -> None:
    """
    Updates the 'selected_aws' data node in the scenario based on GUI's AWS selection.

    Reads the currently selected AWS display names from `state.Aws_default`,
    converts them to AWS IDs using `state.aws_name_to_id_map`, and writes
    these IDs to the `state.scenario.selected_aws` data node.

    Args:
        state (State): The Taipy GUI state object, providing access to all global variables.
    """
    selected_aws_display_names: List[str] = state.Aws_default
    if not isinstance(selected_aws_display_names, list): # Should be a list due to multiple=True
        selected_aws_display_names = [selected_aws_display_names] if selected_aws_display_names else []

    selected_aws_ids: List[str] = []
    current_map: Dict[str, str] = state.aws_name_to_id_map

    for name in selected_aws_display_names:
        if name in current_map:
            selected_aws_ids.append(current_map[name])
        # else: print(f"DEBUG: AWS name '{name}' not found in map during update_aws_state_and_scenario.")

    state.scenario.selected_aws.write(selected_aws_ids)


def on_site_selection(state: State) -> None:
    """
    Handles changes in site selection (bound to 'site_names' state variable). 
    Updates the list of available AWS sites ('Aws' state variable),
    the AWS name-to-ID map ('aws_name_to_id_map' state variable), 
    and the default AWS selection ('Aws_default' state variable). 
    Triggers scenario submission to update data and figure.

    Args:
        state (State): The Taipy GUI state object. `state.site_names` contains selected sites.
    """
    try:
        sites_data: pd.DataFrame = state.scenario.sites_data.read()
        # It's good practice to check if data is as expected, though Taipy should handle node availability.
        if not isinstance(sites_data, pd.DataFrame):
            print("Error: sites_data is not a DataFrame in on_site_selection.")
            # Optionally, set an error message in Taipy state to display on GUI
            return 
            
        current_selected_sites: List[str] = state.site_names # This is already updated by Taipy
        state.scenario.selected_sites.write(current_selected_sites) 
        
        if current_selected_sites:
            relevant_sites_df: pd.DataFrame = sites_data[sites_data[SITES_COLUMN].isin(current_selected_sites)]
            aws_display_names: List[str] = relevant_sites_df[SITE_AWS_DISPLAY_COLUMN].unique().tolist()
            
            state.Aws = aws_display_names 
            state.aws_name_to_id_map = pd.Series(
                relevant_sites_df[AWS_ID_COLUMN].values, 
                index=relevant_sites_df[SITE_AWS_DISPLAY_COLUMN]
            ).to_dict()
            
            new_aws_default: List[str] = [aws_display_names[0]] if aws_display_names else []
            state.Aws_default = new_aws_default # This will update the AWS selector's displayed value
        else:
            state.Aws = []
            state.Aws_default = []
            state.aws_name_to_id_map = {}
        
        update_aws_state_and_scenario(state) 
        state.scenario.submit() 
        state.figure = state.scenario.figure.read() 
        
    except Exception as e:
        print(f"Error in on_site_selection: {str(e)}")
        # Consider adding a user-facing error message via a Taipy state variable e.g. state.error_message = "..."
        raise # Re-raising can be helpful for debugging but might halt execution in some setups.

def on_aws_selection(state: State) -> None:
    """
    Handles changes in AWS selection (bound to 'Aws_default' state variable). 
    Updates the scenario's 'selected_aws' data node and refreshes the figure.

    Args:
        state (State): The Taipy GUI state object. `state.Aws_default` contains selected AWS names.
    """
    try:
        # 'state.Aws_default' is already updated by Taipy GUI due to the selector's 'value' binding.
        update_aws_state_and_scenario(state) 
        state.scenario.submit() 
        state.figure = state.scenario.figure.read() 
    except Exception as e:
        print(f"Error in on_aws_selection: {str(e)}")
        raise

def on_date_change(state: State) -> None:
    """
    Handles changes in the selected date range (bound to 'dates' state variable). 
    Updates the scenario's 'dates' data node and refreshes the figure.

    Args:
        state (State): The Taipy GUI state object. `state.dates` contains the selected (start_date, end_date) tuple.
    """
    try:
        # 'state.dates' is already updated by Taipy GUI.
        state.scenario.dates.write(state.dates) 
        state.scenario.submit()
        state.figure = state.scenario.figure.read()
    except Exception as e:
        print(f"Error in on_date_change: {str(e)}")
        raise

def on_parameter_change(state: State) -> None:
    """
    Handles changes in parameter selection (bound to 'parameters' state variable). 
    Updates the scenario's 'parameters' data node and refreshes the figure.

    Args:
        state (State): The Taipy GUI state object. `state.parameters` contains the list of selected parameters.
    """
    try:
        # 'state.parameters' is already updated by Taipy GUI.
        state.scenario.parameters.write(state.parameters) 
        state.scenario.submit()
        state.figure = state.scenario.figure.read()
    except Exception as e:
        print(f"Error in on_parameter_change: {str(e)}")
        raise

# --- GUI Layout Definition ---
# Using tgb (Taipy GUI Builder) for a more structured layout.
# This defines the visual structure of the page.
with tgb.Page() as page:
    # Main title for the dashboard
    tgb.text("# Marine Weather Dashboard", mode="md", class_name="text-center mt-4 mb-4")

    # Date range selector
    tgb.date_range(
        value="{dates}", # Binds to the 'dates' global variable
        label_start="Start Date",
        label_end="End Date",
        class_name="mb-3",
        on_change=on_date_change
    )
    
    # Layout for Site and AWS selectors side-by-side
    with tgb.layout("1 1", class_name="mb-3"): 
        tgb.selector(
            label="Site",
            class_name="fullwidth", 
            value="{site_names}", # Binds to 'site_names' (selected sites)
            lov="{site_names_lov}", # Binds to 'site_names_lov' (all available sites)
            dropdown=True, 
            multiple=True,
            on_change=on_site_selection
        )
        tgb.selector(
            label="AWS",
            class_name="fullwidth", 
            value="{Aws_default}", # Binds to 'Aws_default' (selected AWS names)
            lov="{Aws}", # Binds to 'Aws' (available AWS names for selected site)      
            dropdown=True, 
            multiple=True,
            on_change=on_aws_selection
        )
    
    # Parameter selector
    tgb.selector(
        label="Parameters",
        class_name="fullwidth mb-4", 
        lov="{all_parameters_list}", # Binds to 'all_parameters_list' (all available parameters)
        value="{parameters}", # Binds to 'parameters' (selected parameters)    
        dropdown=True,
        multiple=True,
        on_change=on_parameter_change
    )
    
    # Chart display
    tgb.chart(figure="{figure}", class_name="mb-4") # Binds to 'figure'

# --- Main application execution ---
if __name__ == "__main__":
    gui: Gui = Gui(page=page)
    # Taipy GUI automatically picks up global variables as state.
    # `run` starts the web server and makes the dashboard accessible.
    gui.run(title="Weather Dashboard", dark_mode=True, use_reloader=True, margin="1em")
