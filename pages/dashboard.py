import taipy.gui.builder as tgb
import pandas as pd
from datetime import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.config import scenario_cfg, SITES_DATA_PATH, WEATHER_DATA_PATH
from scenarios.pipeline import create_scenario_instance

# Initialize scenario
scenario = create_scenario_instance()
def on_site_selection(state):
    """Handle site selection change"""
    try:
        # Ensure we're working with the scenario's data
        sites_data = state.scenario.sites_data.read()
        
        # Check if sites_data is a DataFrame
        if not isinstance(sites_data, pd.DataFrame):
            raise ValueError("sites_data is not a DataFrame")
            
        # Write selected sites to scenario
        selected_sites = state.site_names if state.site_names else None
        state.scenario.selected_sites.write(selected_sites)
        
        # Update AWS list based on selected sites
        if selected_sites:
            # Make sure column names match exactly what's in your CSV
            aws_list = sites_data.loc[sites_data['Sites'].isin(selected_sites), 'AWS'].tolist()
            state.Aws = aws_list
            state.Aws_default = aws_list[0] if aws_list else None
        else:
            state.Aws = []
            state.Aws_default = None
        
        # Submit scenario to update data
        state.scenario.submit()
        state.figure = state.scenario.figure.read()
        
    except Exception as e:
        print(f"Error in on_site_selection: {str(e)}")
        raise
def on_aws_selection(state):
    """Handle AWS selection change"""
    state.scenario.selected_aws.write(
        state.Aws_default if isinstance(state.Aws_default, list) else [state.Aws_default] if state.Aws_default else []
    )
    state.scenario.submit()
    state.figure = state.scenario.figure.read()

def on_date_change(state):
    """Handle date range change"""
    state.scenario.dates.write((state.dates[0], state.dates[1]))
    state.scenario.submit()
    state.figure = state.scenario.figure.read()

def on_parameter_change(state):
    """Handle parameter selection change"""
    state.scenario.parameters.write(state.parameters)
    state.scenario.submit()
    state.figure = state.scenario.figure.read()

# Initial parameters
site_names = scenario.site_names.read()
dates = scenario.dates.read()
parameters = scenario.parameters.read()
figure = scenario.figure.read()

# Initialize AWS lists
Aws = []
Aws_default = None

# Check if any sites are selected by default and set AWS accordingly
selected_sites = scenario.selected_sites.read()
if selected_sites:
    sites_data = scenario.sites_data.read()
    Aws = sites_data[sites_data['Sites'].isin(selected_sites)]['AWS ID'].tolist()
    Aws_default = Aws[0] if Aws else None

with tgb.Page() as page:
    with tgb.part("text-center"):
        tgb.image("Weatherdash.png", width="10vw")
        tgb.text("# Marine Weather Dashboard", mode="md")
        
        tgb.date_range(
            "{dates}",
            label_start="Start Date",
            label_end="End Date",
            on_change=on_date_change
        )
        
        with tgb.layout("20 80"):
            tgb.selector(
                label="Site",
                class_name="fullwidth",
                value="{site_names}",
                lov=site_names,
                dropdown=True,
                value_by_id=True,
                multiple=True,
                on_change=on_site_selection
            )
            
            tgb.selector(
                label="AWS",
                class_name="fullwidth",
                value="{Aws_default}",
                lov="{Aws}",
                dropdown=True,
                value_by_id=True,
                multiple=True,
                on_change=on_aws_selection
            )
        
        tgb.selector(
            label="Parameters",
            #lov=["temperature_air_20_seconds", "pressure", "gust", "wind"],
            lov=["temperature_air_20_seconds", "temperature_sea_surface_20_seconds", 
                "pressure_20_seconds", "humidity_relative_20_seconds", 
                "wind_speed_10_minute", "wind_gust_speed_10_minute", 
                "wind_direction_10_minute"],
            value="{parameters}",
            dropdown=True,
            multiple=True,
            on_change=on_parameter_change
        )
        
        tgb.chart(figure="{figure}")

if __name__ == "__main__":
    from taipy.gui import Gui
    Gui(page=page).run(title="Weather Dashboard", dark_mode=True, use_reloader=True)
