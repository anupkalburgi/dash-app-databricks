import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
import plotly.express as px
from dash.dependencies import Input, Output
import pandas as pd
import dash_table
from db_sql import BrickSQLAlchemy, connection_string
from flask import Flask


brick = BrickSQLAlchemy(connection_string=connection_string)

# app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])
server = Flask(__name__)
app = dash.Dash(__name__, server=server, external_stylesheets=[dbc.themes.FLATLY])

# --------------------------------------------------------
# Data
# --------------------------------------------------------
df = px.data.gapminder()
csv_files = {}
for table_name in brick.get_table_names():
    csv_files[table_name] = table_name

print(f"CSV files: {csv_files}")
# --------------------------------------------------------
# Layout Components (Modular functions)
# --------------------------------------------------------


def make_aggrid_table():
    """
    Returns a dash_ag_grid.AgGrid component with the gapminder data.
    """
    return dcc.Loading(
        id="grid-loading",
        children=dag.AgGrid(
            id="data-table",
            columnDefs=[{"headerName": col, "field": col} for col in df.columns],
            rowData=[],
            dashGridOptions={"rowHeight": 32, "animateRows": False},
            defaultColDef={"filter": True, "editable": True},
            style={"height": "400px", "width": "100%"},
        ),
        
    )


def make_tab1_content():
    return dcc.Loading(
        dbc.Card(
            dbc.CardBody(
                [
                    html.H4("Tab 1 Table"),
                    dbc.Alert(id="empty-message-1", color="info", dismissable=True, is_open=False),  # Styled alert  # To show a message if no data
                    dash_table.DataTable(
                        id="table-tab1",
                        data=[],  # we’ll fill this via callback
                        page_size=10,
                    ),
                ]
            )
        )
    )


def make_tab2_content():
    return dcc.Loading(
        id="loading-2",
        children=dbc.Card(
            dbc.CardBody(
                [
                    html.H4("Tab 2 Table"),
                    dbc.Alert(id="empty-message-2", color="info", dismissable=True, is_open=False),
                    dash_table.DataTable(
                        id="table-tab2",
                        data=[],
                        page_size=10,
                    ),
                ]
            )
        ),
    )


def make_tab3_content():
    """
    Returns the content for Tab 3.
    """
    return dcc.Loading(
        id="loading-3",
        children=dbc.Card(
            dbc.CardBody(
                [
                    html.H4("Tab 3 Table"),
                    dbc.Alert(id="empty-message-3", color="info", dismissable=True, is_open=False),  # Styled alert  # To show a message if no data
                    dash_table.DataTable(
                        id="table-tab3",
                        data=[],
                        page_size=10,
                    ),
                ]
            )
        ),
    )


def make_tabs():
    return dbc.Tabs(
        [
            dbc.Tab(
                make_tab1_content(), label="check_duplicates", id="tab-1", disabled=True
            ),
            dbc.Tab(
                make_tab2_content(),
                label="check_negative_debits_credits",
                id="tab-2",
                disabled=True,
            ),
            dbc.Tab(
                make_tab3_content(),
                label="check_region_country_mismatch",
                id="tab-3",
                disabled=True,
            ),
        ],
        id="main-tabs",
        active_tab="tab-0",
        style={"margin-top": 30},
    )


def make_left_panel():
    """
    Returns the left panel (logo, instructions, radio items, dropdowns, image).
    """
    return html.Div(
        id="sidebar",
        children=[
            html.Div(
                [
                    html.Div(
                        dbc.RadioItems(
                            className="btn-group",
                            inputClassName="btn-check",
                            labelClassName="btn btn-outline-light",
                            labelCheckedClassName="btn btn-light",
                            options=[
                                {"label": "Table", "value": 2},
                            ],
                            value=1,
                            style={"width": "100%"},
                            id="radio-graph-or-table",
                        ),
                        style={"width": 206},
                    ),
                ],
                style={"margin-left": 15, "margin-right": 15, "display": "flex"},
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.H2("Select a Dataset:"),
                            dcc.Dropdown(
                                options=[
                                    {"label": name, "value": path}
                                    for name, path in csv_files.items()
                                ],
                                clearable=True,
                                optionHeight=40,
                                className="customDropdown",
                                id="dataset-dropdown",
                            ),
                        ]
                    ),
                    html.Div(
                        [
                            html.H2("Group By:"),
                            dcc.Dropdown(
                                options=[],
                                value=2,
                                clearable=False,
                                optionHeight=40,
                                className="customDropdown",
                                id="group-by-dropdown",
                            ),
                        ]
                    ),
                    html.Div(
                        [
                            html.H2("Aggregation Column:"),
                            dcc.Dropdown(
                                options=[],
                                value=None,
                                clearable=False,
                                className="customDropdown",
                                id="aggregate-column-dropdown",
                            ),
                        ]
                    ),
                    html.Div(
                        [
                            html.H2("Aggregation Function:"),
                            dcc.Dropdown(
                                options=[
                                    {"label": "SUM", "value": "SUM"},
                                    {"label": "AVG", "value": "AVG"},
                                    {"label": "COUNT", "value": "COUNT"},
                                    {"label": "MAX", "value": "MAX"},
                                    {"label": "MIN", "value": "MIN"},
                                ],
                                clearable=True,
                                optionHeight=40,
                                placeholder="Select an aggregation function",
                                className="customDropdown",
                                id="aggregation-function-dropdown",
                            ),
                        ]
                    ),
                ],
                style={"margin-left": 15, "margin-right": 15, "margin-top": 30},
            ),
        ],
        style={
            "width": 340,
            "margin-left": 35,
            "margin-top": 35,
            "margin-bottom": 35,
        },
    )


def make_main_content():
    """
    Returns the main content area (the table and the tabs).
    """
    return html.Div(
        [
            html.Div(
                [
                    make_aggrid_table(),  # The table
                    html.Pre(id="output-value-setter"),
                    make_tabs(),  # The tabs below the table
                    html.Div(
                        id="tab-switch-output", style={"marginTop": 20}
                    ),  # <--- placeholder
                ],
                style={
                    "width": "100%",
                    "min-width": 1300,
                    "margin-top": 20,
                    "margin-right": 0,
                    "margin-bottom": 35,
                },
            )
        ],
        style={"display": "flex"},
    )

def make_navbar():
    """
    Returns a NavbarSimple component at the top of the app.
    """
    return dbc.NavbarSimple(
        children=[
            dbc.NavItem(dbc.NavLink("About", href="#")),
            dbc.NavItem(dbc.NavLink("Help", href="#")),
        ],
        brand="Interactive Data Analysis",
        brand_href="#",
        color="primary",
        dark=True,
    )

def serve_layout():
    """
    Combine the navbar, left panel, and main content into a dbc.Container.
    """
    return html.Div(
        [
            make_navbar(),  # Add the navbar at the top
            dbc.Container(
                [
                    make_left_panel(),
                    make_main_content(),
                ],
                fluid=True,
                style={
                    "display": "flex",
                    "flex-direction": "row",  # Ensure left panel and main content align properly
                },
                className="dashboard-container",
                id="main-container",
            ),
        ],
        style={"backgroundColor": "#12163a"},  # Matches the dark theme of your app
    )


app.layout = serve_layout()


@app.callback(
    [
        Output("group-by-dropdown", "options"),
        Output("aggregate-column-dropdown", "options"),
    ],
    Input("dataset-dropdown", "value"),
)
def populate_dropdowns(selected_file):
    if not selected_file:
        return [], []

    # Example: pulling schema for the selected dataset
    table_schema = brick.get_schema_for_table(selected_file)
    print(f"Table schema: {table_schema}")
    column_options = [
        {"label": col.name, "value": col.name} for col in table_schema.fields
    ]
    return column_options, column_options


# For updating the data table based on grouping/aggregation
# Callback to update table data
@app.callback(
    [
        Output("data-table", "columnDefs"),
        Output("data-table", "rowData"),
    ],
    [
        Input("dataset-dropdown", "value"),
        Input("group-by-dropdown", "value"),
        Input("aggregate-column-dropdown", "value"),
        Input("aggregation-function-dropdown", "value"),
        Input("data-table", "filterModel"),
        Input("data-table", "cellValueChanged"),  # For edits
    ],
)
def update_group_by_table(
    selected_file, group_by, aggregate_column, agg_function, filter_model, cell_value_changed
):
    if not selected_file:
        return [], []

    if selected_file and cell_value_changed:
        brick.save_row_data(selected_file, cell_value_changed)

    print(
        f"Selected file: {selected_file}, Group by: {group_by}, Aggregate: {aggregate_column}, Function: {agg_function}, Filter: {filter_model}"
    )
    # Group by and aggregate the data makes sense only if all 3 are selected

    if not all([group_by, aggregate_column, agg_function]):
        df = brick.get_data_query(table_name=selected_file, filter_model=filter_model)
    else:
        df = brick.get_data_query(
            table_name=selected_file,
            group_by=group_by,
            aggregate_columns=[{"column": aggregate_column, "agg": agg_function}],
        )
    # dash_ag_grid expects "columnDefs" in the form [{"headerName": ..., "field": ...}]
    column_defs = [{"headerName": col, "field": col} for col in df.columns]
    row_data = df.to_dict("records")
    return column_defs, row_data


@app.callback(
    [
        Output("tab-1", "disabled"),
        Output("tab-2", "disabled"),
        Output("tab-3", "disabled"),
    ],
    Input("dataset-dropdown", "value"),
)
def toggle_tabs_when_no_dataset(selected_dataset):
    if not selected_dataset:
        # No dataset selected -> disable all tabs
        return True, True, True
    # Otherwise, enable them
    return False, False, False


# Only load data for each tab if that tab is active AND dataset is chosen
@app.callback(
    [
        Output("table-tab1", "data"),
        Output("empty-message-1", "children"),  # Add this output for the message
        Output("empty-message-1", "is_open"),
    ],
    [Input("main-tabs", "active_tab"), Input("dataset-dropdown", "value")],
)
def update_table_tab1(active_tab, selected_dataset):
    if active_tab != "tab-0" or not selected_dataset:
        return [], "Please select a dataset.", True
    print(f"Updating table for tab 1 with dataset: {selected_dataset}")
    df1 = brick.check_duplicates(table_name=selected_dataset)
    if df1.size == 0:
        return [], "All good! No duplicates found.", True
    return df1.to_dict("records"), "", False


@app.callback(
    [
        Output("table-tab2", "data"),
        Output("empty-message-2", "children"),  # Add this output for the message
        Output("empty-message-2", "is_open"),
    ],
    [Input("main-tabs", "active_tab"), Input("dataset-dropdown", "value")],
)
def update_table_tab2(active_tab, selected_dataset):
    if active_tab != "tab-1" or not selected_dataset:
        return [], "Please select a dataset.", True
    # Example data for tab 2
    df2 = brick.check_negative_debits_credits(table_name=selected_dataset)
    if df2.size == 0:
        return [], "All good! No mismatches found.", True
    return df2.to_dict("records"), "", False


@app.callback(
    [
        Output("table-tab3", "data"),
        Output("empty-message-3", "children"),  # Add this output for the message
        Output("empty-message-3", "is_open"),
    ],
    [Input("main-tabs", "active_tab"), Input("dataset-dropdown", "value")],
)
def update_table_tab3(active_tab, selected_dataset):
    if active_tab != "tab-2" or not selected_dataset:
        return [], "Please select a dataset.", True
    # Example data for tab 3
    df3 = brick.check_region_country_mismatch(table_name=selected_dataset)
    if df3.size == 0:
        return [], "All good! No mismatches found.", True
    return df3.to_dict("records"), "", False

if __name__ == "__main__":
    app.run_server(debug=False, host='0.0.0.0', port=8080, use_reloader=False)
