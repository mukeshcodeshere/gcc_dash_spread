import pandas as pd
import plotly.graph_objects as go
from dash import Dash, html, dcc, Input, Output
import dash_bootstrap_components as dbc
import dash_table
from datetime import datetime
from sqlalchemy import create_engine
from urllib import parse
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv("credential.env")

tradepricetable = os.getenv("tradepricetable")
contract_margin_table = os.getenv("contract_margin_table")

# Fetch credentials from environment variables
connection_params = {
    "server": os.getenv("DB_SERVER"),
    "database": os.getenv("DB_NAME"),
    "username": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
}

connecting_string = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server={connection_params['server']};"
    f"Database={connection_params['database']};"
    f"Uid={connection_params['username']};"
    f"Pwd={connection_params['password']};"
    f"Encrypt=yes;"
    f"TrustServerCertificate=no;"
    f"Connection Timeout=30;"
)

params = parse.quote_plus(connecting_string)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)
# Load data from SQL
query = f"SELECT * FROM {tradepricetable}.{contract_margin_table}" 
data = pd.read_sql(query, con=engine)
data["Date"] = pd.to_datetime(data["Date"], errors="coerce")
data["LastTrade"] = pd.to_datetime(data["LastTrade"], errors="coerce")

# Initialize Dash app
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = dbc.Container([
    html.H2("Seasonal Spread Analysis"),

    dbc.Row([
        dbc.Col([dcc.Dropdown(id='group-dropdown', placeholder='Select Group')]),
        dbc.Col([dcc.Dropdown(id='region-dropdown', placeholder='Select Region')]),
        dbc.Col([dcc.Dropdown(id='instrument-dropdown', placeholder='Select Instrument')]),
        dbc.Col([dcc.Dropdown(id='month-dropdown', placeholder='Select Month')]),
    ]),

    html.Br(),
    dcc.Graph(id='spread-figure'),
    html.Br(),
    dcc.Graph(id='spread-histogram'),

    html.H4("Filtered Data Preview"),
    dash_table.DataTable(
        id='data-preview',
        page_size=10,
        style_table={'overflowX': 'auto'},
        style_cell={
            'backgroundColor': 'black',
            'color': 'white',
            'textAlign': 'left',
            'fontSize': 12,
        },
        style_header={
            'backgroundColor': 'rgb(30, 30, 30)',
            'fontWeight': 'bold'
        }
    )
], fluid=True)

# Dropdown: Group
@app.callback(
    Output('group-dropdown', 'options'),
    Input('group-dropdown', 'id')
)
def populate_group(_):
    groups = sorted(data['Group'].dropna().unique())
    return [{'label': g, 'value': g} for g in groups]

# Dropdown: Region
@app.callback(
    Output('region-dropdown', 'options'),
    Input('group-dropdown', 'value')
)
def update_region(group):
    if group:
        regions = sorted(data[data['Group'] == group]['Region'].dropna().unique())
        return [{'label': r, 'value': r} for r in regions]
    return []

# Dropdown: Instrument
@app.callback(
    Output('instrument-dropdown', 'options'),
    Input('region-dropdown', 'value'),
    Input('group-dropdown', 'value')
)
def update_instrument(region, group):
    if group and region:
        instruments = sorted(data[
            (data['Group'] == group) &
            (data['Region'] == region)
        ]['InstrumentName'].dropna().unique())
        return [{'label': i, 'value': i} for i in instruments]
    return []

# Dropdown: Month
@app.callback(
    Output('month-dropdown', 'options'),
    Input('instrument-dropdown', 'value'),
    Input('region-dropdown', 'value'),
    Input('group-dropdown', 'value')
)
def update_month(instrument, region, group):
    if group and region and instrument:
        months = sorted(data[
            (data['Group'] == group) &
            (data['Region'] == region) &
            (data['InstrumentName'] == instrument)
        ]['Month'].dropna().unique())
        return [{'label': m, 'value': m} for m in months]
    return []

# Callback for seasonal chart and histogram
@app.callback(
    Output('spread-figure', 'figure'),
    Output('spread-histogram', 'figure'),
    Input('group-dropdown', 'value'),
    Input('region-dropdown', 'value'),
    Input('instrument-dropdown', 'value'),
    Input('month-dropdown', 'value')
)
def update_figure(group, region, instrument, month):
    # Ensure all dropdowns have a value selected before filtering
    if None in [group, region, instrument, month]:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title="Please select all dropdowns to view data",
            template='plotly_dark'
        )
        return empty_fig, empty_fig

    filtered_df = data[
        (data['Group'] == group) &
        (data['Region'] == region) &
        (data['InstrumentName'] == instrument) &
        (data['Month'] == month)
    ].copy()

    filtered_df = filtered_df.sort_values("Date")
    today = pd.Timestamp.today().normalize()

    historical_df = filtered_df[filtered_df['LastTrade'] <= today].copy()
    current_df = filtered_df[filtered_df['LastTrade'] > today].copy()

    fig = go.Figure()
    seasonal_data = {}

    for year in historical_df['Year'].unique():
        year_group = historical_df[historical_df['Year'] == year].copy()
        last_trade = year_group['LastTrade'].max()
        year_filtered = year_group[year_group['Date'] <= last_trade].sort_values('Date').tail(252).copy()
        if len(year_filtered) == 252:
            year_filtered = year_filtered.reset_index(drop=True)
            year_filtered['TradingDay'] = range(1, 253)
            seasonal_data[str(year)] = year_filtered

    if not historical_df.empty and not current_df.empty:
        last_hist_trade = historical_df['LastTrade'].max()
        next_month_start = (last_hist_trade + pd.offsets.MonthBegin(1)).normalize()
        current_filtered = current_df[current_df['Date'] >= next_month_start].sort_values('Date').head(252).copy()
        if not current_filtered.empty:
            current_filtered = current_filtered.reset_index(drop=True)
            current_filtered['TradingDay'] = range(1, len(current_filtered) + 1)
            seasonal_data["Current"] = current_filtered

    if not seasonal_data:
        # Fallback to simple time series if no seasonal data can be plotted
        fig.add_trace(go.Scatter(
            x=filtered_df["Date"],
            y=filtered_df["spread"],
            mode="lines",
            name="All Data (Time Series)",
            line=dict(color="lightblue", width=2)
        ))
        fig.update_layout(
            title="Spread Time Series (No Seasonal Data Available)",
            xaxis_title="Date",
            yaxis_title="Spread",
            margin=dict(l=40, r=40, t=60, b=40),
            template='plotly_dark'
        )
    else:
        for label, df in seasonal_data.items():
            fig.add_trace(go.Scatter(
                x=df["TradingDay"],
                y=df["spread"],
                mode="lines",
                name=label,
                line=dict(color="white" if label == "Current" else None,
                          width=3 if label == "Current" else 1.5),
                opacity=1.0 if label == "Current" else 0.6
            ))

        fig.update_layout(
            title="Seasonal Spread by Year",
            xaxis_title="Trading Day (1 to 252)",
            yaxis_title="Spread",
            margin=dict(l=40, r=40, t=60, b=40),
            legend_title="Season",
            template='plotly_dark'
        )

    hist_fig = go.Figure()
    if not filtered_df.empty and 'spread' in filtered_df.columns:
        spread_values = filtered_df["spread"]
        
        # Calculate statistics
        latest_spread = spread_values.iloc[-1] if not spread_values.empty else None
        mean_spread = spread_values.mean()
        median_spread = spread_values.median()
        std_dev = spread_values.std()

        hist_fig.add_trace(go.Histogram(
            x=spread_values,
            marker_color='lightblue',
            nbinsx=50,
            name='Spread Distribution'
        ))

        # Add vertical lines for statistics
        if latest_spread is not None:
            hist_fig.add_vline(x=latest_spread, line_dash="dash", line_color="yellow",
                               annotation_text=f"Latest: {latest_spread:.2f}",
                               annotation_position="top right", annotation_font_color="yellow")
        
        hist_fig.add_vline(x=mean_spread, line_dash="dash", line_color="red",
                           annotation_text=f"Mean: {mean_spread:.2f}",
                           annotation_position="top left", annotation_font_color="red")
        
        hist_fig.add_vline(x=median_spread, line_dash="dash", line_color="green",
                           annotation_text=f"Median: {median_spread:.2f}",
                           annotation_position="top right", annotation_font_color="green")
        
        hist_fig.add_vline(x=mean_spread - std_dev, line_dash="dot", line_color="orange",
                           annotation_text=f"-1 Std Dev: {(mean_spread - std_dev):.2f}",
                           annotation_position="bottom left", annotation_font_color="orange")
        hist_fig.add_vline(x=mean_spread + std_dev, line_dash="dot", line_color="orange",
                           annotation_text=f"+1 Std Dev: {(mean_spread + std_dev):.2f}",
                           annotation_position="bottom right", annotation_font_color="orange")
                           
        hist_fig.add_vline(x=mean_spread - 2 * std_dev, line_dash="dot", line_color="purple",
                           annotation_text=f"-2 Std Dev: {(mean_spread - 2 * std_dev):.2f}",
                           annotation_position="bottom left", annotation_font_color="purple")
        hist_fig.add_vline(x=mean_spread + 2 * std_dev, line_dash="dot", line_color="purple",
                           annotation_text=f"+2 Std Dev: {(mean_spread + 2 * std_dev):.2f}",
                           annotation_position="bottom right", annotation_font_color="purple")

        # Add a text box for statistics
        stats_text = (
            f"Latest Spread: {latest_spread:.2f}<br>"
            f"Mean: {mean_spread:.2f}<br>"
            f"Median: {median_spread:.2f}<br>"
            f"Std Dev: {std_dev:.2f}"
        )
        
        hist_fig.add_annotation(
            text=stats_text,
            xref="paper", yref="paper",
            x=0.98, y=0.98,  # Position in top right corner of the plot area
            showarrow=False,
            align="left",
            bordercolor="white",
            borderwidth=1,
            bgcolor="rgba(0,0,0,0.7)", # Semi-transparent background
            font=dict(color="white", size=10)
        )

    hist_fig.update_layout(
        title="Distribution of Spread (Histogram) with Key Statistics",
        xaxis_title="Spread",
        yaxis_title="Frequency",
        template="plotly_dark",
        margin=dict(l=40, r=40, t=60, b=40)
    )

    return fig, hist_fig

# DataTable: Filtered Data Preview
@app.callback(
    Output('data-preview', 'data'),
    Output('data-preview', 'columns'),
    Input('group-dropdown', 'value'),
    Input('region-dropdown', 'value'),
    Input('instrument-dropdown', 'value'),
    Input('month-dropdown', 'value')
)
def update_table(group, region, instrument, month):
    # Ensure all dropdowns have a value selected before filtering
    if None in [group, region, instrument, month]:
        return [], []

    filtered_df = data[
        (data['Group'] == group) &
        (data['Region'] == region) &
        (data['InstrumentName'] == instrument) &
        (data['Month'] == month)
    ].copy()

    filtered_df = filtered_df.sort_values("Date")
    filtered_df["LastTrade"] = pd.to_datetime(filtered_df["LastTrade"], errors="coerce")
    filtered_df["Date"] = pd.to_datetime(filtered_df["Date"], errors="coerce")
    filtered_df["Year"] = filtered_df["Date"].dt.year

    if filtered_df.empty:
        return [], []

    columns = [{"name": i, "id": i} for i in filtered_df.columns]
    return filtered_df.to_dict("records"), columns

if __name__ == '__main__':
    app.run(port=8052)