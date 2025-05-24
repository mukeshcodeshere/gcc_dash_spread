import dash
from dash import dcc, html, Input, Output, State
import subprocess
import threading
import psutil
import os
import time

# Initialize Dash app
app = dash.Dash(__name__)
server = app.server

# App-to-script mapping
APP_CONFIG = {
    'preset': {'script': 'dash_preset.py', 'port': 8051},
    'on_the_fly': {'script': 'dash_onthefly.py', 'port': 8052},
}

# Track launched subprocesses
launched_processes = {}

def is_port_open(port):
    """Check if a port is already open."""
    return any(
        conn.status == psutil.CONN_LISTEN and conn.laddr.port == port
        for conn in psutil.net_connections()
    )

def launch_dash_app(script, port, key):
    """Launch a Dash app in a separate process."""
    try:
        proc = subprocess.Popen(['python', script])
        launched_processes[key] = proc
    except Exception as e:
        print(f"Error launching {key}: {e}")

# Layout
app.layout = html.Div([
    html.Div(
        className="container",
        children=[
            html.H1(
                "Welcome to the Spread Calculator",
                className="title"
            ),
            html.P(
                "Choose your desired calculation mode below and launch the application.",
                className="subtitle"
            ),
            html.Div(
                className="radio-group",
                children=[
                    dcc.RadioItems(
                        id='calc-choice',
                        options=[
                            {'label': html.Span('Preset Calculation', className="radio-label"), 'value': 'preset'},
                            {'label': html.Span('On-the-fly Calculation', className="radio-label"), 'value': 'on_the_fly'}
                        ],
                        value='preset',
                        className="radio-items"
                    ),
                ]
            ),
            html.Button(
                "Launch Application",
                id='launch-btn',
                className="launch-button"
            ),
            html.Div(
                id='status-div',
                className="status-message"
            ),
            dcc.Loading(
                id='loading',
                type='circle',
                color='#1a6d91',  # A sleek blue for the loading spinner
                children=html.Div(id='redirect-div')
            )
        ]
    )
], style={
    'fontFamily': 'Roboto, sans-serif',
    'background': 'linear-gradient(to right, #ece9e6, #ffffff)', # Subtle gradient background
    'minHeight': '100vh',
    'display': 'flex',
    'alignItems': 'center',
    'justifyContent': 'center',
    'padding': '20px'
})

# Add custom CSS for styling
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <link href="https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;700&display=swap" rel="stylesheet">
        <style>
            body {
                margin: 0;
                font-family: 'Roboto', sans-serif;
                background: linear-gradient(to right, #ece9e6, #ffffff);
                min-height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 20px;
            }
            .container {
                background-color: #ffffff;
                border-radius: 12px;
                box-shadow: 0 8px 30px rgba(0, 0, 0, 0.1);
                padding: 40px;
                max-width: 600px;
                width: 100%;
                text-align: center;
                animation: fadeIn 0.8s ease-out;
            }
            @keyframes fadeIn {
                from { opacity: 0; transform: translateY(-20px); }
                to { opacity: 1; transform: translateY(0); }
            }
            .title {
                color: #2c3e50;
                font-size: 34px;
                font-weight: 700;
                margin-bottom: 15px;
            }
            .subtitle {
                color: #7f8c8d;
                font-size: 18px;
                margin-bottom: 30px;
            }
            .radio-group {
                margin-bottom: 30px;
            }
            .radio-items .dash-radioitems .radio-label {
                display: block;
                background-color: #f8f9fa;
                border: 1px solid #e0e0e0;
                border-radius: 8px;
                padding: 15px 20px;
                margin: 10px 0;
                cursor: pointer;
                transition: all 0.3s ease;
                color: #34495e;
                font-size: 16px;
                font-weight: 400;
                text-align: left;
            }
            .radio-items .dash-radioitems .radio-label:hover {
                background-color: #e9ecef;
                border-color: #c9d0d6;
            }
            .radio-items .dash-radioitems input[type="radio"]:checked + .radio-label {
                background-color: #1a6d91; /* Darker blue for selected */
                border-color: #1a6d91;
                color: white;
                box-shadow: 0 4px 15px rgba(26, 109, 145, 0.3);
            }
            .radio-items .dash-radioitems input[type="radio"] {
                display: none; /* Hide default radio button */
            }
            .launch-button {
                background-color: #2980b9; /* A professional blue */
                color: white;
                padding: 14px 28px;
                border: none;
                border-radius: 8px;
                font-size: 18px;
                font-weight: 500;
                cursor: pointer;
                transition: background-color 0.3s ease, transform 0.2s ease, box-shadow 0.3s ease;
                box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
            }
            .launch-button:hover {
                background-color: #1a6d91; /* Slightly darker on hover */
                transform: translateY(-2px);
                box-shadow: 0 6px 20px rgba(0, 0, 0, 0.15);
            }
            .status-message {
                margin-top: 25px;
                font-size: 17px;
                color: #2c3e50;
                font-weight: 400;
                min-height: 25px; /* To prevent layout shift */
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# Callback to handle launching apps
@app.callback(
    Output('status-div', 'children'),
    Output('redirect-div', 'children'),
    Input('launch-btn', 'n_clicks'),
    State('calc-choice', 'value'),
    prevent_initial_call=True
)
def handle_launch(n_clicks, selected_app):
    config = APP_CONFIG.get(selected_app)
    
    if not config:
        return "Invalid app configuration.", ""

    script = config['script']
    port = config['port']
    app_name = selected_app.replace('_', ' ').title()

    if not is_port_open(port):
        threading.Thread(
            target=launch_dash_app, args=(script, port, selected_app), daemon=True
        ).start()
        time.sleep(5)  # Allow time for app to start
        status_msg = f"✓ {app_name} app started successfully on http://127.0.0.1:{port}"
    else:
        status_msg = f"ⓘ {app_name} app is already running on http://127.0.0.1:{port}"

    redirect_component = dcc.Location(href=f'http://127.0.0.1:{port}', id='redirect')
    return status_msg, redirect_component

# Run launcher
if __name__ == '__main__':
    app.run(debug=True, host="127.0.0.1", port=8050, use_reloader=False)