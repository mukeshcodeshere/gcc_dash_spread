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
    # Animated background particles
    html.Div(className="particles"),
    
    html.Div(
        className="container",
        children=[
            # Header section with icon
            html.Div([
                html.Div(className="app-icon"),
                html.H1(
                    "Spread Calculator",
                    className="title"
                ),
                html.P(
                    "Choose your calculation mode and launch the application with style",
                    className="subtitle"
                ),
            ], className="header-section"),
            
            # Radio selection with cards
            html.Div(
                className="selection-container",
                children=[
                    html.H3("Select Calculation Mode", className="section-title"),
                    dcc.RadioItems(
                        id='calc-choice',
                        options=[
                            {'label': html.Div([
                                html.Div(className="card-icon preset-icon"),
                                html.H4("Preset Calculation", className="card-title"),
                                html.P("Use predefined calculation parameters", className="card-description")
                            ], className="option-card"), 'value': 'preset'},
                            {'label': html.Div([
                                html.Div(className="card-icon dynamic-icon"),
                                html.H4("On-the-fly Calculation", className="card-title"),
                                html.P("Configure parameters dynamically", className="card-description")
                            ], className="option-card"), 'value': 'on_the_fly'}
                        ],
                        value='preset',
                        className="radio-items"
                    ),
                ]
            ),
            
            # Launch button with glow effect
            html.Div([
                html.Button([
                    html.Span("🚀", className="button-icon"),
                    html.Span("Launch Application", className="button-text")
                ], id='launch-btn', className="launch-button"),
            ], className="button-container"),
            
            # Status section with enhanced styling
            html.Div(
                id='status-div',
                className="status-message"
            ),
            
            # Loading with custom animation
            dcc.Loading(
                id='loading',
                type='circle',
                color='#667eea',
                children=html.Div(id='redirect-div'),
                className="custom-loading"
            )
        ]
    )
], className="app-wrapper")

# Enhanced custom CSS for stunning visuals
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
        <style>
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
            
            body {
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
                overflow-x: hidden;
            }
            
            .app-wrapper {
                min-height: 100vh;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                position: relative;
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 20px;
            }
            
            /* Animated background particles */
            .particles {
                position: absolute;
                top: 0;
                left: 0;
                width: 100%;
                height: 100%;
                overflow: hidden;
                z-index: 1;
            }
            
            .particles::before,
            .particles::after {
                content: '';
                position: absolute;
                width: 300px;
                height: 300px;
                border-radius: 50%;
                background: rgba(255, 255, 255, 0.1);
                animation: float 6s ease-in-out infinite;
            }
            
            .particles::before {
                top: 10%;
                left: 10%;
                animation-delay: 0s;
            }
            
            .particles::after {
                bottom: 10%;
                right: 10%;
                animation-delay: 3s;
                width: 200px;
                height: 200px;
            }
            
            @keyframes float {
                0%, 100% { transform: translateY(0px) rotate(0deg); }
                50% { transform: translateY(-20px) rotate(10deg); }
            }
            
            .container {
                background: rgba(255, 255, 255, 0.95);
                backdrop-filter: blur(20px);
                border-radius: 24px;
                box-shadow: 0 25px 50px rgba(0, 0, 0, 0.15);
                padding: 50px;
                max-width: 800px;
                width: 100%;
                position: relative;
                z-index: 2;
                animation: slideUp 0.8s cubic-bezier(0.25, 0.46, 0.45, 0.94);
                border: 1px solid rgba(255, 255, 255, 0.2);
            }
            
            @keyframes slideUp {
                from { opacity: 0; transform: translateY(30px); }
                to { opacity: 1; transform: translateY(0); }
            }
            
            .header-section {
                text-align: center;
                margin-bottom: 40px;
            }
            
            .app-icon {
                width: 80px;
                height: 80px;
                background: linear-gradient(135deg, #667eea, #764ba2);
                border-radius: 20px;
                margin: 0 auto 20px;
                display: flex;
                align-items: center;
                justify-content: center;
                box-shadow: 0 10px 30px rgba(102, 126, 234, 0.3);
                position: relative;
            }
            
            .app-icon::before {
                content: '📊';
                font-size: 36px;
            }
            
            .title {
                color: #2d3748;
                font-size: 42px;
                font-weight: 700;
                margin-bottom: 15px;
                background: linear-gradient(135deg, #667eea, #764ba2);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                background-clip: text;
            }
            
            .subtitle {
                color: #718096;
                font-size: 18px;
                font-weight: 400;
                line-height: 1.6;
            }
            
            .selection-container {
                margin-bottom: 40px;
            }
            
            .section-title {
                color: #2d3748;
                font-size: 20px;
                font-weight: 600;
                margin-bottom: 25px;
                text-align: center;
            }
            
            .radio-items {
                display: flex;
                gap: 20px;
                justify-content: center;
                flex-wrap: wrap;
            }
            
            .option-card {
                background: #f7fafc;
                border: 2px solid #e2e8f0;
                border-radius: 16px;
                padding: 30px 25px;
                width: 280px;
                cursor: pointer;
                transition: all 0.3s cubic-bezier(0.25, 0.46, 0.45, 0.94);
                text-align: center;
                position: relative;
                overflow: hidden;
            }
            
            .option-card::before {
                content: '';
                position: absolute;
                top: 0;
                left: -100%;
                width: 100%;
                height: 100%;
                background: linear-gradient(90deg, transparent, rgba(102, 126, 234, 0.1), transparent);
                transition: left 0.5s;
            }
            
            .option-card:hover {
                transform: translateY(-5px);
                box-shadow: 0 15px 40px rgba(0, 0, 0, 0.1);
                border-color: #667eea;
            }
            
            .option-card:hover::before {
                left: 100%;
            }
            
            .card-icon {
                width: 50px;
                height: 50px;
                border-radius: 12px;
                margin: 0 auto 15px;
                display: flex;
                align-items: center;
                justify-content: center;
                font-size: 24px;
            }
            
            .preset-icon {
                background: linear-gradient(135deg, #4facfe, #00f2fe);
            }
            
            .preset-icon::before {
                content: '⚡';
            }
            
            .dynamic-icon {
                background: linear-gradient(135deg, #43e97b, #38f9d7);
            }
            
            .dynamic-icon::before {
                content: '🎛️';
            }
            
            .card-title {
                color: #2d3748;
                font-size: 18px;
                font-weight: 600;
                margin-bottom: 8px;
            }
            
            .card-description {
                color: #718096;
                font-size: 14px;
                line-height: 1.4;
            }
            
            /* Hide default radio buttons */
            .radio-items input[type="radio"] {
                position: absolute;
                opacity: 0;
                pointer-events: none;
            }
            
            /* Selected state */
            .radio-items input[type="radio"]:checked + .option-card {
                background: linear-gradient(135deg, #667eea, #764ba2);
                border-color: #667eea;
                color: white;
                transform: scale(1.05);
                box-shadow: 0 20px 50px rgba(102, 126, 234, 0.4);
            }
            
            .radio-items input[type="radio"]:checked + .option-card .card-title,
            .radio-items input[type="radio"]:checked + .option-card .card-description {
                color: white;
            }
            
            .button-container {
                text-align: center;
                margin-bottom: 30px;
            }
            
            .launch-button {
                background: linear-gradient(135deg, #667eea, #764ba2);
                color: white;
                padding: 18px 40px;
                border: none;
                border-radius: 50px;
                font-size: 18px;
                font-weight: 600;
                cursor: pointer;
                transition: all 0.3s cubic-bezier(0.25, 0.46, 0.45, 0.94);
                box-shadow: 0 10px 30px rgba(102, 126, 234, 0.3);
                display: inline-flex;
                align-items: center;
                gap: 10px;
                position: relative;
                overflow: hidden;
            }
            
            .launch-button::before {
                content: '';
                position: absolute;
                top: 0;
                left: -100%;
                width: 100%;
                height: 100%;
                background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.2), transparent);
                transition: left 0.5s;
            }
            
            .launch-button:hover {
                transform: translateY(-3px);
                box-shadow: 0 15px 40px rgba(102, 126, 234, 0.4);
            }
            
            .launch-button:hover::before {
                left: 100%;
            }
            
            .launch-button:active {
                transform: translateY(-1px);
            }
            
            .button-icon {
                font-size: 20px;
                animation: bounce 2s infinite;
            }
            
            @keyframes bounce {
                0%, 20%, 50%, 80%, 100% { transform: translateY(0); }
                40% { transform: translateY(-5px); }
                60% { transform: translateY(-3px); }
            }
            
            .status-message {
                text-align: center;
                font-size: 16px;
                font-weight: 500;
                min-height: 25px;
                padding: 15px;
                border-radius: 12px;
                transition: all 0.3s ease;
            }
            
            .status-message:not(:empty) {
                background: rgba(102, 126, 234, 0.1);
                border: 1px solid rgba(102, 126, 234, 0.2);
                color: #667eea;
            }
            
            .custom-loading {
                text-align: center;
                margin-top: 20px;
            }
            
            /* Responsive design */
            @media (max-width: 768px) {
                .container {
                    padding: 30px 25px;
                    margin: 10px;
                }
                
                .title {
                    font-size: 32px;
                }
                
                .radio-items {
                    flex-direction: column;
                    align-items: center;
                }
                
                .option-card {
                    width: 100%;
                    max-width: 300px;
                }
            }
            
            /* Smooth scrolling */
            html {
                scroll-behavior: smooth;
            }
            
            /* Focus states for accessibility */
            .launch-button:focus,
            .option-card:focus {
                outline: 3px solid rgba(102, 126, 234, 0.5);
                outline-offset: 2px;
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
        time.sleep(7)  # Allow time for app to start
        status_msg = f"✓ {app_name} app started successfully on http://127.0.0.1:{port}"
    else:
        status_msg = f"ⓘ {app_name} app is already running on http://127.0.0.1:{port}"

    redirect_component = dcc.Location(href=f'http://127.0.0.1:{port}', id='redirect')
    return status_msg, redirect_component

# Run launcher
if __name__ == '__main__':
    app.run(debug=True, host="127.0.0.1", port=8050, use_reloader=False)