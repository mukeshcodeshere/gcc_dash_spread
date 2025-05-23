import dash
from dash import dcc, html, Input, Output, State
import subprocess
import time

app = dash.Dash(__name__)

# Keep track of started processes to avoid multiple launches
started_processes = {}

app.layout = html.Div([
    html.H2("Select Calculation Mode"),
    dcc.RadioItems(
        id='calc-choice',
        options=[
            {'label': 'Preset Calculation', 'value': 'preset'},
            {'label': 'On-the-fly Calculation', 'value': 'on_the_fly'}
        ],
        value='preset'
    ),
    html.Br(),
    html.Button("Launch App", id='launch-btn'),
    html.Div(id='status-div'),
    html.Div(id='redirect-div')
])

@app.callback(
    [Output('status-div', 'children'),
     Output('redirect-div', 'children')],
    Input('launch-btn', 'n_clicks'),
    State('calc-choice', 'value')
)
def launch_and_redirect(n_clicks, choice):
    if n_clicks is None:
        return '', ''

    if choice == 'preset':
        port = 8052
        script = 'dash_preset.py'
    elif choice == 'on_the_fly':
        port = 8051
        script = 'dash_onthefly.py'
    else:
        return 'Invalid choice', ''

    # Check if process already started to avoid multiple launches
    if choice not in started_processes or started_processes[choice].poll() is not None:
        # Start the chosen dash app as a subprocess
        # Use shell=True on Windows; otherwise shell=False on Linux/Mac
        proc = subprocess.Popen(['python', script], shell=True)
        started_processes[choice] = proc
        status = f"Started {choice} app on port {port}. Redirecting..."
        # Wait briefly to allow server to start before redirecting
        time.sleep(2)
    else:
        status = f"{choice} app already running on port {port}. Redirecting..."

    # Redirect user to the started app
    return status, dcc.Location(href=f'http://localhost:{port}', id='redirect')

if __name__ == '__main__':
    app.run(port=8050)
