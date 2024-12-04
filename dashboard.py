import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import sqlite3
import pandas as pd
from dash.dependencies import Input, Output

# Create Dash app
app = dash.Dash(__name__)

# Function to fetch data from SQLite
def fetch_data():
    conn = sqlite3.connect("streaming_data.db")
    query = "SELECT * FROM messages"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Define layout of the dashboard
app.layout = html.Div([
    html.H1("Real-Time Spark Streaming Dashboard"),
    dcc.Graph(id="live-update-graph"),
    dcc.Interval(
        id="interval-component",
        interval=1000,  # Update every second
        n_intervals=0
    )
])

# Define callback to update graph
@app.callback(
    Output("live-update-graph", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_graph(n):
    # Fetch the latest data from SQLite
    df = fetch_data()

    # Create a simple line plot or bar plot based on the data
    fig = go.Figure(data=[go.Bar(x=df.index, y=df["message"])])
    
    fig.update_layout(
        title="Messages Stream",
        xaxis_title="Time",
        yaxis_title="Message"
    )
    return fig

if __name__ == "__main__":
    app.run_server(debug=True)
