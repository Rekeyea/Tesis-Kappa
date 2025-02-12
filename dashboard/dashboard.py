from dash import Dash, html, dcc
import plotly.graph_objs as go
from dash.dependencies import Input, Output, State
import pymysql
from datetime import datetime, timedelta
import pandas as pd

class VitalsDB:
    def __init__(self):
        self.conn = pymysql.connect(
            host="localhost",
            port=10302,
            user="kappa",
            password="kappa",
            database="kappa",
            cursorclass=pymysql.cursors.DictCursor
        )
        
    def get_latest_vitals(self, patient_id, minutes=5):
        with self.conn.cursor() as cursor:
            query = """
            SELECT 
                window_start,
                COALESCE(respiratory_rate_value, 0) as respiratory_rate_value,
                COALESCE(oxygen_saturation_value, 0) as oxygen_saturation_value,
                COALESCE(blood_pressure_value, 0) as blood_pressure_value,
                COALESCE(heart_rate_value, 0) as heart_rate_value,
                COALESCE(temperature_value, 0) as temperature_value,
                COALESCE(consciousness_value, 'N/A') as consciousness_value,
                COALESCE(gdnews2_total, 0) as gdnews2_total
            FROM gdnews2_scores 
            WHERE patient_id = %s 
            AND window_start >= %s
            ORDER BY window_start DESC
            """
            
            time_threshold = datetime.now() - timedelta(minutes=minutes)
            cursor.execute(query, (patient_id, time_threshold))
            results = cursor.fetchall()
            return pd.DataFrame(results)

    def get_patient_ids(self):
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT patient_id FROM gdnews2_scores")
            return [row['patient_id'] for row in cursor.fetchall()]

def format_value(value, format_str, unit):
    if pd.isna(value) or value is None:
        return "N/A"
    try:
        return f"{value:{format_str}} {unit}"
    except (ValueError, TypeError):
        return "N/A"

app = Dash(__name__)

# Layout with time controls

# Colors and styles
COLORS = {
    'background': '#F0F2F5',
    'card': '#FFFFFF',
    'primary': '#1976D2',     # Medical blue
    'secondary': '#E3F2FD',
    'text': '#2C3E50',
    'accent': '#ff4444',      # For alerts/important values
    'success': '#4CAF50',     # For good vitals
    'warning': '#FFC107',     # For concerning vitals
    'danger': '#f44336'       # For critical vitals
}

CARD_STYLE = {
    'backgroundColor': COLORS['card'],
    'borderRadius': '10px',
    'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
    'padding': '20px',
    'marginBottom': '20px'
}

app.layout = html.Div([
    # Header
    html.H1('Patient Vitals Monitor', style={
        'textAlign': 'center',
        'color': COLORS['primary'],
        'fontSize': '2.5em',
        'padding': '20px 0',
        'marginBottom': '20px',
        'borderBottom': f'3px solid {COLORS["secondary"]}',
        'fontFamily': 'Helvetica Neue, Arial, sans-serif'
    }),
    
    # Controls section
    html.Div([
        html.Div([
            html.Label('Patient ID', style={
                'fontWeight': 'bold',
                'color': COLORS['text'],
                'marginBottom': '8px'
            }),
            dcc.Dropdown(
                id='patient-selector',
                options=[{'label': pid, 'value': pid} for pid in VitalsDB().get_patient_ids()],
                placeholder='Select Patient ID',
                style={'borderRadius': '5px'}
            )
        ], style={'width': '30%', 'display': 'inline-block', 'marginRight': '20px'}),
        
        html.Div([
            html.Label('Time Range', style={
                'fontWeight': 'bold',
                'color': COLORS['text'],
                'marginBottom': '8px'
            }),
            dcc.RadioItems(
                id='time-range',
                options=[
                    {'label': '5 min', 'value': 5},
                    {'label': '15 min', 'value': 15},
                    {'label': '30 min', 'value': 30},
                    {'label': '1 hour', 'value': 60},
                    {'label': '2 hours', 'value': 120}
                ],
                value=5,
                inline=True,
                style={
                    'backgroundColor': COLORS['secondary'],
                    'padding': '10px',
                    'borderRadius': '5px'
                }
            )
        ], style={'width': '40%', 'display': 'inline-block'}),
        
        html.Div([
            html.Label('Update Interval', style={
                'fontWeight': 'bold',
                'color': COLORS['text'],
                'marginBottom': '8px'
            }),
            dcc.Dropdown(
                id='update-interval',
                options=[
                    {'label': '1 second', 'value': 1000},
                    {'label': '5 seconds', 'value': 5000},
                    {'label': '10 seconds', 'value': 10000},
                    {'label': '30 seconds', 'value': 30000}
                ],
                value=5000,
                clearable=False,
                style={'borderRadius': '5px'}
            )
        ], style={'width': '20%', 'display': 'inline-block'})
    ], style={**CARD_STYLE, 'marginBottom': '20px'}),
    
    # Current Values Card
    html.Div([
        html.H3('Current Values', style={
            'textAlign': 'center',
            'color': COLORS['primary'],
            'marginTop': '0',
            'marginBottom': '20px',
            'fontFamily': 'Helvetica Neue, Arial, sans-serif'
        }),
        html.Div(id='current-values', style={
            'display': 'grid',
            'gridTemplateColumns': 'repeat(auto-fit, minmax(200px, 1fr))',
            'gap': '20px',
            'padding': '10px'
        })
    ], style=CARD_STYLE),
    
    # Charts Container
    html.Div([
        dcc.Graph(id='vitals-graph', style={'marginBottom': '20px'}),
        dcc.Graph(id='gdnews2-graph')
    ], style=CARD_STYLE),
    
    dcc.Interval(id='interval-component', interval=5000),
    html.Div(id='last-update-time', style={'display': 'none'})
    
], style={
    'padding': '20px',
    'backgroundColor': COLORS['background'],
    'minHeight': '100vh',
    'fontFamily': 'Helvetica Neue, Arial, sans-serif'
})

# Callback to update interval component
@app.callback(
    Output('interval-component', 'interval'),
    [Input('update-interval', 'value')]
)
def update_interval(value):
    return value

@app.callback(
    [Output('vitals-graph', 'figure'),
     Output('gdnews2-graph', 'figure'),
     Output('current-values', 'children')],
    [Input('interval-component', 'n_intervals'),
     Input('patient-selector', 'value'),
     Input('time-range', 'value')]
)
def update_graphs(n, patient_id, time_range):
    if not patient_id:
        return {}, {}, html.Div("No patient selected")
    
    try:
        db = VitalsDB()
        df = db.get_latest_vitals(patient_id, minutes=time_range)
        
        if df.empty:
            return {}, {}, html.Div("No data available")
        
        # Vitals figure
        vitals_fig = {
            'data': [
                go.Scatter(x=df['window_start'], y=df['heart_rate_value'], 
                        name='Heart Rate', line=dict(color=COLORS['accent'], width=2)),
                go.Scatter(x=df['window_start'], y=df['oxygen_saturation_value'], 
                        name='SpO2', line=dict(color=COLORS['primary'], width=2)),
                go.Scatter(x=df['window_start'], y=df['respiratory_rate_value'], 
                        name='Resp Rate', line=dict(color=COLORS['success'], width=2)),
                go.Scatter(x=df['window_start'], y=df['blood_pressure_value'], 
                        name='Blood Pressure', line=dict(color=COLORS['warning'], width=2))
            ],
            'layout': go.Layout(
                title=f'Real-time Vitals (Last {time_range} minutes)',
                xaxis=dict(
                    title='Time',
                    gridcolor=COLORS['secondary'],
                    showgrid=True
                ),
                yaxis=dict(
                    title='Value',
                    gridcolor=COLORS['secondary'],
                    showgrid=True
                ),
                height=400,
                margin=dict(l=50, r=20, t=40, b=50),
                paper_bgcolor=COLORS['card'],
                plot_bgcolor=COLORS['card'],
                font=dict(color=COLORS['text'])
            )
        }
        
        # gdNEWS2 figure
        gdnews2_fig = {
            'data': [
                go.Scatter(x=df['window_start'], y=df['gdnews2_total'],
                          name='gdNEWS2', line=dict(color='#ff4444', width=2))
            ],
            'layout': go.Layout(
                title='gdNEWS2 Score',
                xaxis=dict(title='Time'),
                yaxis=dict(title='Score', range=[0, 20]),
                height=200,
                margin=dict(l=50, r=20, t=40, b=50),
                paper_bgcolor='white',
                plot_bgcolor='rgba(0,0,0,0)'
            )
        }

        def get_value_color(value, metric):
            # Add your thresholds for different metrics
            return COLORS['text']  # Default color
        
        latest = df.iloc[0]
        current_values = html.Div([
            # Main container with flex display
            html.Div([
                # Left side - Big Score
                html.Div([
                    html.Div('gdNEWS2 Score', style={
                        'fontSize': '1.2em',
                        'color': COLORS['text'],
                        'marginBottom': '10px',
                        'fontWeight': 'bold'
                    }),
                    html.Div(
                        format_value(latest['gdnews2_total'], '.1f', ''),
                        style={
                            'fontSize': '3.5em',
                            'fontWeight': 'bold',
                            'color': COLORS['accent'],
                            'textAlign': 'center',
                            'padding': '20px',
                            'backgroundColor': COLORS['secondary'],
                            'borderRadius': '10px',
                            'marginBottom': '10px'
                        }
                    )
                ], style={
                    'flex': '0 0 200px',
                    'marginRight': '30px',
                    'borderRight': f'2px solid {COLORS["secondary"]}',
                    'paddingRight': '30px'
                }),
                
                # Right side - Component Values
                html.Div([
                    # Grid for vital signs
                    html.Div([
                        # Row 1
                        html.Div([
                            html.Div([
                                html.Div('Heart Rate', style={'color': COLORS['text'], 'fontSize': '0.9em'}),
                                html.Div(format_value(latest['heart_rate_value'], '.1f', 'bpm'), 
                                    style={'fontSize': '1.2em', 'fontWeight': 'bold'})
                            ], style={'flex': '1'}),
                            html.Div([
                                html.Div('SpO2', style={'color': COLORS['text'], 'fontSize': '0.9em'}),
                                html.Div(format_value(latest['oxygen_saturation_value'], '.1f', '%'),
                                    style={'fontSize': '1.2em', 'fontWeight': 'bold'})
                            ], style={'flex': '1'}),
                            html.Div([
                                html.Div('Respiratory Rate', style={'color': COLORS['text'], 'fontSize': '0.9em'}),
                                html.Div(format_value(latest['respiratory_rate_value'], '.1f', '/min'),
                                    style={'fontSize': '1.2em', 'fontWeight': 'bold'})
                            ], style={'flex': '1'})
                        ], style={'display': 'flex', 'gap': '20px', 'marginBottom': '20px'}),
                        
                        # Row 2
                        html.Div([
                            html.Div([
                                html.Div('Blood Pressure', style={'color': COLORS['text'], 'fontSize': '0.9em'}),
                                html.Div(format_value(latest['blood_pressure_value'], '.1f', 'mmHg'),
                                    style={'fontSize': '1.2em', 'fontWeight': 'bold'})
                            ], style={'flex': '1'}),
                            html.Div([
                                html.Div('Temperature', style={'color': COLORS['text'], 'fontSize': '0.9em'}),
                                html.Div(format_value(latest['temperature_value'], '.1f', '°C'),
                                    style={'fontSize': '1.2em', 'fontWeight': 'bold'})
                            ], style={'flex': '1'}),
                            html.Div([
                                html.Div('Consciousness', style={'color': COLORS['text'], 'fontSize': '0.9em'}),
                                html.Div(latest['consciousness_value'] if latest['consciousness_value'] != 'N/A' else 'N/A',
                                    style={'fontSize': '1.2em', 'fontWeight': 'bold'})
                            ], style={'flex': '1'})
                        ], style={'display': 'flex', 'gap': '20px'})
                    ])
                ], style={'flex': '1'})
            ], style={
                'display': 'flex',
                'alignItems': 'center',
                'backgroundColor': COLORS['card'],
                'padding': '20px',
                'borderRadius': '10px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
            })
        ])
        
        return vitals_fig, gdnews2_fig, current_values
        
    except Exception as e:
        print(f"Error updating graphs: {e}")
        return {}, {}, html.Div(f"Error fetching data: {str(e)}")

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=48050)