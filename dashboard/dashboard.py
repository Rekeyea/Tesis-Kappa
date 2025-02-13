from dash import Dash, html, dcc
import plotly.graph_objs as go
from dash.dependencies import Input, Output, State
import pymysql
from datetime import datetime, timedelta
import pandas as pd
from plotly.subplots import make_subplots  # For combining multiple subplots

# Database access class
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
        
    def get_vitals_by_date_range(self, patient_id, start_date, end_date):
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
                COALESCE(gdnews2_total, 0) as gdnews2_total,
                COALESCE(overall_confidence, 0) as overall_confidence,
                COALESCE(valid_parameters, 0) as valid_parameters,
                COALESCE(degraded_parameters, 0) as degraded_parameters,
                COALESCE(invalid_parameters, 0) as invalid_parameters
            FROM gdnews2_scores 
            WHERE patient_id = %s 
            AND window_start BETWEEN %s AND %s
            ORDER BY window_start DESC
            """
            cursor.execute(query, (patient_id, start_date, end_date))
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

app = Dash(__name__, suppress_callback_exceptions=True)

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
        # Patient Selector
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
        
        # Date Range Picker
        html.Div([
            html.Label('Date Range', style={
                'fontWeight': 'bold',
                'color': COLORS['text'],
                'marginBottom': '8px'
            }),
            html.Div([
                dcc.DatePickerRange(
                    id='date-range',
                    min_date_allowed=datetime(2020, 1, 1),
                    max_date_allowed=datetime.now(),
                    initial_visible_month=datetime.now(),
                    start_date=datetime.now() - timedelta(days=7),
                    end_date=datetime.now(),
                    display_format='YYYY-MM-DD',
                    style={'zIndex': 9999}
                ),
            ], style={
                'backgroundColor': COLORS['secondary'],
                'padding': '10px',
                'borderRadius': '5px'
            })
        ], style={'width': '40%', 'display': 'inline-block'}),
        
        # Update Interval
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
        # This container will be updated with both the prominent gdNEWS2 score and a grid of all vitals
        html.Div(id='current-values')
    ], style=CARD_STYLE),
    
    # Charts Container
    html.Div([
        dcc.Graph(id='heart-rate-graph', style={'marginBottom': '20px'}),
        dcc.Graph(id='oxygen-saturation-graph', style={'marginBottom': '20px'}),
        dcc.Graph(id='respiratory-rate-graph', style={'marginBottom': '20px'}),
        dcc.Graph(id='blood-pressure-graph', style={'marginBottom': '20px'}),
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

# Main callback for updating graphs and values
@app.callback(
    [Output('heart-rate-graph', 'figure'),
     Output('oxygen-saturation-graph', 'figure'),
     Output('respiratory-rate-graph', 'figure'),
     Output('blood-pressure-graph', 'figure'),
     Output('gdnews2-graph', 'figure'),
     Output('current-values', 'children')],
    [Input('interval-component', 'n_intervals'),
     Input('patient-selector', 'value'),
     Input('date-range', 'start_date'),
     Input('date-range', 'end_date')]
)
def update_graphs(n, patient_id, start_date, end_date):
    if not patient_id or not start_date or not end_date:
        empty_fig = {
            'data': [],
            'layout': go.Layout(
                title='No data available',
                height=400
            )
        }
        return empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, html.Div("Please select patient and date range")
    
    try:
        # Convert string dates to datetime objects
        start_date = datetime.strptime(start_date.split('T')[0], '%Y-%m-%d')
        end_date = datetime.strptime(end_date.split('T')[0], '%Y-%m-%d') + timedelta(days=1)
        
        db = VitalsDB()
        df = db.get_vitals_by_date_range(patient_id, start_date, end_date)
        
        if df.empty:
            empty_fig = {
                'data': [],
                'layout': go.Layout(
                    title='No data available for selected date range',
                    height=400
                )
            }
            return empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, html.Div("No data available for selected date range")
        
        # Heart Rate figure
        heart_rate_fig = {
            'data': [
                go.Scatter(x=df['window_start'], y=df['heart_rate_value'], 
                        name='Heart Rate', line=dict(color=COLORS['accent'], width=2))
            ],
            'layout': go.Layout(
                title='Heart Rate',
                xaxis=dict(title='Time', gridcolor=COLORS['secondary'], showgrid=True),
                yaxis=dict(title='Value (bpm)', gridcolor=COLORS['secondary'], showgrid=True),
                height=400,
                margin=dict(l=50, r=20, t=40, b=50),
                paper_bgcolor=COLORS['card'],
                plot_bgcolor=COLORS['card'],
                font=dict(color=COLORS['text'])
            )
        }
        
        # Oxygen Saturation figure
        oxygen_saturation_fig = {
            'data': [
                go.Scatter(x=df['window_start'], y=df['oxygen_saturation_value'], 
                        name='SpO2', line=dict(color=COLORS['primary'], width=2))
            ],
            'layout': go.Layout(
                title='Oxygen Saturation',
                xaxis=dict(title='Time', gridcolor=COLORS['secondary'], showgrid=True),
                yaxis=dict(title='Value (%)', gridcolor=COLORS['secondary'], showgrid=True),
                height=400,
                margin=dict(l=50, r=20, t=40, b=50),
                paper_bgcolor=COLORS['card'],
                plot_bgcolor=COLORS['card'],
                font=dict(color=COLORS['text'])
            )
        }
        
        # Respiratory Rate figure
        respiratory_rate_fig = {
            'data': [
                go.Scatter(x=df['window_start'], y=df['respiratory_rate_value'], 
                        name='Respiratory Rate', line=dict(color=COLORS['success'], width=2))
            ],
            'layout': go.Layout(
                title='Respiratory Rate',
                xaxis=dict(title='Time', gridcolor=COLORS['secondary'], showgrid=True),
                yaxis=dict(title='Value (breaths/min)', gridcolor=COLORS['secondary'], showgrid=True),
                height=400,
                margin=dict(l=50, r=20, t=40, b=50),
                paper_bgcolor=COLORS['card'],
                plot_bgcolor=COLORS['card'],
                font=dict(color=COLORS['text'])
            )
        }
        
        # Blood Pressure figure
        blood_pressure_fig = {
            'data': [
                go.Scatter(x=df['window_start'], y=df['blood_pressure_value'], 
                        name='Blood Pressure', line=dict(color=COLORS['warning'], width=2))
            ],
            'layout': go.Layout(
                title='Blood Pressure',
                xaxis=dict(title='Time', gridcolor=COLORS['secondary'], showgrid=True),
                yaxis=dict(title='Value (mmHg)', gridcolor=COLORS['secondary'], showgrid=True),
                height=400,
                margin=dict(l=50, r=20, t=40, b=50),
                paper_bgcolor=COLORS['card'],
                plot_bgcolor=COLORS['card'],
                font=dict(color=COLORS['text'])
            )
        }
        
        # gdNEWS2 figure with quality metrics in a two-row subplot
        gdnews2_fig_obj = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            row_heights=[0.6, 0.4],
            vertical_spacing=0.1,
            subplot_titles=["gdNEWS2 Score", "Quality Metrics"]
        )
        gdnews2_fig_obj.add_trace(
            go.Scatter(x=df['window_start'], y=df['gdnews2_total'],
                       name='gdNEWS2 Score', line=dict(color=COLORS['accent'], width=2)),
            row=1, col=1
        )
        gdnews2_fig_obj.add_trace(
            go.Scatter(x=df['window_start'], y=df['overall_confidence'],
                       name='Overall Confidence', line=dict(color=COLORS['primary'], width=2)),
            row=2, col=1
        )
        gdnews2_fig_obj.add_trace(
            go.Scatter(x=df['window_start'], y=df['valid_parameters'],
                       name='Valid Parameters', line=dict(color=COLORS['success'], width=2)),
            row=2, col=1
        )
        gdnews2_fig_obj.add_trace(
            go.Scatter(x=df['window_start'], y=df['degraded_parameters'],
                       name='Degraded Parameters', line=dict(color=COLORS['warning'], width=2)),
            row=2, col=1
        )
        gdnews2_fig_obj.add_trace(
            go.Scatter(x=df['window_start'], y=df['invalid_parameters'],
                       name='Invalid Parameters', line=dict(color=COLORS['danger'], width=2)),
            row=2, col=1
        )
        gdnews2_fig_obj.update_layout(
            height=400,
            margin=dict(l=50, r=20, t=40, b=50),
            paper_bgcolor=COLORS['card'],
            plot_bgcolor=COLORS['card'],
            font=dict(color=COLORS['text'])
        )
        gdnews2_fig = gdnews2_fig_obj.to_dict()
        
        # Update Current Values card to show a prominent gdNEWS2 score and all vital signs in a grid
        latest = df.iloc[0]
        current_values = html.Div([
            # Prominent gdNEWS2 score
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
                        'borderRadius': '10px'
                    }
                )
            ], style={'textAlign': 'center'}),
            
            # Grid for all vitals
            html.Div([
                html.Div([
                    html.Div('Heart Rate', style={'color': COLORS['text'], 'fontSize': '0.9em'}),
                    html.Div(format_value(latest['heart_rate_value'], '.1f', 'bpm'), 
                             style={'fontSize': '1.2em', 'fontWeight': 'bold'})
                ], style={'padding': '10px', 'backgroundColor': COLORS['card'], 'borderRadius': '5px'}),
                
                html.Div([
                    html.Div('Respiratory Rate', style={'color': COLORS['text'], 'fontSize': '0.9em'}),
                    html.Div(format_value(latest['respiratory_rate_value'], '.1f', 'breaths/min'), 
                             style={'fontSize': '1.2em', 'fontWeight': 'bold'})
                ], style={'padding': '10px', 'backgroundColor': COLORS['card'], 'borderRadius': '5px'}),
                
                html.Div([
                    html.Div('Oxygen Saturation', style={'color': COLORS['text'], 'fontSize': '0.9em'}),
                    html.Div(format_value(latest['oxygen_saturation_value'], '.1f', '%'), 
                             style={'fontSize': '1.2em', 'fontWeight': 'bold'})
                ], style={'padding': '10px', 'backgroundColor': COLORS['card'], 'borderRadius': '5px'}),
                
                html.Div([
                    html.Div('Temperature', style={'color': COLORS['text'], 'fontSize': '0.9em'}),
                    html.Div(format_value(latest['temperature_value'], '.1f', '°C'), 
                             style={'fontSize': '1.2em', 'fontWeight': 'bold'})
                ], style={'padding': '10px', 'backgroundColor': COLORS['card'], 'borderRadius': '5px'}),
                
                html.Div([
                    html.Div('Blood Pressure', style={'color': COLORS['text'], 'fontSize': '0.9em'}),
                    html.Div(format_value(latest['blood_pressure_value'], '.1f', 'mmHg'), 
                             style={'fontSize': '1.2em', 'fontWeight': 'bold'})
                ], style={'padding': '10px', 'backgroundColor': COLORS['card'], 'borderRadius': '5px'}),
                
                html.Div([
                    html.Div('Consciousness', style={'color': COLORS['text'], 'fontSize': '0.9em'}),
                    html.Div(latest['consciousness_value'] if latest['consciousness_value'] != 'N/A' else 'N/A',
                             style={'fontSize': '1.2em', 'fontWeight': 'bold'})
                ], style={'padding': '10px', 'backgroundColor': COLORS['card'], 'borderRadius': '5px'})
            ], style={
                'display': 'grid',
                'gridTemplateColumns': 'repeat(auto-fit, minmax(200px, 1fr))',
                'gap': '20px',
                'marginTop': '20px'
            })
        ])
        
        return heart_rate_fig, oxygen_saturation_fig, respiratory_rate_fig, blood_pressure_fig, gdnews2_fig, current_values
        
    except Exception as e:
        print(f"Error updating graphs: {e}")
        empty_fig = {
            'data': [],
            'layout': go.Layout(
                title='Error loading data',
                height=400
            )
        }
        return empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, html.Div(f"Error fetching data: {str(e)}")

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=48050, debug=True)
