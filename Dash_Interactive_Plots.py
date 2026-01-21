# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import pandas as pd
from dash import Dash, html, dcc, callback, Input, Output, dash_table
import plotly.express as px

app = Dash()

#pre-configuring data types
df=pd.read_csv(r"C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\StatswRegion.csv")

print(df)

#for independent bar chart no callbacks
count_df=df.groupby(["region"])['region'].count()
fig2 = px.bar(count_df, x=count_df.index, y=count_df)
fig2.update_layout(xaxis_title="Region #", yaxis_title='Total Catchments')

#for dropdown1 data and labels
labels1=["NSE","RMSE","KGE","Pearson Coefficient"]
dropdown1=[]
for i in range(0,4):
    dic={'label': labels1[i], 'value' : df.columns[i+2]}
    dropdown1.append(dic)

#for dropdown 2 data and labels
dropdown2=[]
for i in range(len(count_df)):
    dic2={'label': f'Region {i}', 'value' : count_df.index[i-1]}
    dropdown2.append(dic2)

print(dropdown2)

#Layout of online terminal
app.layout = html.Div([
    html.H4("Summary Statistics for CAMELS Regions"),
    html.Div([
        dcc.Dropdown(options=dropdown2,
            value=count_df.index[2],
            multi=True,
            id='region-dropdown'),
        dash_table.DataTable(data=[], columns=[{"name": i, "id": i} for i in ['region','nse_o','rmse_o','kge_12_o','pearson_o']], id='table')]),
    html.Div([
        dcc.Graph(id='box_plot'),
        dcc.Dropdown(options=dropdown1,
             value=df.columns[2],
             id='stat-dropdown')]),
    html.Div([
        dcc.Graph(id='bar_chart', figure=fig2)])
    ])

@callback(
    Output('box_plot', 'figure'),
    Input('stat-dropdown', 'value')
)
def update_box(stat):
    fig=px.box(df, x='region', y=stat)
    fig.update_layout(transition_duration=500)
    if stat=='nse_o':
        fig.update_yaxes(range=(-60,0))
    elif stat=='kge_12_o':
        fig.update_yaxes(range=(-40,1))
    return fig

@callback(
    Output('table', 'data'),
    Input('region-dropdown', 'value')
)
def update_table(region):
    if not isinstance(region, list):
        region=[region]
    group_df=df.groupby(['region'])[['nse_o','rmse_o', 'kge_12_o','pearson_o']].mean().reset_index()
    mean_df=group_df[group_df['region'].isin(region)]
    return mean_df.to_dict('records')

if __name__ == '__main__':
    app.run(debug=True)


