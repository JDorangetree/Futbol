import dash
import pandas as pd
import numpy as np
from dash import dash_table
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
from datetime import date, datetime
import pymongo
import os
from dotenv import load_dotenv
from whitenoise import WhiteNoise
from dash import html 


load_dotenv()
mongodb = os.getenv('PASS')


app = dash.Dash(__name__,    external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server
server.wsgi_app = WhiteNoise(server.wsgi_app, root='assets/')




def puntos_locales(delta):
    if delta > 0:
        return(3)
    if delta < 0:
        return(0)
    else:
        return(1)

def puntos_visitante(delta):
    if delta > 0:
        return(0)
    if delta < 0:
        return(3)
    else:
        return(1)

def df_constructor_img():
    client = pymongo.MongoClient("mongodb+srv://jnaranjo:Samba1987!@descenso.23jivof.mongodb.net/?retryWrites=true&w=majority".format(mongodb))
    db = client['Futbol']
    mycol = db["Imagenes"]
    img = []
    for doc in mycol.find():
        img.append(doc)
    df_img = pd.DataFrame(img, columns=['_id', 'Team', 'url'])
    df_img.drop(['_id'], axis=1, inplace=True)
    return(df_img)

df_url = df_constructor_img()

def df_constructor_consolidado():
    client = pymongo.MongoClient("mongodb+srv://jnaranjo:{}@descenso.23jivof.mongodb.net/?retryWrites=true&w=majority".format(mongodb))
    db = client['Futbol']
    mycol = db["Partidos"]
    myquery = { "Date": { "$gt": datetime(2021, 1, 1, 0, 0), "$lt": datetime(2023, 3, 7, 0, 0)}}
    partidos = []
    mydoc = mycol.find(myquery)
    for doc in mydoc:
        partidos.append(doc)
    df = pd.DataFrame(partidos, columns=['_id', 'Status', 'Date', 'Goal_1', 'Team_1', 'Goal_2', 'Team_2'])
    df['Goal_1'] = df['Goal_1'].astype(int)
    df['Goal_2'] = df['Goal_2'].astype(int)
    df['delta'] = df['Goal_1'] - df['Goal_2']
    df['local_points'] = df['delta'].apply(puntos_locales)
    df['visit_points'] = df['delta'].apply(puntos_visitante)

    team_list = df['Team_1'].unique().tolist()

    df_total = pd.DataFrame()
    for i in team_list:
        df_pivot_local = df.loc[df['Team_1'] == i]
        df_pivot_local = df_pivot_local.reset_index()
        df_pivot_local['Points'] = df_pivot_local['local_points']
        df_pivot_local['Team'] = df_pivot_local['Team_1']
        df_pivot_local['GC'] = df_pivot_local['Goal_2']
        df_pivot_local['GF'] = df_pivot_local['Goal_1']
        df_pivot_visit = df.loc[df['Team_2'] == i]
        df_pivot_visit = df_pivot_visit.reset_index()
        df_pivot_visit['Points'] = df_pivot_visit['visit_points']
        df_pivot_visit['Team'] = df_pivot_visit['Team_2']
        df_pivot_visit['GC'] = df_pivot_visit['Goal_1']
        df_pivot_visit['GF'] = df_pivot_visit['Goal_2']
        df_pivot = pd.concat([df_pivot_local, df_pivot_visit])
        df_pivot = df_pivot.reset_index()
        df_total = pd.concat([df_total, df_pivot])

    df_total.drop(['index'], axis=1, inplace=True)
    df_total.drop(['level_0'], axis=1, inplace=True)



    df_resume = df_total.copy()
    df_resume['DIF'] = df_resume['GF'] - df_resume['GC']
    df_resume['Año'] = df_resume['Date'].dt.year
    df_resume['Mes'] = df_resume['Date'].dt.month


    df_resume['Season'] = np.where(df_resume['Mes'] > 6, 'Clausura', np.where(df_resume['Año'] == 2020, 'Clausura', 'Apertura'))
    df_resume['Season'] = df_resume['Año'].astype(str) + ' - ' + df_resume['Season']
    return(df_resume)


def season_team(df, season):
    df_pivot = df.copy()
    df_pivot = df_pivot.loc[df_pivot['Año'] == season]
    team_list = df_pivot['Team'].unique().tolist()
    return(team_list)


def team_filter(df):
    df_0 = pd.DataFrame()
    df_pivot = df.copy()
    team_list = season_team(df_pivot, 2023)
    for i in team_list:
        df_pivot_0 = df_pivot.loc[df_pivot['Team'] == i]
        df_0 = pd.concat([df_0, df_pivot_0])
    return(df_0)

def contador_2023(año):
    if año < 2023:
        return(0)
    elif año > 2023:
        return(0)
    else:
        return(1)

def contador_2022(año):
    if año < 2022:
        return(0)
    elif año > 2022:
        return(0)
    else:
        return(1)

def contador_2021(año):
    if año < 2021:
        return(0)
    elif año > 2021:
        return(0)
    else:
        return(1)

def contador_2020(año):
    if año < 2020:
        return(0)
    elif año > 2020:
        return(0)
    else:
        return(1)

def contador_2019(año):
    if año < 2019:
        return(0)
    elif año > 2019:
        return(0)
    else:
        return(1)
    
def contador_2018(año):
    if año < 2018:
        return(0)
    elif año > 2018:
        return(0)
    else:
        return(1)

def contador_2017(año):
    if año < 2017:
        return(0)
    elif año > 2017:
        return(0)
    else:
        return(1)

def contador_2017(año):
    if año < 2017:
        return(0)
    elif año > 2017:
        return(0)
    else:
        return(1)
    
def contador_2016(año):
    if año < 2016:
        return(0)
    elif año > 2016:
        return(0)
    else:
        return(1)

def contador_2015(año):
    if año < 2015:
        return(0)
    elif año > 2015:
        return(0)
    else:
        return(1)
    


def df_constructor_descenso(df):
    df1 = df[['Team', 'GC', 'GF', 'DIF', 'Points', 'Año', 'Season']].copy()
    df1['count_match'] = 1
    df1['partidos 2021'] = df1['Año'].apply(contador_2021)
    df1['partidos 2022'] = df1['Año'].apply(contador_2022)
    df1['partidos 2023'] = df1['Año'].apply(contador_2023)
    df1 = df1.reset_index()
    df1 = df1.drop(['index'], axis=1)

    df1.loc[(df1['Año'] == 2021), 'Puntos 2021'] = (df1.Points)  
    df1.loc[(df1['Año'] != 2021), 'Puntos 2021'] = 0
    df1['Puntos 2021'] = df1['Puntos 2021'].astype('int32')

    df1.loc[(df1['Año'] == 2022), 'Puntos 2022'] = (df1.Points)  
    df1.loc[(df1['Año'] != 2022), 'Puntos 2022'] = 0
    df1['Puntos 2022'] = df1['Puntos 2022'].astype('int32')

    df1.loc[(df1['Año'] == 2023), 'Puntos 2023'] = (df1.Points)  
    df1.loc[(df1['Año'] != 2023), 'Puntos 2023'] = 0
    df1['Puntos 2023'] = df1['Puntos 2023'].astype('int32')

    df1.loc[(df1['Año'] == 2023), 'DF 2023'] = (df1.GF - df1.GC)  
    df1.loc[(df1['Año'] != 2023), 'DF 2023'] = 0
    df1['DF 2023'] = df1['DF 2023'].astype('int32')

    df1.loc[(df1['Año'] == 2022), 'DF 2022'] = (df1.GF - df1.GC)  
    df1.loc[(df1['Año'] != 2022), 'DF 2022'] = 0
    df1['DF 2022'] = df1['DF 2022'].astype('int32')


    df1.loc[(df1['Año'] == 2021), 'DF 2021'] = (df1.GF - df1.GC)  
    df1.loc[(df1['Año'] != 2021), 'DF 2021'] = 0
    df1['DF 2021'] = df1['DF 2021'].astype('int32')
    return(df1)


actual_date = date.today()
actual_year = int(actual_date.strftime('%Y'))
df = df_constructor_consolidado()
df = df_constructor_descenso(df)
df['Año'] = df['Año'].astype('int32')


def season_team(df, season):
    df_pivot = df.copy()
    df_pivot = df_pivot.loc[df_pivot['Año'] == season]
    team_list = df_pivot['Team'].unique().tolist()
    return(team_list)


def team_filter(df, season):
    df_0 = pd.DataFrame()
    df_pivot = df.copy()
    team_list = season_team(df_pivot, season)
    for i in team_list:
        df_pivot_0 = df_pivot.loc[df_pivot['Team'] == i]
        df_0 = pd.concat([df_0, df_pivot_0])
    return(df_0)



button_howto = dbc.Button(
    "Información",
    id="howto-open",
    outline=True,
    color="info",
    # Turn off lowercase transformation for class .button in stylesheet
    style={"textTransform": "none"},
)

button_reglamento = dbc.Button(
    "Reglamento Liga Betplay Dimayor 2023",
    outline=True,
    color="primary",
    href="https://dimayor.com.co/wp-content/uploads/2023/01/Regla-Liga.pdf",
    id="season",
    style={"text-transform": "none"},
)

button_season = dbc.Button(
    "Temporada 2023",
    outline=True,
    color="primary",
    href="2023",
    id="season",
    style={"text-transform": "none"},
)

# modal_overlay = dbc.Modal(
#     [
#         dbc.ModalBody(html.Div([dcc.Markdown(howto_md)], id="howto-md")),
#         dbc.ModalFooter(dbc.Button("Close", id="howto-close", className="howto-bn")),
#     ],
#     id="modal",
#     size="lg",
# )


dash_table_1 = dash_table.DataTable(id='datatable-descenso',
            css=[dict(selector="p", rule="margin: 0px;")],
            data = [],
            columns=[],
            style_header={
        'backgroundColor': '#989f9f',
        'fontWeight': 'bold',
        'color': 'White'
    },
            style_data_conditional=[
                {
                    'if': {
                        'filter_query': '{Pos} > 18',
                        'column_id': 'Pos'
                },
            'backgroundColor': '#ff6a00',
            'color': 'white'
        },
                {
                    'if': {'column_id': 'Equipos'},
                    'textAlign': 'left'
                }
        ],
            markdown_options={"html": True},
            style_cell={'textAlign': 'center'},
            page_size=20)


dash_table_2 = dash_table.DataTable(id='datatable-posiciones',
            css=[dict(selector="p", rule="margin: 0px;")],
            data = [],
            columns=[],
            style_header={
        'backgroundColor': '#989f9f',
        'fontWeight': 'bold',
        'color': 'White'
    },
            style_data_conditional=[
                {
                    'if': {
                        'filter_query': '{Pos} < 9',
                        'column_id': 'Pos'
                },
            'backgroundColor': 'rgba(36, 210, 135)',
            'color': 'white'
        },
                {
                    'if': {'column_id': 'Team'},
                    'textAlign': 'left'
                }
        ],
            markdown_options={"html": True},
            style_cell={'textAlign': 'center'},
            page_size=20)


dash_table_3 = dash_table.DataTable(id='datatable-reclasificacion',
            css=[dict(selector="p", rule="margin: 0px;")],
            data = [],
            columns=[],
            style_header={
        'backgroundColor': '#989f9f',
        'fontWeight': 'bold',
        'color': 'White'
    },
            style_data_conditional=[
                {
                    'if': {
                        'filter_query': '{Pos} < 9',
                        'column_id': 'Pos'
                },
            'backgroundColor': 'rgba(36, 210, 135)',
            'color': 'white'
        },
                {
                    'if': {'column_id': 'Team'},
                    'textAlign': 'left'
                }
        ],
            markdown_options={"html": True},
            style_cell={'textAlign': 'center'},
            page_size=20)



tab1_content = html.Div(
    [
    dbc.Card(
        dbc.CardBody([
            html.H1('Descenso', style={'textAlign': 'center'}),
            html.Hr(),

            dbc.Row([
                dbc.Col([
                    dash_table_1,
                    html.Br()],sm=6
                )
            ], justify='center')
        ])
    )
    ]
)


tab2_content = html.Div(
    [
    dbc.Card(
        dbc.CardBody([
            html.H1('Posiciones', style={'textAlign': 'center'}),
            html.Hr(),

            dbc.Row([
                dbc.Col([
                    dash_table_2,
                    html.Br()],sm=6
                )
            ], justify='center')
        ])
    )
    ]
)

tab3_content = html.Div(
    [
    dbc.Card(
        dbc.CardBody([
            html.H1('Reclasificación', style={'textAlign': 'center'}),
            html.Hr(),

            dbc.Row([
                dbc.Col([
                    dash_table_3,
                    html.Br()],sm=6
                )
            ], justify='center')
        ])
    )
    ]
)

header = dbc.Navbar(
    dbc.Container(
        [
            dbc.Row(
                [
                    dbc.Col(
                        html.Img(
                            id="logo",
                            src="ligabetplay.png", height="100px"), width=2),
                    dbc.Col(
                        [
                            html.Div(
                                [
                                    html.H3("Estadísticas Futbol profesional Colombiano"),
                                    html.P("Temporada 2023"),
                                ],
                                id="app-title",
                            )
                        ],
                        md=True,
                        align="center",
                    ),
                ],
                align="center",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dbc.NavbarToggler(id="navbar-toggler"),
                            dbc.Collapse(
                                dbc.Nav(
                                    [
                                        dbc.NavItem(button_reglamento),
                                    ],
                                    navbar=True,
                                ),
                                id="navbar-collapse",
                                navbar=True,
                            ),
                            #modal_overlay,
                        ],
                        md=2,
                    ),
                ],
                align="center",
            ),
        ],
        fluid=True,
    ),
    dark=False,
    color='#989f9f',
    sticky="top",
)

app.layout = html.Div(
    [
        header,
        dbc.Tabs(
            [
                dbc.Tab(tab2_content, label="Posiciones apertura - 2023"),
                dbc.Tab(tab1_content, label="Descenso"),
                dbc.Tab(tab3_content, label="Reclasificación")
            ]
        ),
    ],
)

@app.callback(
    Output('datatable-descenso', 'data'),
    Output('datatable-descenso', 'columns'),
    Input('season', 'href'),
    State('datatable-descenso', 'data')
)

def update_table_descenso(value, data):
    actual_date = date.today()
    actual_year = int(actual_date.strftime('%Y'))
    dff = df.copy()
    dff_1 = dff[dff['Año'] == actual_year]
    dff_2 = dff[dff['Año'] == actual_year -1]
    dff_3 = dff[dff['Año'] == actual_year - 2]
    dff_4 = pd.concat([dff_1, dff_2, dff_3])
    dff_5 = team_filter(dff_4, 2023)
    dff_5 = dff_5.groupby('Team').sum().reset_index()
    menos_40 = dff_5.loc[dff_5['partidos 2022'] < 40]
    ascendidos_2023 = list(menos_40['Team'])
    df_ascendidos = df.copy()
    df_ascendidos_1 = df_ascendidos.loc[df_ascendidos['Team'] == ascendidos_2023[0]]
    df_ascendidos_2 = df_ascendidos.loc[df_ascendidos['Team'] == ascendidos_2023[1]]
    df_ascendidos = pd.concat([df_ascendidos_1, df_ascendidos_2])
    df_ascendidos = df_ascendidos.groupby('Team').sum().reset_index()
    df_ascendidos['count_match'] = df_ascendidos['partidos ' + str(actual_year)]
    df_ascendidos['Points'] = df_ascendidos['Puntos ' + str(actual_year)]
    df_ascendidos['Prom'] = round(df_ascendidos['Points'] / df_ascendidos['count_match'], 2)
    df_ascendidos['DG'] = df_ascendidos['DF ' + str(2023)]
    dff_5 = dff_5.drop(dff_5[dff_5['Team'] ==ascendidos_2023[0]].index)
    dff_5 = dff_5.drop(dff_5[dff_5['Team'] ==ascendidos_2023[1]].index)
    dff_5['Points'] = dff_5['Puntos ' + str(2023 -2)] + dff_5['Puntos ' + str(2023 -1)] + dff_5['Puntos ' + str(2023)]
    dff_5['DG'] = dff_5['DF ' + str(2023 -2)] + dff_5['DF ' + str(2023 -1)] + dff_5['DF ' + str(2023)]
    dff_5['count_match'] = dff_5['partidos ' + str(2023 -2)] + dff_5['partidos ' + str(2023 -1)] + dff_5['partidos ' + str(2023)]
    dff_5['DG'] = dff_5['DF ' + str(2023 -2)] + dff_5['DF ' + str(2023 -1)] + dff_5['DF ' + str(2023)]
    dff_5['Prom'] = round(dff_5['Points'] / dff_5['count_match'], 2)
    dff_5 = pd.concat([dff_5, df_ascendidos])
    dff_5 = pd.merge(dff_5, df_url, left_on='Team', right_on='Team')
    list_team = list(dff_5['url'])
    list_img = []
    for i in list_team:
        pivot = "<img src='{}' height='20' />".format(i)
        list_img.append(pivot)
    dff_5['imagen'] = list_img
    dff_5 =dff_5.loc[:, ['Team','count_match', 'Points', 'DG', 'Prom', 'imagen']]
    dff_5 = dff_5.rename(columns={'Team':'Equipos',
                                   'count_match':'Partidos Jugados',
                                   'Points':'Puntos', 'Prom' : 'Promedio'})
    dff_5.sort_values(by= ['Promedio'], ascending= True, inplace=True)
    pos = list(range(dff_5.shape[0], 0, -1))
    dff_5.insert(0, 'Pos', pos)
    columns = [{'id': 'Pos', 'name': 'Pos'},{"id": "imagen", "name": "", "presentation": "markdown"},{'id': 'Equipos', 'name': 'Equipos'},
               {'id': 'Partidos Jugados', 'name': 'Partidos Jugados'},{'id': 'Puntos', 'name': 'Puntos'}, {'id': 'DG', 'name': 'DG'}, {'id': 'Promedio', 'name': 'Promedio'}]
    #columns = [{'id': c, 'name': c} for c in dff_5.columns]
    return (dff_5.to_dict('records'), columns)


@app.callback(
    Output('datatable-posiciones', 'data'),
    Output('datatable-posiciones', 'columns'),
    Input('season', 'href'),
    State('datatable-posiciones', 'data')
)

def update_table_posiciones(value, data):
    actual_date = date.today()
    actual_year = int(actual_date.strftime('%Y'))
    dff = df.copy()
    dff_2 = dff[dff['Season'] == '2023 - Apertura']
    dff_2.loc[(dff_2['Points'] == 3), 'PG'] = 1  
    dff_2.loc[(dff_2['Points'] == 0), 'PG'] = 0
    dff_2.loc[(dff_2['Points'] == 1), 'PG'] = 0
    dff_2.loc[(dff_2['Points'] == 3), 'PP'] = 0  
    dff_2.loc[(dff_2['Points'] == 0), 'PP'] = 1
    dff_2.loc[(dff_2['Points'] == 1), 'PP'] = 0
    dff_2.loc[(dff_2['Points'] == 3), 'PE'] = 0  
    dff_2.loc[(dff_2['Points'] == 0), 'PE'] = 0
    dff_2.loc[(dff_2['Points'] == 1), 'PE'] = 1
    dff_2 = dff_2.groupby('Team').sum().reset_index()
    dff_2['PJ'] = dff_2['PP'] + dff_2['PG'] + dff_2['PE']
    dff_2 = pd.merge(dff_2, df_url, left_on='Team', right_on='Team')
    list_team = list(dff_2['url'])
    list_img = []
    for i in list_team:
        pivot = "<img src='{}' height='20' />".format(i)
        list_img.append(pivot)
    dff_2['imagen'] = list_img
    dff_2.sort_values(by= ['Points', 'DIF'], ascending= False, inplace=True)
    pos = list(range(1, dff_2.shape[0]+1))
    dff_2.insert(0, 'Pos', pos)
    dff_2 =dff_2.loc[:, ['Pos', 'imagen', 'Team','PJ','PG', 'PP', 'PE', 'DIF', 'Points']]
    columns = [{'id': 'Pos', 'name': 'Pos'},{"id": "imagen", "name": "", "presentation": "markdown"},{'id': 'Team', 'name': 'Equipos'},
            {'id': 'PJ', 'name': 'PJ'},{'id': 'PG', 'name': 'PG'}, {'id': 'PE', 'name': 'PE'}, {'id': 'PP', 'name': 'PP'},
            {'id': 'DIF', 'name': 'DG'}, {'id': 'Points', 'name': 'Puntos'} ]
    return (dff_2.to_dict('records'), columns)


@app.callback(
    Output('datatable-reclasificacion', 'data'),
    Output('datatable-reclasificacion', 'columns'),
    Input('season', 'href'),
    State('datatable-reclasificacion', 'data')
)

def update_table_posiciones(value, data):
    actual_date = date.today()
    actual_year = int(actual_date.strftime('%Y'))
    dff = df.copy()
    dff_2 = dff[dff['Año'] == actual_year]
    dff_2.loc[(dff_2['Points'] == 3), 'PG'] = 1  
    dff_2.loc[(dff_2['Points'] == 0), 'PG'] = 0
    dff_2.loc[(dff_2['Points'] == 1), 'PG'] = 0
    dff_2.loc[(dff_2['Points'] == 3), 'PP'] = 0  
    dff_2.loc[(dff_2['Points'] == 0), 'PP'] = 1
    dff_2.loc[(dff_2['Points'] == 1), 'PP'] = 0
    dff_2.loc[(dff_2['Points'] == 3), 'PE'] = 0  
    dff_2.loc[(dff_2['Points'] == 0), 'PE'] = 0
    dff_2.loc[(dff_2['Points'] == 1), 'PE'] = 1
    dff_2 = dff_2.groupby('Team').sum().reset_index()
    dff_2['PJ'] = dff_2['PP'] + dff_2['PG'] + dff_2['PE']
    dff_2 = pd.merge(dff_2, df_url, left_on='Team', right_on='Team')
    list_team = list(dff_2['url'])
    list_img = []
    for i in list_team:
        pivot = "<img src='{}' height='20' />".format(i)
        list_img.append(pivot)
    dff_2['imagen'] = list_img
    dff_2.sort_values(by= ['Points', 'DIF'], ascending= False, inplace=True)
    pos = list(range(1, dff_2.shape[0]+1))
    dff_2.insert(0, 'Pos', pos)
    dff_2 =dff_2.loc[:, ['Pos', 'imagen', 'Team','PJ','PG', 'PP', 'PE', 'DIF', 'Points']]
    columns = [{'id': 'Pos', 'name': 'Pos'},{"id": "imagen", "name": "", "presentation": "markdown"},{'id': 'Team', 'name': 'Equipos'},
            {'id': 'PJ', 'name': 'PJ'},{'id': 'PG', 'name': 'PG'}, {'id': 'PE', 'name': 'PE'}, {'id': 'PP', 'name': 'PP'},
            {'id': 'DIF', 'name': 'DG'}, {'id': 'Points', 'name': 'Puntos'} ]
    return (dff_2.to_dict('records'), columns)


# navbar = dbc.Navbar(
#     [
#         html.A(
#             # Use row and col to control vertical alignment of logo / brand
#             dbc.Row(
#                 [
#                     dbc.Col(html.Img(src='assets\ligabetplay.png', height="130px"),width=3),
#                     dbc.Col(
#                         [html.Label("Estadísticas Futbol profesional Colombiano",id = "label1"),
#                          html.Label("Explore the differences between old-school and new talents",className = "label2"),
#                          html.Br(),
#                          html.Label("Dashboard created by: Catarina Pinheiro, Henrique Renda, Nguyen Phuc, Lorenzo Pigozzi",className = "label2",style={'margin-bottom':'.34rem'})],width=8)
#                 ],
#                 align="between",
#                 #no_gutters=True,
#             ),
#         ),
#     ],
# )

# app.layout = dbc.Container([
#         #html.H1("Fifa Players Analysis"),
#         navbar,

#         # dbc.Tabs(
#         #     [
#         #         dbc.Tab(tab1_content, label="Players Comparison"),
#         #         dbc.Tab(tab2_content, label="League & Club Analysis"),
#         #     ], 
#         # ),
#     ],
#     fluid=True,
# )

if __name__ == '__main__':
    app.run_server(debug=True)