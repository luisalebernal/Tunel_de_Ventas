import dash
import dash_table
from dash import html
from dash import dcc
from dash import Dash, html, Input, Output, callback_context, ctx
import plotly.express as px
import pandas as pd
import numpy as np
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
from dash.dependencies import Output, Input, State
import plotly.graph_objects as go
from datetime import datetime
import dash_daq as daq
# Importar hojas de trabajo de google drive     https://bit.ly/3uQfOvs
from googleapiclient.discovery import build
from google.oauth2 import service_account
from datetime import datetime
from datetime import date
import plotly.express as px
import dash_auth
import time
import mysql.connector
import pymysql
from sqlalchemy import create_engine
import sqlalchemy as sa



app = dash.Dash(__name__, external_stylesheets=[dbc.themes.SANDSTONE])
#app.css.append_css({'external_url': '/static/reset.css'})
#app.server.static_folder = 'static'
server = app.server

#auth = dash_auth.BasicAuth(
#    app,
#    {'MONICA': 'GERENTE',
#     'LUISA': 'COMERCIAL',
#     'NELSY': 'COMERCIAL',
#     'ADRIANA': 'GERENTE',
#     }
#)



data = [['3/11/2020', '9:32:00 a. m.', '9:34:00 a. m.', '0.8', '1']]
data = [['3/11/2020', '9:32:00 a. m.', '9:34:00 a. m.', '0.8', '1', '2', '0.8', '211.3376', '303.2', '80096.9504', '344', '124', '1'], ['3/11/2020', '9:47:00 a. m.', '9:48:00 a. m.', '1.1', '1.8', '1', '2.8', '739.6816', '307.2', '81153.6384', '347', '126', '1'], ['3/11/2020', '9:55:00 a. m.', '9:57:00 a. m.', '1.8', '2.2', '2', '1.6', '422.6752', '308.8', '81576.3136', '349', '127', '1']]
data = [['Borrar!', 'Borrar!', 'Borrar!', 'Borrar!', 'Borrar!', 'Borrar!']]
data = []


app.layout = dbc.Container([
    dcc.Store(id='store-data-df_ventas', storage_type='memory'),  # 'local' or 'session'
    dcc.Store(id='store-data-df_oportunidades-tabla', storage_type='memory'),  # 'local' or 'session'
    dcc.Store(id='store-data-df_ofertas-tabla', storage_type='memory'),  # 'local' or 'session'
    dcc.Store(id='store-data-df_principal', storage_type='memory'),  # 'local' or 'session'
    dcc.Store(id='store-data-df_secundaria', storage_type='memory'),  # 'local' or 'session'

    dcc.Interval(
        id='my_interval',
        disabled=False,
        interval=1 * 1000,
        n_intervals=0,
        max_intervals=1
    ),
    dbc.Row([
        dbc.Col([dbc.CardImg(
            src="/assets/Logo.jpg",

            style={"width": "6rem",
                   'text-align': 'center'},
        ),
        ]),
        dbc.Col(html.H5('"Cualquier tecnología lo suficientemente avanzada, es indistinguible de la magia." - Arthur C. Clarke '),style={'color':"green", 'font-family': "Franklin Gothic"})
    ]),
    dbc.Row([
        dbc.Tabs([
            dbc.Tab([
                # Dropdown base de datos principal
                dbc.Row([
                    dbc.Col([
                        dbc.Button(
                            "Base de Datos Principal:",
                            id="lista1-target",
                            color="primary",
                            style={'font-family': "Franklin Gothic"},
                            # className="me-1",
                            n_clicks=0,
                        ),
                        dbc.Popover(
                            "Muestra las bases de datos disponibles a las cuales se les pueden realizar el análisis de ventas principal.",
                            target="lista1-target",
                            body=True,
                            trigger="hover",
                            style={'font-family': "Franklin Gothic"}
                        ),

                    ], width=3, align='center', className="d-grid gap-2"),
                    dbc.Col(
                        dbc.Spinner(children=[dcc.Dropdown(id='lista-bd1', style={'font-family': "Franklin Gothic"})],
                                    size="lg",
                                    color="primary", type="border", fullscreen=True, ),
                        xs=3, sm=3, md=3, lg=6, xl=6, align='center'),
                ]),
                # Dropdown base de datos secundaria
                dbc.Row([
                    dbc.Col([
                        dbc.Button(
                            "Base de Datos Secundaria:",
                            id="lista2-target",
                            color="primary",
                            style={'font-family': "Franklin Gothic"},
                            # className="me-1",
                            n_clicks=0,
                        ),
                        dbc.Popover(
                            "Muestra las bases de datos disponibles a las cuales se les pueden realizar el análisis de ventas secundario.",
                            target="lista2-target",
                            body=True,
                            trigger="hover",
                            style={'font-family': "Franklin Gothic"}
                        ),

                    ], width=3, align='center', className="d-grid gap-2"),
                    dbc.Col(
                        dbc.Spinner(children=[dcc.Dropdown(id='lista-bd2', style={'font-family': "Franklin Gothic"})],
                                    size="lg",
                                    color="primary", type="border", fullscreen=True, ),
                        xs=3, sm=3, md=3, lg=6, xl=6, align='center'),
                ]),
                # Botón para iniciar análisis de ventas
                dbc.Row([
                    dbc.Col([
                        dbc.Spinner(children=[html.Button('Iniciar Análisis de Ventas', id='iniciar', n_clicks=0),], size="lg", color="primary",
                                    type="border", fullscreen=True,),

                    ])

                ]),
            ], label="Ingresar Bases de Datos", label_style={'color': '#082255', 'font-family': "Franklin Gothic"}),
            dbc.Tab([
                dbc.Row([
                    dbc.Col(html.H1(
                        "Túnel de Ventas",
                        style={'textAlign': 'center', 'color': '#082255', 'font-family': "Franklin Gothic"}),
                        width=12, )
                ]),
                html.Br(),
                dbc.Row([
                    dbc.Col([
                        dbc.Button(
                            "Base de Datos Seleccionada:",
                            id="base-datos-seleccionada-target",
                            value='Ventas Geo',
                            color="info",
                            style={'font-family': "Franklin Gothic"},
                            # className="me-1",
                            n_clicks=0,
                        ),
                    ], width=3, align='center', className="d-grid gap-2"),

                    dbc.Col([
                        html.Div(id='base_de_datos_primaria_seleccionada', style={'font-family': "Franklin Gothic"})
                    ], xs=2, sm=2, md=2, lg=2, xl=2, style={'textAlign': 'center'}, align='center'),
                ]),
                dbc.Row([
                    dbc.Col([
                        dbc.Button(
                            "Seleccionar Línea de Negocio:",
                            id="selec-agente-target",
                            value='Ventas Geo',
                            color="info",
                            style={'font-family': "Franklin Gothic"},
                            # className="me-1",
                            n_clicks=0,
                        ),
                    ], width=3, align='center', className="d-grid gap-2"),

                    dbc.Col(
                        dbc.Spinner(children=[dcc.Dropdown(id='linea', style={'font-family': "Franklin Gothic"})],
                                    size="lg",
                                    color="primary", type="border", fullscreen=True, ),
                        xs=3, sm=3, md=3, lg=4, xl=6, align='center'),
                ]),
                html.Br(),
                dbc.Row(dbc.Col(
                    dbc.Spinner(children=[dcc.Graph(id="embudo")], size="lg",
                                color="primary", type="border", fullscreen=True, ),
                    width={'size': 12, 'offset': 0}),
                ),
                html.Br(),
                dbc.Row(dbc.Col(
                    dbc.Spinner(children=[dcc.Graph(id="fig-barras-venta-prospecto-sumatoria")], size="lg",
                                color="primary", type="border", fullscreen=True, ),
                    width={'size': 12, 'offset': 0}),
                ),
                dbc.Row(dbc.Col(
                    dbc.Spinner(children=[dcc.Graph(id="fig-pie-potencial")], size="lg",
                                color="primary", type="border", fullscreen=True, ),
                    width={'size': 12, 'offset': 0}),
                ),
                dbc.Row(dbc.Col(
                    dbc.Spinner(children=[dcc.Graph(id="fig-barras-venta-prospecto-cantidad")], size="lg",
                                color="primary", type="border", fullscreen=True, ),
                    width={'size': 12, 'offset': 0}),
                ),
                dbc.Row(dbc.Col(
                    dbc.Spinner(children=[dcc.Graph(id="fig-pie-cantidad")], size="lg",
                                color="primary", type="border", fullscreen=True, ),
                    width={'size': 12, 'offset': 0}),
                ),

            ], label="Túnel de Ventas", label_style={'color': '#082255', 'font-family': "Franklin Gothic"}),
            dbc.Tab([
                dbc.Row([
                    dbc.Col(html.H1(
                        "Resumen Estado",
                        style={'textAlign': 'center', 'color': '#082255', 'font-family': "Franklin Gothic"}),
                        width=12, )
                ]),
                dbc.Row([
                    dbc.Col([
                        dbc.Button(
                            "Seleccionar Estado:",
                            id="selec-uni-dia-target",
                            color="info",
                            style={'font-family': "Franklin Gothic"},
                            # className="me-1",
                            n_clicks=0,
                        ),
                    ], xs=3, sm=3, md=3, lg=3, xl=3, align='center'),

                    dbc.Col(
                        dbc.Spinner(children=[dcc.Dropdown(id='Estado', style={'font-family': "Franklin Gothic"}, value='Leads',) ],
                                    size="lg",
                                    color="primary", type="border", fullscreen=True, ),
                        xs=3, sm=3, md=3, lg=6, xl=6, align='center'),
                ]),
                dbc.Row(dbc.Col(
                    dbc.Spinner(children=[dcc.Graph(id="fig-potencial")], size="lg",
                                color="primary", type="border", fullscreen=True, ),
                    width={'size': 12, 'offset': 0}),
                ),
                dbc.Row(dbc.Col(
                    dbc.Spinner(children=[dcc.Graph(id="fig-potencial-cantidad")], size="lg",
                                color="primary", type="border", fullscreen=True, ),
                    width={'size': 12, 'offset': 0}),
                ),

            ], label="Resumen Estado", label_style={'color': '#082255', 'font-family': "Franklin Gothic"}),

            dbc.Tab([
                dbc.Row([
                    dbc.Col(html.H1(
                        "Comparación Bases de Datos",
                        style={'textAlign': 'center', 'color': '#082255', 'font-family': "Franklin Gothic"}),
                        width=12, )
                ]),
                html.Br(),
                dbc.Row([
                    dbc.Col(html.H4(
                        "Ofertas que se encuentran en ambas bases de datos y han cambiado su estado",
                        style={'textAlign': 'center', 'color': '#082255', 'font-family': "Franklin Gothic"}),
                        width=12, )
                ]),
                # html.Br(),
                dbc.Row([
                    dbc.Col([
                        dbc.Button(
                            "Estado_y:",
                            id="base-datos-seleccionada-target2",
                            value='Ventas Geo',
                            color="info",
                            style={'font-family': "Franklin Gothic"},
                            # className="me-1",
                            n_clicks=0,
                        ),
                    ], width=3, align='center', className="d-grid gap-2"),

                    dbc.Col([
                        html.Div(id='base_de_datos_primaria_seleccionada2', style={'font-family': "Franklin Gothic"})
                    ], xs=2, sm=2, md=2, lg=2, xl=2, style={'textAlign': 'center'}, align='center'),
                ]),
                dbc.Row([
                    dbc.Col([
                        dbc.Button(
                            "Estado_x:",
                            id="base-datos-seleccionada-target3",
                            value='Ventas Geo',
                            color="info",
                            style={'font-family': "Franklin Gothic"},
                            # className="me-1",
                            n_clicks=0,
                        ),
                    ], width=3, align='center', className="d-grid gap-2"),

                    dbc.Col([
                        html.Div(id='base_de_datos_secundaria_seleccionada', style={'font-family': "Franklin Gothic"})
                    ], xs=2, sm=2, md=2, lg=2, xl=2, style={'textAlign': 'center'}, align='center'),
                ]),
                html.Br(),

                dbc.Row(
                    dash_table.DataTable(
                        id='datatable-df_comparado',
                        # columns=[
                        #     {"name": "Número oferta", "id": 1, "deletable": False, "selectable": False, "hideable": True},
                        #     {"name": "Estado oferta", "id": 2, "deletable": False, "selectable": False, "hideable": True},
                        #     {"name": "Fecha oferta", "id": 3, "deletable": False, "selectable": False, "hideable": True},
                        #     {"name": "Asunto", "id": 4, "deletable": False, "selectable": False, "hideable": True},
                        #     {"name": "Tipo oferta", "id": 5, "deletable": False, "selectable": False, "hideable": True},
                        #     {"name": "Cuenta", "id": 6, "deletable": False, "selectable": False, "hideable": True},
                        #     {"name": "Contacto", "id": 7, "deletable": False, "selectable": False, "hideable": True},
                        #     {"name": "Representante comercial", "id": 8, "deletable": False, "selectable": False, "hideable": True},
                        #     {"name": "Total", "id": 9, "deletable": False, "selectable": False, "hideable": True},
                        #     {"name": "Unidad de negocio", "id": 10, "deletable": False, "selectable": False, "hideable": True},
                        #     {"name": "Linea", "id": 11, "deletable": False, "selectable": False, "hideable": True},
                        #     {"name": "Sub linea", "id": 12, "deletable": False, "selectable": False, "hideable": True},
                        #     {"name": "Fuente", "id": 13, "deletable": False, "selectable": False, "hideable": True},
                        #     {"name": "Último seguimiento", "id": 14, "deletable": False, "selectable": False, "hideable": True},
                        #     {"name": "Situacion actual", "id": 15, "deletable": False, "selectable": False, "hideable": True},
                        #     {"name": "Entendimiento necesidad cliente", "id": 16, "deletable": False, "selectable": False, "hideable": True},
                        #     {"name": "Solucion propuesta", "id": 17, "deletable": False, "selectable": False, "hideable": True},
                        #     {"name": "Estado proceso comercial", "id": 18, "deletable": False, "selectable": False, "hideable": True},
                        #     {"name": "Razón perdida/cancelada", "id": 19, "deletable": False, "selectable": False, "hideable": True},
                        #
                        #
                        #
                        #
                        # ],
                        data=[],  # the contents of the table NO INICIALIZAR CON [] SINO CON EL ID!!!

                        editable=False,  # allow editing of data inside all cells
                        filter_action="none",  # allow filtering of data by user ('native') or not ('none')
                        sort_action="none",  # enables data to be sorted per-column by user or not ('none')
                        sort_mode="none",  # sort across 'multi' or 'single' columns
                        column_selectable="none",  # allow users to select 'multi' or 'single' columns
                        row_selectable="multi",  # allow users to select 'multi' or 'single' rows
                        row_deletable=True,  # choose if user can delete a row (True) or not (False)
                        selected_columns=[],  # ids of columns that user selects
                        selected_rows=[],  # indices of rows that user selects
                        page_action="native",  # all data is passed to the table up-front or not ('none')
                        page_current=0,  # page number that user is on
                        page_size=20,  # number of rows visible per page
                        style_cell={  # ensure adequate header width when text is shorter than cell's text
                            'minWidth': 95, 'maxWidth': 95, 'width': 95
                        },
                        style_data={  # overflow cells' content into multiple lines
                            'whiteSpace': 'normal',
                            'height': 'auto',
                            'font-family': "Franklin Gothic",
                            'textAlign': 'center',
                        },
                        style_header={
                            'font-family': "Franklin Gothic",
                            'textAlign': 'center',
                            'fontWeight': 'bold'
                        },

                    )
                ),
                html.Br(),
                html.Br(),
                html.Br(),
                html.Br(),

                dbc.Row([
                    dbc.Col(html.H4(
                        "Ofertas que se encuentran únicamente en la base de datos principal (nuevas)",
                        style={'textAlign': 'center', 'color': '#082255', 'font-family': "Franklin Gothic"}),
                        width=12, )
                ]),
                dbc.Row(
                    dash_table.DataTable(
                        id='datatable-df_primaria_only',
                        data=[],  # the contents of the table NO INICIALIZAR CON [] SINO CON EL ID!!!
                        editable=False,  # allow editing of data inside all cells
                        filter_action="none",  # allow filtering of data by user ('native') or not ('none')
                        sort_action="none",  # enables data to be sorted per-column by user or not ('none')
                        sort_mode="none",  # sort across 'multi' or 'single' columns
                        column_selectable="none",  # allow users to select 'multi' or 'single' columns
                        row_selectable="multi",  # allow users to select 'multi' or 'single' rows
                        row_deletable=True,  # choose if user can delete a row (True) or not (False)
                        selected_columns=[],  # ids of columns that user selects
                        selected_rows=[],  # indices of rows that user selects
                        page_action="native",  # all data is passed to the table up-front or not ('none')
                        page_current=0,  # page number that user is on
                        page_size=20,  # number of rows visible per page
                        style_cell={  # ensure adequate header width when text is shorter than cell's text
                            'minWidth': 95, 'maxWidth': 95, 'width': 95
                        },
                        style_data={  # overflow cells' content into multiple lines
                            'whiteSpace': 'normal',
                            'height': 'auto',
                            'font-family': "Franklin Gothic",
                            'textAlign': 'center',
                        },
                        style_header={
                            'font-family': "Franklin Gothic",
                            'textAlign': 'center',
                            'fontWeight': 'bold'
                        },

                    )
                ),
                html.Br(),
                dbc.Row(dbc.Col(
                    dbc.Spinner(children=[dcc.Graph(id="fig-barras-venta-prospecto-sumatoria_primaria_only")], size="lg",
                                color="primary", type="border", fullscreen=True, ),
                    width={'size': 12, 'offset': 0}),
                ),

                dbc.Row(dbc.Col(
                    dbc.Spinner(children=[dcc.Graph(id="fig-barras-venta-prospecto-cantidad_primaria_only")], size="lg",
                                color="primary", type="border", fullscreen=True, ),
                    width={'size': 12, 'offset': 0}),
                ),

            ], label="Comparación", label_style={'color': '#082255', 'font-family': "Franklin Gothic"}),
            dbc.Tab([
                dbc.Row([
                    dbc.Col([
                        html.Button("Descargar Base de Datos Principal a Excel", id="btn_xlsx"),
                        dcc.Download(id="download-dataframe-xlsx"),
                    ]),
                ]),
                dbc.Row([
                    dbc.Col([
                        html.Button("Descargar Base de Datos Secundaria a Excel", id="btn_xlsx2"),
                        dcc.Download(id="download-dataframe-xlsx2"),
                    ]),
                ]),
            ], label="Descargar", label_style={'color': '#082255', 'font-family': "Franklin Gothic"}),

        ]),
    ]),
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Div(id='output1', style={'font-family': "Franklin Gothic"})
                        ])
                    ]),
                ])
            ])
        ])
    ]),



])

# Inicializa los dropdowns de bases de datos principal y secundario
@app.callback(
    Output('lista-bd1', 'options'),
    Output('lista-bd2', 'options'),

    Input('my_interval', 'n_intervals'),
)

def dropdown_estado(value_intervals):

    # Obtiene la lista de bases de datos disponibles en clever console
    # Create SQLAlchemy engine to connect to MySQL Database
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                           .format(host='bieit9g0hwdvjmgqdijo-mysql.services.clever-cloud.com',
                                   db='bieit9g0hwdvjmgqdijo',
                                   user='ur13ijqsmkxtfd0k',
                                   pw='Nm8h1CMChO3AkubnN8zE'))

    # Obtener lista de bases de datos
    db_list = engine.table_names()

    return db_list, db_list


# Callback que transfiere las bases de datos seleccionadas al siguiente callback
@app.callback(
    Output('store-data-df_principal', 'data'),
    Output('store-data-df_secundaria', 'data'),
    Output('linea', 'options'),
    Output('base_de_datos_primaria_seleccionada', 'children'),
    Output('Estado', 'options'),
    Output('base_de_datos_primaria_seleccionada2', 'children'),
    Output('base_de_datos_secundaria_seleccionada', 'children'),
    Output('linea', 'value'),

    Input('iniciar', 'n_clicks'),
    State('lista-bd1', 'value'),
    State('lista-bd2', 'value'),

)
def transferir_bases_de_datos(n_clicks, value_principal, value_secundaria):

    if n_clicks >= 1:
        # Realiza conexión con phpMyAdmin Clever Console y transfiere los datos

        # Create SQLAlchemy engine to connect to MySQL Database
        engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                               .format(host='bieit9g0hwdvjmgqdijo-mysql.services.clever-cloud.com',
                                       db='bieit9g0hwdvjmgqdijo',
                                       user='ur13ijqsmkxtfd0k',
                                       pw='Nm8h1CMChO3AkubnN8zE'))


        prequery = "SELECT * FROM "

        selectquery_principal = prequery + value_principal
        df_principal = pd.read_sql_query(selectquery_principal, engine)
        df_principal.drop([0], inplace=True)
        df_principal = df_principal.rename(index=lambda x: x - 1)

        selectquery_secundaria = prequery + value_secundaria
        df_secundaria = pd.read_sql_query(selectquery_secundaria, engine)
        df_secundaria.drop([0], inplace=True)
        df_secundaria = df_secundaria.rename(index=lambda x: x - 1)

        df_principal = df_principal.replace(to_replace='None', value=np.nan).dropna(axis=0, how="all")
        df_principal = df_principal.apply(lambda x: x.str.replace("UEN Prevención y control de derrames", "U.E.N Prevencion y control de derrames"))
        df_principal = df_principal.apply(lambda x: x.str.replace("UEN Suministro e instalación Geosinteticos", "U.E.N Venta e Instalacion de Geosinteticos"))
        df_principal['Tipo Potencial'] = df_principal['Tipo Potencial'].apply(lambda x: "No reportado" if x == "" else x)
        df_principal['Ultimo Seguimiento'] = np.where(df_principal['Ultimo Seguimiento'] == "", df_principal['Fecha Contacto'], df_principal['Ultimo Seguimiento'])
        #df_principal['Ppto'] = df_principal['Ppto'].str.replace("$", "")
        #df_principal['Ppto'] = df_principal['Ppto'].str.replace(".", "")
        df_principal['Ppto'] = df_principal['Ppto'].str.replace(",", "")
        df_principal['Ppto'] = df_principal['Ppto'].str.replace("U", "")


        df_secundaria = df_secundaria.replace(to_replace='None', value=np.nan).dropna(axis=0, how="all")
        df_secundaria = df_secundaria.apply(lambda x: x.str.replace("UEN Prevención y control de derrames", "U.E.N Prevencion y control de derrames"))
        df_secundaria = df_secundaria.apply(lambda x: x.str.replace("UEN Suministro e instalación Geosinteticos", "U.E.N Venta e Instalacion de Geosinteticos"))
        df_secundaria['Tipo Potencial'] = df_secundaria['Tipo Potencial'].apply(lambda x: "No reportado" if x == "" else x)


        #df_secundaria['Ppto'] = df_secundaria['Ppto'].str.replace("$", "")
        #df_secundaria['Ppto'] = df_secundaria['Ppto'].str.replace(".", "")
        df_secundaria['Ppto'] = df_secundaria['Ppto'].str.replace(",", "")
        df_secundaria['Ppto'] = df_secundaria['Ppto'].str.replace("U", "")

        lineaDD = list(dict.fromkeys(df_principal["Unidad Estrategica"]))
        linea_value = lineaDD[0]
        estadoDD = list(dict.fromkeys(df_principal["Estado"]))

        # lineaDD = lineaDD.apply(lambda x: datetime.strptime(x, "%d/%m/%Y")  if (x != None and x != '') else x)

    return df_principal.to_dict('records'), df_secundaria.to_dict('records'), lineaDD, value_principal,\
           estadoDD, value_principal, value_secundaria, linea_value



@app.callback(
    Output(component_id='output1', component_property='children'),
    Output('embudo', component_property='figure'),
    Output('fig-pie-potencial', component_property='figure'),
    Output('fig-pie-cantidad', component_property='figure'),
    Output('fig-potencial', component_property='figure'),
    Output('fig-potencial-cantidad', component_property='figure'),
    Output('fig-barras-venta-prospecto-sumatoria', component_property='figure'),
    Output('fig-barras-venta-prospecto-cantidad', component_property='figure'),
    Output('datatable-df_comparado', component_property='data'),
    Output('datatable-df_primaria_only', component_property='data'),
    Output('fig-barras-venta-prospecto-sumatoria_primaria_only', component_property='figure'),
    Output('fig-barras-venta-prospecto-cantidad_primaria_only', component_property='figure'),

    Input('my_interval', 'n_intervals'),
    Input(component_id='linea', component_property='value'),
    Input(component_id='store-data-df_principal', component_property='data'),
    Input(component_id='Estado', component_property='value'),
    Input(component_id='store-data-df_secundaria', component_property='data'),
    State('lista-bd1', 'value'),
    State('lista-bd2', 'value'),


)

def tunel_de_ventas(value_interval, value_linea, df, value_estado, df_secundaria, value_principal, value_secundaria):
    df = pd.DataFrame(df)
    df = df.replace(to_replace='None', value=np.nan).dropna(axis=0, how="all")
    df_primaria = df

    df_secundaria = pd.DataFrame(df_secundaria)
    df_secundaria = df_secundaria.replace(to_replace='None', value=np.nan).dropna(axis=0, how="all")

    retorno =''


    # Asignar tipos a cada variable
    df["Fecha Contacto"] = df["Fecha Contacto"].apply(lambda x: datetime.strptime(x, "%d/%m/%Y")  if (x != None and x != '') else x)
    df["Ultimo Seguimiento"] = df["Ultimo Seguimiento"].apply(lambda x: datetime.strptime(x.strip(), "%d/%m/%Y") if (x != None and x != '') else x)
    df['Ppto'] = df['Ppto'].apply(pd.to_numeric, errors='ignore')
    df['Ppto'] = df['Ppto'].fillna(0)
    df["Tipo Potencial"] = df["Tipo Potencial"].apply(lambda x: x.strip() if (x != None and x != '') else x)


    # Filtra el dataframe por la línea comercial seleccionada
    df['Valor Venta Estimado'] = df['Ppto']
    df['Clase'] = df['Estado']
    df["Último Seguimiento"] = df["Ultimo Seguimiento"]
    df["Categoría oportunidad"] = df["Tipo Potencial"]
    df = df[df['Unidad Estrategica'] == value_linea]


    #pd.set_option('display.max_rows', None)

    # Número de clientes y venta potencial TOTALES
    clientesTotal = len(df['Valor Venta Estimado'])
    ventaProsTotal = df['Valor Venta Estimado'].sum()


    # Crea función que cuenta la cantidad y la venta potencial de un estado
    def cant_sum_estado(estado):
        df_estado = df[df['Clase'] == estado]
        cantidad = len(df_estado['Clase'])
        sum_propecto = df_estado['Valor Venta Estimado'].sum()
        return cantidad, sum_propecto

    # Número de clientes y venta potencial de POTENCIAL
    clientesPotenciales, ventaProsPot = cant_sum_estado('Potencial')
    # Número de clientes y venta potencial de LEADS
    clientesLeads, ventaProsLead = cant_sum_estado('Leads')
    # Número de clientes y venta potencial de DESARROLLO
    clientesDesa, ventaProsDesa = cant_sum_estado('Desarrollo')
    # Número de clientes y venta potencial EN PROPUESTA
    clientesProp, ventaProsProp = cant_sum_estado('En Propuesta')
    # Número de clientes y venta potencial GANADA
    clientesGanada, ventaProsGanada = cant_sum_estado('Ganada')
    # Número de clientes y venta potencial PERDIDA
    clientesPerdida, ventaProsPerdida = cant_sum_estado('Perdida')

    ############################ Crea Figura de pie chart ############################
    estados = df['Clase']
    estados = list(dict.fromkeys(estados))
    dfEstadoSumCant = pd.DataFrame(index=estados, columns=['Suma Prospecto', 'Cantidad', 'Estado'])

    for i in estados:
        dfestado = df[df["Clase"] == i]
        estadoProspSuma = dfestado['Valor Venta Estimado'].values.sum()
        estadoProspCant = len(dfestado['Valor Venta Estimado'])
        dfEstadoSumCant['Suma Prospecto'][i] = estadoProspSuma
        dfEstadoSumCant['Cantidad'][i] = estadoProspCant
        dfEstadoSumCant['Estado'][i] = i

    # print(dfEstadoSumCant)

    def crear_pie_chart(df, valores, nombre, titulo):
        figura_pie = px.pie(
            data_frame=df,
            values=valores,
            names=nombre,
            color=nombre,  # differentiate markers (discrete) by color
            color_discrete_map={"Potencial": "light green", "Leads": "blue", "Desarrollo": "red",
                                "En Propuesta": "green",
                                "Ganada": "yellow", "Perdida": "black"},
            title=titulo,  # figure title
            hole=0.5,  # represents the hole in middle of pie
        )

        figura_pie.update_layout(
            font_family="Franklin Gothic",
            title_font_family="Franklin Gothic",
        )

        return figura_pie

    pie_chart_prospeccto = crear_pie_chart(dfEstadoSumCant, 'Suma Prospecto', 'Estado', 'Prospecto de Ventas por Valor')

    pie_chart_cantidad = crear_pie_chart(dfEstadoSumCant, 'Cantidad', 'Estado', 'Prospecto de Ventas por Cantidad')


    ############################ Gráficas de Barras Ventas Prospecto ###################################
    def crear_figura_barras(df, x, y, titulo, x_titulo, y_titulo):
        figura = px.bar(df, x=x, y=y)

        figura.update_layout(
            font_family="Franklin Gothic",
            title_font_family="Franklin Gothic",
            barmode='group',
            title=titulo,
            xaxis_title=x_titulo,
            yaxis_title=y_titulo
        )

        return figura

    fig_barras_ventas_pros_cant = crear_figura_barras(dfEstadoSumCant, 'Estado', 'Cantidad',
                                                      "Prospecto de Ventas por Cantidad", "Estado", "Cantidad")

    fig_barras_ventas_pros_sum = crear_figura_barras(dfEstadoSumCant, 'Estado', 'Suma Prospecto',
                                                      "Prospecto de Ventas por Valor", "Estado", "Sumatoria Propecto [COP]")




    ############################ Cajas del túnel de ventas ###################################

    # Create figure
    fig = go.Figure()

    # Constants
    img_width = 1600
    img_height = 900
    scale_factor = 0.5

    # Add invisible scatter trace.
    # This trace is added to help the autoresize logic work.
    fig.add_trace(
        go.Scatter(
            x=[0, img_width * scale_factor],
            y=[0, img_height * scale_factor],
            mode="markers",
            marker_opacity=0
        )
    )

    # Configure axes
    fig.update_xaxes(
        visible=False,
        range=[0, img_width * scale_factor]
    )

    fig.update_yaxes(
        visible=False,
        range=[0, img_height * scale_factor],
        # the scaleanchor attribute ensures that the aspect ratio stays constant
        scaleanchor="x"
    )

    # Add image
    fig.add_layout_image(
        dict(
            x=0,
            sizex=img_width * scale_factor,
            y=img_height * scale_factor,
            sizey=img_height * scale_factor,
            xref="x",
            yref="y",
            opacity=1.0,
            layer="below",
            sizing="stretch",
            source="/assets/Embudo_de_Ventas.png")
    )

    # Configure other layout
    fig.update_layout(
        width=img_width * scale_factor,
        height=img_height * scale_factor,
        margin={"l": 0, "r": 0, "t": 0, "b": 0},
    )

    # Crea una función para crear las cajas de cada estado en la figura del túnel de ventas
    def crear_caja(figura, x1, y1, texto1, x2, y2, texto2, x3, y3, texto3):

        figura.add_annotation(
            x=x1,
            y=y1,
            text=texto1,
            yanchor='bottom',
            showarrow=False,
            font=dict(size=30, color="black", family="Franklin Gothic"),
            align="left",
            bordercolor='black',
            borderwidth=2,
            bgcolor="#CFECEC",
            opacity=0.8,
        )
        figura.add_annotation(
            x=x2,
            y=y2,
            text=texto2,
            yanchor='bottom',
            showarrow=False,
            font=dict(size=30, color="black", family="Franklin Gothic"),
            align="left",
            bordercolor='black',
            borderwidth=2,
            bgcolor="#CFECEC",
            opacity=0.8,
        )

        texto3 = round(texto3 / 1000000, 2)
        texto3 = str(texto3) + ' MM'
        figura.add_annotation(
            x=x3,
            y=y3,
            text=texto3,
            yanchor='bottom',
            showarrow=False,
            font=dict(size=30, color="black", family="Franklin Gothic"),
            align="left",
            bordercolor='black',
            borderwidth=2,
            bgcolor="#CFECEC",
            opacity=0.8,
        )



        return figura

    crear_caja(fig, 105, 365, 'Potenciales', 397, 365, clientesPotenciales, 690, 365, ventaProsPot)
    crear_caja(fig, 150, 278, 'Leads', 397, 278, clientesLeads, 653, 278, ventaProsLead)
    crear_caja(fig, 189, 191, 'Desarrollo', 397, 191, clientesDesa, 620, 191, ventaProsDesa)
    crear_caja(fig, 226, 106, 'Propuesta', 397, 106, clientesProp, 573, 106, ventaProsProp)
    crear_caja(fig, 260, 30, 'Ganadas', 397, 30, clientesGanada, 540, 30, ventaProsGanada)
    crear_caja(fig, 751, 191, 'Perdidas', 753, 106, clientesPerdida, 730, 30, ventaProsPerdida)




    # Crea figuras de ventas potenciales y cantidad de potenciales por categoría por mes

    dfEstado = df[df['Clase'] == str(value_estado)] # Clase
    dfEstadoxx = dfEstado.loc[:,  ['Contratante', 'Fecha Contacto', 'Ultimo Seguimiento',]]
    pd.set_option('display.max_columns', None)

    print("dfEstadoxx")
    print(dfEstadoxx)
    #dfEstado = df
    tipoDePotencial = list(dict.fromkeys(dfEstado['Tipo Potencial'])) # Fuente
    print("tipoDePotencial")
    print(tipoDePotencial)
    dfEstado["Último Seguimiento"] = pd.to_datetime(dfEstado["Último Seguimiento"]) # Último Seguimiento

    figTipodePotencialPotencial = go.Figure()
    figTipodePotencialCantidad = go.Figure()

    for i in tipoDePotencial:
        print(i)
        tipoDePotenciali = dfEstado[dfEstado["Tipo Potencial"] == i] # Fuente
        print(tipoDePotenciali)

        tipoDePotencialiMES = tipoDePotenciali.groupby(tipoDePotenciali['Último Seguimiento'].dt.to_period('M')).sum()
        tipoDePotencialiMESfrec = tipoDePotenciali.groupby(tipoDePotenciali['Último Seguimiento'].dt.to_period('M')).count()
        tipoDePotencialiMESfrec = tipoDePotencialiMESfrec['Valor Venta Estimado'] # Valor Venta Estimado
        xgraph = tipoDePotencialiMES.index.to_timestamp()
        ygraph = tipoDePotencialiMES['Valor Venta Estimado'].values.tolist() # Valor Venta Estimado
        figTipodePotencialPotencial.add_trace(go.Bar(x=xgraph,
                                                         y=ygraph,
                                                         name=i, ))
        ygraph = tipoDePotencialiMESfrec.values.tolist()
        figTipodePotencialCantidad.add_trace(go.Bar(x=xgraph, y=ygraph, name=i, ))

    figTipodePotencialPotencial.update_xaxes(tickformat="%b %Y")
    figTipodePotencialPotencial.update_layout(title="Ventas Potenciales Por Valor", xaxis_title="Fecha", yaxis_title="Valor [COP]")
    figTipodePotencialPotencial.update_layout(legend=dict(orientation="h",
                                         yanchor="bottom",
                                         y=-0.5,
                                         xanchor="center",
                                         x=0.5
                                         ))

    figTipodePotencialPotencial.update_layout(
        font_family="Franklin Gothic",
        title_font_family="Franklin Gothic",
    )
    figTipodePotencialPotencial.update_xaxes(title_font_family="Franklin Gothic")

    figTipodePotencialCantidad.update_xaxes(tickformat="%b %Y")
    figTipodePotencialCantidad.update_layout(title="Ventas Potenciales Por Cantidad", xaxis_title="Fecha", yaxis_title="Cantidad")
    figTipodePotencialCantidad.update_layout(legend=dict(orientation="h",
                                         yanchor="bottom",
                                         y=-0.5,
                                         xanchor="center",
                                         x=0.5
                                         ))

    figTipodePotencialCantidad.update_layout(
        font_family="Franklin Gothic",
        title_font_family="Franklin Gothic",
    )
    figTipodePotencialCantidad.update_xaxes(title_font_family="Franklin Gothic")


    # Crea figuras de ventas potenciales y cantidad de potenciales por categoría por mes DE LAS POTENCIALES

    dfEstado2 = df[df['Clase'] == 'Potenciales'] # Clase
    tipoDePotencial2 = list(dict.fromkeys(dfEstado2['Categoría oportunidad'])) # Fuente
    dfEstado2["Último Seguimiento"] = pd.to_datetime(dfEstado2["Último Seguimiento"]) # Último Seguimiento

    figTipodePotencialPotencial2 = go.Figure()
    figTipodePotencialCantidad2 = go.Figure()

    for i in tipoDePotencial2:
        tipoDePotenciali2 = dfEstado2[dfEstado2["Categoría oportunidad"] == i] # Fuente
        tipoDePotencialiMES2 = tipoDePotenciali2.groupby(tipoDePotenciali2['Último Seguimiento'].dt.to_period('M')).sum()
        tipoDePotencialiMESfrec2 = tipoDePotenciali2.groupby(tipoDePotenciali2['Último Seguimiento'].dt.to_period('M')).count()
        tipoDePotencialiMESfrec2 = tipoDePotencialiMESfrec2['Valor Venta Estimado'] # Valor Venta Estimado
        xgraph2 = tipoDePotencialiMES2.index.to_timestamp()
        ygraph2 = tipoDePotencialiMES2['Valor Venta Estimado'].values.tolist() # Valor Venta Estimado
        figTipodePotencialPotencial2.add_trace(go.Bar(x=xgraph2,
                                                         y=ygraph2,
                                                         name=i, ))
        ygraph2 = tipoDePotencialiMESfrec2.values.tolist()
        figTipodePotencialCantidad2.add_trace(go.Bar(x=xgraph2, y=ygraph2, name=i, ))

    figTipodePotencialPotencial2.update_xaxes(tickformat="%b %Y")
    figTipodePotencialPotencial2.update_layout(title="Ventas Potenciales Por Valor", xaxis_title="Fecha", yaxis_title="Valor [COP]")
    figTipodePotencialPotencial2.update_layout(legend=dict(orientation="h",
                                         yanchor="bottom",
                                         y=-0.5,
                                         xanchor="center",
                                         x=0.5
                                         ))

    figTipodePotencialPotencial2.update_layout(
        font_family="Franklin Gothic",
        title_font_family="Franklin Gothic",
    )
    figTipodePotencialPotencial2.update_xaxes(title_font_family="Franklin Gothic")

    figTipodePotencialCantidad2.update_xaxes(tickformat="%b %Y")
    figTipodePotencialCantidad2.update_layout(title="Ventas Potenciales Por Cantidad", xaxis_title="Fecha", yaxis_title="Cantidad")
    figTipodePotencialCantidad2.update_layout(legend=dict(orientation="h",
                                         yanchor="bottom",
                                         y=-0.5,
                                         xanchor="center",
                                         x=0.5
                                         ))

    figTipodePotencialCantidad2.update_layout(
        font_family="Franklin Gothic",
        title_font_family="Franklin Gothic",
    )
    figTipodePotencialCantidad2.update_xaxes(title_font_family="Franklin Gothic")

    # Obtiene los clientes que aparecen únicamente en la base de datos principal
    df_primaria_only = df_primaria[df_primaria['Gestion / Oferta'].isin(df_secundaria['Gestion / Oferta']) == False]
    df_primaria_only = df_primaria_only.loc[:, ["Gestion / Oferta", 'Contratante', "Estado", 'Ppto']]


    # Obtiene los clientes que aparecen únicamente en la base de datos secundaria
    df_secundaria_only = df_secundaria[df_secundaria['Gestion / Oferta'].isin(df_primaria['Gestion / Oferta']) == False]

    # Obtiene los clientes que se encuentran en ambas bases de datos
    df_interseccion = df_primaria['Gestion / Oferta'].isin(df_secundaria['Gestion / Oferta'])
    df_interseccion = df_primaria[df_interseccion]
    # df_interseccion = df_interseccion['Gestion / Oferta']

    # df_interseccion = df_secundaria['Gestion / Oferta'].isin(df_primaria['Gestion / Oferta'])
    # df_interseccion = df_secundaria[df_interseccion]

    # df_interseccion = df_interseccion[df_interseccion != 'None']
    # df_interseccion = df_interseccion[df_interseccion != None]
    df_interseccion = df_interseccion[df_interseccion['Gestion / Oferta'] != '']

    # Tomando Gestion / Oferta como llave principal, se compara el cambio del Estado
    suf_sec = value_secundaria
    suf_prim = value_principal
    df_comparado = df_secundaria.merge(df_primaria, on=['Gestion / Oferta'], how='left', indicator='Match')
    # df_comparado = df_comparado.loc[:, ["Gestion / Oferta", "Estado_x", "Estado_y", "Match"]]
    # df_comparado = df_comparado[df_comparado["Match"] == 'left']
    df_comparado = df_comparado[df_comparado["Gestion / Oferta"] != '']
    df_comparado = df_comparado[df_comparado["Estado_x"] != df_comparado["Estado_y"]]
    df_comparado[value_principal] = df_comparado["Estado_y"]
    df_comparado[value_secundaria] = df_comparado["Estado_x"]
    # df_comparado = df_comparado.loc[:, ["Gestion / Oferta", value_secundaria, value_principal, "Match"]]
    df_comparado = df_comparado.loc[:, ["Gestion / Oferta", 'Contratante_y', "Estado_x", "Estado_y", 'Ppto_y']]

    #df_comparado = df_comparado.columns

    # pd.set_option('display.max_columns', None)

    ############################ Crea Figura de pie chart ############################
    df_primaria_only
    # df_primaria_only_g = df_primaria_only.groupby('Estado').count().reset_index()
    df_primaria_only_g = df_primaria_only.groupby('Estado').count().reset_index()
    df_primaria_only_gs = df_primaria_only.groupby('Estado').sum()
    # df_primaria_only_g = df_primaria_only['Estado'].nunique()

    ############################ Gráficas de Barras Ventas Prospecto ###################################

    fig_barras_ventas_pros_cant_primaria_only = crear_figura_barras(df_primaria_only_g, 'Estado', 'Gestion / Oferta',
                                                      "Prospecto de Ventas por Cantidad", "Estado", "Cantidad")

    fig_barras_ventas_pros_sum_primaria_only = px.bar(df_primaria_only_gs)

    fig_barras_ventas_pros_sum_primaria_only.update_layout(
        font_family="Franklin Gothic",
        title_font_family="Franklin Gothic",
        barmode='group',
        title="Prospecto de Ventas",
        xaxis_title="Estado",
        yaxis_title="Sumatoria Propecto [COP]"
    )



    return retorno, fig, pie_chart_prospeccto, pie_chart_cantidad, figTipodePotencialPotencial, \
           figTipodePotencialCantidad, \
           fig_barras_ventas_pros_sum, fig_barras_ventas_pros_cant, df_comparado.to_dict('records'), \
           df_primaria_only.to_dict('records'), fig_barras_ventas_pros_cant_primaria_only, \
           fig_barras_ventas_pros_sum_primaria_only




# Descargar bases de datos principal a hojas de excel
@app.callback(
    Output("download-dataframe-xlsx", "data"),

    Input(component_id='store-data-df_principal', component_property='data'),
    Input('lista-bd1', 'value'),
    Input("btn_xlsx", "n_clicks"),

    prevent_initial_call=True,
)
def descargar_principal_excel(df_principal, nombre_df_principal, n_clicks):

    df_principal = pd.DataFrame(df_principal)

    nombre_excel = nombre_df_principal + ".xlsx"

    if "btn_xlsx" == ctx.triggered_id:
        enviar = dcc.send_data_frame(df_principal.to_excel, nombre_excel, sheet_name=nombre_excel)

    return enviar


# Descargar bases de datos a hojas de excel
@app.callback(
    Output("download-dataframe-xlsx2", "data"),

    Input(component_id='store-data-df_secundaria', component_property='data'),
    Input('lista-bd2', 'value'),
    Input("btn_xlsx2", "n_clicks"),

    prevent_initial_call=True,
)
def descargar_secundaria_excel(df_secundaria, nombre_df_secundaria, n_clicks2,):

    df_secundaria = pd.DataFrame(df_secundaria)

    nombre_excel2 = nombre_df_secundaria + ".xlsx"

    if "btn_xlsx2" == ctx.triggered_id:
        enviar = dcc.send_data_frame(df_secundaria.to_excel, nombre_excel2, sheet_name=nombre_excel2)

    return enviar


if __name__ == '__main__':
    app.run_server()
    #app.run_server(debug=False, port=8077, threaded=True)

