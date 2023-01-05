import dash
from dash import html
from dash import dcc
import plotly.express as px
import pandas as pd
from pandas.io import sql
import numpy as np
import dash_bootstrap_components as dbc
from dash.dependencies import Output, Input, State
import plotly.graph_objects as go
from datetime import datetime
import dash_daq as daq
# Importar hojas de trabajo de google drive     https://bit.ly/3uQfOvs
from googleapiclient.discovery import build
from google.oauth2 import service_account
from datetime import datetime
import time
import mysql.connector
import pymysql
from sqlalchemy import create_engine
import sqlalchemy as sa
from pandas.io import sql

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.SANDSTONE])
app.css.append_css({'external_url': '/static/reset.css'})
app.server.static_folder = 'static'
server = app.server

app.layout = dbc.Container([
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
                   'text-align': 'right'},
        ),

        ], align='right', width=2),
        dbc.Col(html.H5(
            '"Cualquier tecnología lo suficientemente avanzada, es indistinguible de la magia." - Arthur C. Clarke '),
                style={'color': "green", 'font-family': "Franklin Gothic"}, width=7),

    ]),
    dbc.Row([
        dbc.Col(html.H1(
            "Ingreso de Datos - Google Sheets a MYSQL en la Nube - Túnel de Ventas",
            style={'textAlign': 'center', 'color': '#082255', 'font-family': "Franklin Gothic"}), width=12, )
    ]),
    dbc.Row([
        dbc.Col([
            dbc.Accordion([
                dbc.AccordionItem([
                    html.H5([
                                "La siguiente aplicación permite subir a la nube los datos provenientes del archivo de excel en línea del siguiente link: https://docs.google.com/spreadsheets/d/1VVwtc5JN3QBvoyWRzymzEAohr9i3ak-TjlhFsGv2FNc/edit#gid=791180031 pestaña nombrada 'Ingreso de Datos' a la base de datos MySQL en la nube de Clever Console.  "])

                ], title="Introducción"),
            ], start_collapsed=True, style={'textAlign': 'left', 'color': '#082255', 'font-family': "Franklin Gothic"}),

        ], style={'color': '#082255', 'font-family': "Franklin Gothic"}),
    ]),
    dbc.Row([
        dbc.Col([
            dbc.Accordion([
                dbc.AccordionItem([
                    html.H5([
                        'Para poder ingresar una nueva tabla a la base de datos MySQL en la nube se necesitan los siguientes requisitos:']),

                    html.H5([
                        "1. Las columnas deben tener los siguientes nombres y en el siguiente orden: ['Item', 'Contratante', 'Unidad Estrategica', 'Tipo de Servicio', 'B. D. Inicial', 'Tipo Potencial', 'Fecha Contacto', 'Calificacion', 'Gestion / Oferta', 'Pedido', 'Ultimo Seguimiento', 'Obs. Ultimo seguimiento', 'Nombre del Proyecto', 'Estado', 'Año', 'Alcance', 'Ppto', 'Observaciones', 'Madurez', 'Seguimiento', ]."]),

                    html.H5([
                        "2. Las columnas 'Fecha Contacto' y 'Último Seguimiento' deben tener el formato fecha corto dd/mm/aaa."]),

                    html.H5([
                        "3. Se deben eliminar las filas cuyo 'Item' no posea ningún tipo de información además de su número."]),

                    html.H5([
                        "4. La columna 'Ppto' debe estar en formato número, es decir sin el simbolo $, sin separador de miles y con separador decimal con punto '.'."]),
                ], title="Requisitos"),
            ], start_collapsed=True, style={'textAlign': 'left', 'color': '#082255', 'font-family': "Franklin Gothic"}),

        ], style={'color': '#082255', 'font-family': "Franklin Gothic"}),
    ]),

    dbc.Row([
        dbc.Tabs([
            dbc.Tab([
                dbc.Row([
                    dbc.Col([
                        dcc.Textarea(
                            id='textarea-example',
                            # value='Ingrese el nombre de la base de datos que quiere transferir a la nube.',
                            style={'width': '60%', },
                        ),
                    ]),
                ]),
                dbc.Row([
                    dbc.Col([
                        html.Button('Ingresar', id='ingresar', n_clicks=0),
                        dbc.Spinner(children=[html.Div(id='mensaje-de-exito',
                                                       children='Presione "Ingresar" para enviar los datos de Google Sheets a MYSQL')],
                                    size="lg", color="primary",
                                    type="border", fullscreen=True, ),

                    ])

                ]),
            ], label="Ingresar Bases de Datos", label_style={'color': '#082255', 'font-family': "Franklin Gothic"}),

            dbc.Tab([

                dbc.Row([
                    dbc.Col(
                        dbc.Spinner(children=[dcc.Dropdown(id='lista-nombre-bd', style={'font-family': "Franklin Gothic"})],
                                    size="lg",
                                    color="primary", type="border", fullscreen=True, ),
                        xs=3, sm=3, md=3, lg=4, xl=6, align='center'),
                ]),
                dbc.Row([
                    dbc.Col([
                        dcc.Textarea(
                            id='nuevo-nombre',
                            # value='Ingrese el nombre de la base de datos que quiere transferir a la nube.',
                            style={'width': '60%', },
                        ),
                    ]),
                ]),
                dbc.Row([
                    dbc.Col([
                        html.Button('Cambiar Nombre', id='cambiar', n_clicks=0),
                        dbc.Spinner(children=[html.Div(id='mensaje-de-exito-cambiar-nombre',
                                                       children='Presione "Cambiar" para cambiar el nombre de la base de datos MYSQL')],
                                    size="lg", color="primary",
                                    type="border", fullscreen=True, ),

                    ])

                ]),

            ], label="Camiar Nombre Bases de Datos", label_style={'color': '#082255', 'font-family': "Franklin Gothic"}),
            dbc.Tab([
                dbc.Row([
                    dbc.Col(
                        dbc.Spinner(children=[dcc.Dropdown(id='lista-bd', style={'font-family': "Franklin Gothic"})],
                                    size="lg",
                                    color="primary", type="border", fullscreen=True, ),
                        xs=3, sm=3, md=3, lg=4, xl=6, align='center'),
                ]),
                dbc.Row([
                    dbc.Col([
                        html.Button('Eliminar', id='eliminar', n_clicks=0),
                        dbc.Spinner(children=[html.Div(id='mensaje-de-exito-eliminar',
                                                       children='Presione "Eliminar" para eliminar la base de datos MYSQL')],
                                    size="lg", color="primary",
                                    type="border", fullscreen=True, ),

                    ])

                ]),

            ], label="Eliminar Bases de Datos", label_style={'color': '#082255', 'font-family': "Franklin Gothic"}),
        ]),
    ]),


])


# Inicializa las opciones del dropdown de 'Eliminar Base de Datos'

@app.callback(
    Output('lista-bd', 'options'),
    Output('lista-nombre-bd', 'options'),

    Input('my_interval', 'n_intervals'),
)
def dropdownTiempoReal(value_intervals):

    # Create SQLAlchemy engine to connect to MySQL Database
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                           .format(host='bieit9g0hwdvjmgqdijo-mysql.services.clever-cloud.com',
                                   db='bieit9g0hwdvjmgqdijo',
                                   user='ur13ijqsmkxtfd0k',
                                   pw='Nm8h1CMChO3AkubnN8zE'))

    # Obtener lista de bases de datos
    db_list = engine.table_names()

    return db_list, db_list



# Callback de ingreso de datos a la nube
@app.callback(
    Output('mensaje-de-exito', 'children'),
    Input('ingresar', 'n_clicks'),
    State('textarea-example', 'value')
    # Input('lista-bd', 'value'),

)
def ingresar_bases_de_datos(n_clicks, value_textbox):

    if n_clicks >= 1:
        start = time.time()
        # Extraer Ingreso de Datos de google sheets

        namesSQL = ['Item', 'Contratante', 'Unidad Estrategica', 'Tipo de Servicio', 'B. D. Inicial', 'Tipo Potencial', 'Fecha Contacto', 'Calificacion', 'Gestion / Oferta', 'Pedido', 'Ultimo Seguimiento', 'Obs. Ultimo seguimiento', 'Nombre del Proyecto', 'Estado', 'Año', 'Alcance', 'Ppto', 'Observaciones', 'Madurez', 'Seguimiento', ]

        SERVICE_ACCOUNT_FILE = 'keys_tunel_ventas.json'
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        creds = None
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)

        # The ID spreadsheet.
        SAMPLE_SPREADSHEET_ID = '1VVwtc5JN3QBvoyWRzymzEAohr9i3ak-TjlhFsGv2FNc'

        SAMPLE_RANGE_COMBINADO = "Ingreso de Datos"

        service = build('sheets', 'v4', credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result_COMBINADO = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                              range=SAMPLE_RANGE_COMBINADO).execute()

        dfCOMBINADO3 = result_COMBINADO.get('values', [])
        dfCOMBINADO3 = pd.DataFrame(dfCOMBINADO3, columns=namesSQL)
        dfCOMBINADO3.drop([0], inplace=True)
        dfCOMBINADO3 = dfCOMBINADO3.rename(index=lambda x: x - 1)

        # Realiza conexión con phpMyAdmin Clever Console y transfiere los datos

        # Create SQLAlchemy engine to connect to MySQL Database
        engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                               .format(host='bieit9g0hwdvjmgqdijo-mysql.services.clever-cloud.com',
                                       db='bieit9g0hwdvjmgqdijo',
                                       user='ur13ijqsmkxtfd0k',
                                       pw='Nm8h1CMChO3AkubnN8zE'))

        # Corregir ingreso del nombre de la base de datos
        value_textbox = value_textbox.replace(" ", "_")
        value_textbox = value_textbox.replace(".", "")
        value_textbox = value_textbox.replace("/", "")

        dfCOMBINADO3.to_sql(con=engine, name=value_textbox, if_exists='replace', index=False)
        end = time.time()
        t = end - start
        print('Tiempo Clever Console es:')
        print(t)

    return 'Los datos han sido enviados exitosamente de Google sheets a Clever Console MYSQL {} veces'.format(
        n_clicks
    )



# Callback que cambia el nombre de las bases de datos en la nube
@app.callback(
    Output('mensaje-de-exito-cambiar-nombre', 'children'),
    Input('cambiar', 'n_clicks'),
    State('lista-nombre-bd', 'value'),
    State('nuevo-nombre', 'value')

)
def cambiar_nombre_bases_de_datos(n_clicks, value_lista, value_nuevo_nombre):

    if n_clicks >= 1:
        start = time.time()
        # Realiza conexión con phpMyAdmin Clever Console y transfiere los datos
        User = 'ur13ijqsmkxtfd0k'

        # Create SQLAlchemy engine to connect to MySQL Database
        engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                               .format(host='bieit9g0hwdvjmgqdijo-mysql.services.clever-cloud.com',
                                       db='bieit9g0hwdvjmgqdijo',
                                       user=User,
                                       pw='Nm8h1CMChO3AkubnN8zE'))

        query = "RENAME TABLE " + value_lista + " TO " + value_nuevo_nombre
        sql.execute(query, engine)
        # sql.execute(engine)

        end = time.time()
        t = end - start
        print('Tiempo Clever Console cambiar nombre es:')
        print(t)

    return 'El nombre de la base de datos ha sido combiado exitosamente de Clever Console MYSQL {} veces'.format(
        n_clicks
    )



# Callback que elimina bases de datos en la nube
@app.callback(
    Output('mensaje-de-exito-eliminar', 'children'),
    Input('eliminar', 'n_clicks'),
    State('lista-bd', 'value'),

)
def eliminar_bases_de_datos(n_clicks, value_lista):

    if n_clicks >= 1:
        start = time.time()
        # Realiza conexión con phpMyAdmin Clever Console y transfiere los datos
        User = 'ur13ijqsmkxtfd0k'

        # Create SQLAlchemy engine to connect to MySQL Database
        engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                               .format(host='bieit9g0hwdvjmgqdijo-mysql.services.clever-cloud.com',
                                       db='bieit9g0hwdvjmgqdijo',
                                       user=User,
                                       pw='Nm8h1CMChO3AkubnN8zE'))

        # connection = engine.raw_connection()
        # cursor = connection.cursor()
        # command = "DROP TABLE IF EXISTS {};".format(value_lista)
        # cursor.execute(command)
        # connection.commit()
        # cursor.close()

        sql.execute('DROP TABLE IF EXISTS %s' % value_lista, engine)
        # sql.execute(engine)

        end = time.time()
        t = end - start
        print('Tiempo Clever Console Eliminación es:')
        print(t)

    return 'Los datos han sido eliminados exitosamente de Clever Console MYSQL {} veces'.format(
        n_clicks
    )


if __name__ == '__main__':
    app.run_server()

