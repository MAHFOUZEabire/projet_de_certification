from utils import *
import dash
import dash_core_components as dcc
import dash_html_components as html
import warnings
warnings.filterwarnings('ignore')
import dash_table
from dash.dependencies import Input, Output




app = dash.Dash()

"""Le code est organise soue trois parties:
1. recuprer les données a analyser avec des requete SQL NOSQL et faire les analyse.
2. Creer les graphique: apartir des données analyse et avec pie chart et foluim
3. visualiser les graphique sur le tableau de board avec app.layout 

"""





#1. recuprer les données a analyser avec des requete SQL NOSQL et faire les analyse.

# connecter au bd:
utilisateur = "postgres"
#mot_passe = os.environ.get('pg_psw')
mot_passe="Line2009"
nom_bdd="dpe_logement"
conn=ouvrir_connection(nom_bdd, utilisateur, mot_passe, host='localhost', port=5432)


# récuperer les données geographique a partir de MongoDb:
geodonnees=lire_geo_collection_MongoDb("geo_communes","loire_atlantique")



# creation une vue:
creation_vue_logement = """CREATE VIEW logement_info(
                      classe_consommation_energie,
                      consommation_energie,
	              annee_construction,
	              nom_commune,
	              numero_rue,
	              type_voie,
	              nom_rue,
	              SHON)
                      AS
                      SELECT classe_consommation_energie,
                      consommation_energie,
	              annee_construction,
	              nom_commune,
	              numero_rue,
	              type_voie,
	              nom_rue,
	              ROUND((surface_habitable/0.80)::numeric, 2) AS SHON
                      FROM logement_description
                      LEFT JOIN adresse ON logement_description.id_adresse=adresse.id_adresse
	              LEFT JOIN surface ON logement_description.id_logement=surface.id_logement
	              ORDER BY classe_consommation_energie
	              """

#executer_requete(creation_vue_logement, conn)



# requete sql afin de visualiser les nombres de logements par classe de consommation sur piechart
requet_sql="""SELECT classe_consommation_energie,
            COUNT(classe_consommation_energie) AS nombre_logement
            FROM logement_info
            GROUP BY classe_consommation_energie
            ORDER BY classe_consommation_energie;
            """
nb_logement_class = pandas.read_sql_query(requet_sql, conn)


# recuperation les données geo des communes:
#recuprer les nome de commune a partir de MongoDb
commun_df=commune(geodonnees)
commun_df=commun_df[["nom_commune","longitude","latitude"]]
commun_dict=commun_df[["nom_commune"]].to_dict()


# requet sql afin de visualiser les nombres de logement(classe G F) dans les differente communes sur la carte:

requet_sql="""SELECT nom_commune,
            COUNT(classe_consommation_energie) AS nombre_logement
            FROM logement_info
            WHERE classe_consommation_energie='G' OR classe_consommation_energie='F'
            GROUP BY nom_commune
            ORDER BY nombre_logement DESC;
            """
nb_logement_a_renover_commune=pandas.read_sql_query(requet_sql,conn)                                              
#ajouter les données geo a nb_logement_a_renover_commune dataframe:
geo_communes_renov=commun_df[commun_df["nom_commune"].isin(nb_logement_a_renover_commune["nom_commune"])]
nb_logement_a_renover_commune=pandas.merge(nb_logement_a_renover_commune,geo_communes_renov)



# requet sql afin de visualiser le type de renovation (global/par element) sur la carte:
## requete renovation globale:

requet_sql="""SELECT nom_commune,
                     COUNT(nom_commune) AS numbre_logement
                     FROM logement_info
                     WHERE (classe_consommation_energie='G' OR classe_consommation_energie='F')
                     AND SHON>1000
                     AND annee_construction>1948
                     GROUP BY nom_commune;
                     """
df_type_renovation_globale=pandas.read_sql_query(requet_sql,conn).assign(type_renovation="gobale")
#ajouter les données geo a nb_logement_a_renover_commune dataframe:
geo_communes_renov_g=commun_df[commun_df["nom_commune"].isin(df_type_renovation_globale["nom_commune"])]
df_type_renovation_globale=pandas.merge(df_type_renovation_globale,geo_communes_renov_g)


## requete renovation par element:
requet_sql="""SELECT nom_commune,
                     COUNT(nom_commune) AS numbre_logement
                     FROM logement_info
                     WHERE (classe_consommation_energie='G' OR classe_consommation_energie='F')
                     AND SHON<1000
                     GROUP BY nom_commune;
                     """
df_type_renovation_par_element=pandas.read_sql_query(requet_sql,conn).assign(type_renovation="par_element")
#ajouter les données geo a nb_logement_a_renover_commune dataframe:
geo_communes_renov_e=commun_df[commun_df["nom_commune"].isin(df_type_renovation_par_element["nom_commune"])]
df_type_renovation_par_element=pandas.merge(df_type_renovation_par_element,geo_communes_renov_e)



# les noms de communes zero renovation:
commun_zero_renovation=commun_df[~commun_df["nom_commune"].isin(nb_logement_a_renover_commune["nom_commune"])]   # ~ pour renverser l'effet de isin
commun_zero_renovation["nombre_logement"]=0
nb_logement_vis=pandas.concat([nb_logement_a_renover_commune,commun_zero_renovation])




# requet sql pourcréer la table visualiser sur la dash board:
requet_sql="""SELECT  etat_avancement,
                      dpe.date_etablissement_dpe,
                      consommation_energie,
	              annee_construction,
	              surface_habitable AS SURFACE,
	              libelle AS TYPE_DE_LOGEMENT,
	              adresse.nom_commune AS commune,
	              code_postal AS CODE_POSTAL,
	              numero_rue AS NUMERO_RUE,
	              type_voie AS TYPE_VOIE,
	              nom_rue
                      FROM logement_description
                      LEFT JOIN adresse ON logement_description.id_adresse=adresse.id_adresse
	              LEFT JOIN commune ON adresse.nom_commune=commune.nom_commune
	              LEFT JOIN surface ON logement_description.id_logement=surface.id_logement
	              LEFT JOIN dpe ON logement_description.numero_dpe=dpe.numero_dpe
	              LEFT JOIN type_bâtiment ON logement_description.id_type_bâtiment=type_bâtiment.id_type_bâtiment
                      WHERE classe_consommation_energie='G' OR classe_consommation_energie='F';
            """
deatille_logement = pandas.read_sql_query(requet_sql, conn)


###############################################################################

#2. Creer les graphique: apartir des données analyse et avec pie chart et foluim


## Visualiser le nombre de logement par classe de consomation sur pie chart:

color_discrete_map=("green","#0CD220","#ADFF2F","yellow","#FFD700","#F2820D","red","black")
                    
fig = go.Figure(
    data=[go.Pie(
        labels=nb_logement_class['classe_consommation_energie'],
        values=nb_logement_class['nombre_logement'],
        textinfo='percent+label',
        marker_colors=color_discrete_map,
        insidetextorientation='radial',
        textposition='inside',
        #  make sure that Plotly won't reorder your data while plotting
        sort=False,pull=[0, 0, 0, 0,0,0.2,0.2])])

fig.update_traces(hoverinfo='label+value+percent',
                  textinfo='label+percent',
                  textfont_size=12)
fig.update_layout(
    autosize=False,
    width=400,
    height=400,
    margin=dict(
        l=20,
        r=20,
        b=10,
        t=10), 
    paper_bgcolor="white",
    title_font_family="Times New Roman")

fig.update_layout(title_font_color="red")


##visulalisation des données sur la carte:

geo_donnée=geopandas.GeoDataFrame.from_features(geodonnees)      # recuprer les données geo a partir de MongoDB et les lires en geopandas

### deffinir le centre de map:
map_centre=pandas.DataFrame()
for index, row in geo_donnée.iterrows():
    x=row.geometry.centroid.x
    y=row.geometry.centroid.y
    map_centre=map_centre.append([[x,y]])

longitude_centrale=median(map_centre[0])
latitude_centrale=median(map_centre[1])

### creation la map:

m = folium.Map(location=[latitude_centrale,longitude_centrale], zoom_start=9)

folium.features.Choropleth(geo_data=geodonnees,
    data=nb_logement_a_renover_commune,
    columns=['nom_commune','nombre_logement'],
    key_on='feature.properties.nom_comm',
    fill_color='Spectral',
    fill_opacity=0.7,
    line_opacity=0.3,
    bins=9,
    weight=1,
    legend_name='Nombre de logement a renover par commune',
    dashArray='5, 3',
    highlight=True
    ).add_to(m)

### ajouter tooltip nom_commune, nb logement
for  index,row in nb_logement_vis.iterrows():    
     folium.Marker(location=[row["longitude"],row["latitude"]],icon=folium.DivIcon(),
     tooltip=f"""<b>{row["nom_commune"]}</b><br><br>Nombre de logement: {row["nombre_logement"]}"""         
     ).add_to(m)
 

### ajouter layer type de renovation:

layer1=folium.FeatureGroup(name="renovation globale")
m.add_child(layer1)
for  index,row in df_type_renovation_globale.iterrows():    # ajouter tooltip nom_commune, nb logement
     layer1.add_child(folium.Marker(location=[row["longitude"],row["latitude"]],
                    icon=folium.DivIcon(
                    html=f"""<div style="font-family:Gill Sans Extrabold, sans-serif;color:black";font-weight: bold;font-size="10";>{row["numbre_logement"]}</div>"""),
                    tooltip=f"""<b>{row["nom_commune"]}</b><br><br>Nombre de logement: {row["numbre_logement"]}"""))
                                     

layer2=folium.FeatureGroup(name="renovation par elements")
m.add_child(layer2)
for  index,row in df_type_renovation_par_element.iterrows():    # ajouter tooltip nom_commune, nb logement
     layer2.add_child(folium.Marker(location=[row["longitude"],row["latitude"]],
                     icon=folium.DivIcon(
                     html=f"""<div style="font-family:Gill Sans Extrabold, sans-serif;
                                         color:black";
                                         font-weight: bold;
                                         font-size="10";>{row["numbre_logement"]}</div>"""),
                    tooltip=f"""<b>{row["nom_commune"]}</b><br><br>Nombre de logement: {row["numbre_logement"]}"""))
                                     
folium.LayerControl().add_to(m)
m.save("map.html")

    
deatille_logement2=deatille_logement[deatille_logement['commune'] == 'NANTES']


##############################################################################################################
#3. visualiser les graphique sur le tableau de board avec app.layout

app.layout = html.Div(children=[
                       html.H2(" Trajectoire de réduction l'émission de GES dans le secteur de bâtiment en LOIRE-ATLANTIQUE",
                               style={'color':'black',
                                      'text-shadow': '2px 2px 8px #FF7F50',
                                      'text-align': 'center',
                                      'font-family': 'cursive'}),
                       html.H2("Part de logement à renover par rapport aux nombres totaux:",
                               style={'color':'#DCDCDC',
                                      'text-align': 'center',
                                      'background-color': '#006400',}),
                       html.Div(dcc.Graph(id='fig',figure=fig),
                                style={'display':'inline-block',
                                'box-shadow': '8px 8px 12px #555',
                                'margin-left':100}),
                       html.Img(src=app.get_asset_url('dpe.jpg'),
                                style={'height':'35%',
                                       'width':'35%',
                                       'display':'inline-block',
                                       'margin-left':300,
                                       'margin-Top':0,
                                       'box-shadow': '8px 8px 12px #555'}),                                                      
                       html.Br(),
                       html.Br(),
                       html.H2("Visualisation des nombres de logement à renover et les choix de rénovation par communes en LOIRE-ATLANTIQUE",
                               style={'color':'#DCDCDC',
                                      'background-color': '#006400',
                                      'text-align': 'center'}),
                       html.Iframe(id='map',
                                   srcDoc=open('map.html','r').read(),
                                   width='800',height='600',
                                   ),
                       html.Br(),
                       html.Br(),

                       html.H2("Détailes sur les logements à renover:",
                               style={'color':'#DCDCDC',
                                      'background-color': '#006400',
                                      'text-align': 'center'}),
                       dcc.Dropdown(
                          id='commune_filtre',
                          options=[{'label': nom_commune,
                                    'value':nom_commune} for nom_commune in deatille_logement['commune'].unique()],
                          value='SAINT-NAZAIRE',
                          searchable=True,
                          style={'box-shadow': '0 0 10px #666',
                                 'width': '400px',
                                 'margin': '0'}),
                       html.Br(),
                       dash_table.DataTable(id='table_commune',
                           columns=[{"name": i, "id": i} for i in deatille_logement2.columns],
                           data=deatille_logement2.to_dict('records'),
                           style_cell={'textAlign': 'left',},
                           style_data_conditional=[
                              {
                                 'if': {'row_index': 'odd'},
                                 'backgroundColor': '#FBF2B7'
                              }
                           ],
                          style_header={
                             'backgroundColor': '#FF7F50',
                             'fontWeight': 'bold'
                         },
                         page_size=10,
                         
                         style_table={'height': '300px','overflowY': 'auto'}
                        )
                       ], 
                      )

                           

                      
                      



@app.callback(
    Output("table_commune", "data"),
    Input("commune_filtre", "value"))

def display_table(commune):
    deatille_logement2=deatille_logement[deatille_logement['commune'] == commune]
    data=deatille_logement2.to_dict('records')

    return data





if __name__ == '__main__':
    app.run_server(debug=False)



