import pandas
import geopandas
import os
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import requests
import numpy as np
import pymongo
from pymongo import MongoClient,GEOSPHERE
import shapely.geometry
from statistics import *
import folium
import plotly.express as px
import plotly.graph_objects as go
import re
import json

def create_database(db_name,utilisateur, mot_passe, host='localhost', port=5432):
    """Creation base de donne postgresql
    """
    try:
        conn = psycopg2.connect(user=utilisateur, password=mot_passe, host=host, port=5432)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute("SELECT datname FROM pg_database")
        nom_db_exist = cursor.fetchall()
        if (f"{db_name}",) in nom_db_exist:
            print("La base de données existe")
        else:
            cursor.execute(f"CREATE DATABASE {db_name}")
            print("La base de données a été crée avec succès")

    except psycopg2.Error as e:
        print("Erreur lors de la connection et la creation de la base de données")
        print(e)
        return None
    cursor.close()
    conn.close()
    return True




def ouvrir_connection(nom_bdd, utilisateur, mot_passe, host='localhost', port=5432):
    """Se connecter à une source de données PostgreSQL
    """
    try:
        conn = psycopg2.connect(dbname=nom_bdd, user=utilisateur, password=mot_passe, host=host, port=5432)
    except psycopg2.Error as e:
        print("Erreur lors de la connection à la base de données")
        print(e)
        return None
    conn.set_session(autocommit=True)
    return conn


def insert_collectionMongoDB(db_name,collection_name,nom_departement):
    """ En mongodB, la base de donnees se créer lors de la première sauvegarde
        de la valeur dans la collection définie
        nom_departement: pour récuperer des donnees geojson d'un departement apartir d'api
    """
    try:
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")                 # connecter mongodb
        bd_nosql=myclient[db_name]                                                   # connecter au bas de donnees
        collection_list = bd_nosql.list_collection_names()                           # verifie l'existance de la collection avant de créer
        if collection_name in collection_list:
            print("La collection existe")
        else:
            geojson_donnees='https://public.opendatasoft.com/explore/dataset/correspondance-code-insee-code-postal'+\
                             f'/download/?format=geojson&refine.nom_dept={nom_departement}&timezone=Europe/Berlin&lang=fr'
            reponse=requests.get(geojson_donnees)
            obj = reponse.json()
            mycol = bd_nosql[collection_name]
            mycol.insert_one(obj)
            print('Les données est insere avec succée')
    except:
        print('Erreur lors de insertion des données')
    return True



def import_donnees(code_departement):
    """ récupere les données a partire de API koumoul.com et l'enregistre en local dans le fichier data_brute
    """
    try:
        one_directory_up_path =os.path.abspath(os.path.join(os.path.dirname( __file__ ),os.pardir))             #obtenir le chemin du répertoire actuel sous lequel le fichier .py est exécuté,os.pardir: determine le répertoire parant 
        data_path = os.path.join(one_directory_up_path, 'data','data_brute')                                    # determine le chemin pour enregisre les donnees, os.path.join: construit le nom de chemin à partir d'un ou de plusieurs noms de chemins partiels 
        csv_file = open(os.path.join(data_path,f'{code_departement}.csv'), 'wb')                     
        url=f'https://koumoul.com/s/data-fair/api/v1/datasets/dpe-{code_departement}/data-files/dpe-{code_departement}.csv'
        req = requests.get(url)
        url_content = req.content
        csv_file.write(url_content)
        csv_file.close()
    except import_donnees.Error as e:
        print("Erreur lors de l'importation des données")
        print(e)        
        return
    print("Les données importées et sauvegardés  avec succès")                    


###########################################################################################################


def lire_donnees_en_df(nom_fichier,nom_dossier):
    """lire les données csv en forme de pandas dataframe
    """
    one_directory_up_path =os.path.abspath(os.path.join(os.path.dirname( __file__ ),os.pardir))
    data_path = os.path.join(one_directory_up_path, 'data',f'{nom_dossier}')
    donnees_csv=pandas.read_csv(os.path.join(data_path,f'{nom_fichier}.csv'),sep=",",low_memory=False)     # lecture les donnees csv en dataframe 
    return donnees_csv




def lire_geo_collection_MongoDb(nom_db,nom_collection):
    """cette fonction va lire une geo collection apartir de MongoDB
       il va supprimer l'obijetId creer par MongoDB lors que l'insertion
       de la collection. il va retourner une dictionnaire
    """
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client[f'{nom_db}']
    col = db[f'{nom_collection}']
    geodonnees= col.find_one()
    geodonnees.pop('_id', None)
    return(geodonnees)



#######################################################
#Nettoyage des donnees:
# les fonction à appeler successivement pour nettoyer les donnees:

def nettoyage_error(donnees_df):
    """remplacer la @ par la lettre A dans le colonne numero_dpe
       remplase zero value en categorie_erp par NoERP
    """
    try:
        donnees_df['numero_dpe'] = donnees_df['numero_dpe'].str.replace('@','A')
        donnees_df=donnees_df.fillna(0)
        donnees_df['tr012_categorie_erp_categorie'] = donnees_df['tr012_categorie_erp_categorie'].replace(0,'NoERP')
        
    except:
          print("Erreur lors le corrige des données")        

    print("Les error sont corriges avec succès")
    return donnees_df


def nettoyage_drop_duplicates(donnees_df):
    """supprimer les duplicates selon le numero de dpe
    """
    try:
        donnees_df=donnees_df.drop_duplicates(subset =["numero_dpe"],
                                                   keep = 'first')
    except:
          print("Erreur lors que la suppression des duplicates")        

    print("Les duplicates sont supprimer avec succès")
    return donnees_df


def nettoyage_suprim_0value(donnees_df):
    """ supprimer les lignes lors que la consomation d'energie est
        zéro ou non fournie
    """
    try:
         supprimer_consommation_energie0=donnees_df.drop(donnees_df[donnees_df['consommation_energie']==0.00].index)          # supprimer les lignes lorsque la consomation d'energie egale zero

         donnees_df=supprimer_consommation_energie0.dropna(subset = ["consommation_energie","estimation_ges"])                # supprimer les lignes lorsque la consomation d'energie et l'estimation ges non fournis
    except:
          print("Erreur lors la suppression des zeros values")        
          
    print("Les zeros values sont supprimer avec succès")
    return donnees_df


def nettoyage_suprim_col(donnees_df,colonne_supprimer):
    """supprimer les colonnes qui n'sont pas utiliser
       pour l'analyse
    """
    try:
        donnees_df=donnees_df.drop(columns=colonne_supprimer,errors='ignore').fillna(0)
    except:
          print("Erreur lors la suppression des colonnes")        
          
    print("Les colonnes sont supprimes avec succès")
    return donnees_df


def nettoyage_to_numeric(donnees_df,donnees_numeric):
    """transformer les données des colonnes définie dans le list
       donnée_numeric to numeric
    """
    try:
        donnees_df[donnees_numeric]=donnees_df[donnees_numeric].apply(pandas.to_numeric,errors='coerce')    #invalid parsing will be set as NaN, retourn type float meme en passant downcast=integer
        donnees_df[donnees_numeric]=donnees_df[donnees_numeric].fillna(0)                                   # remplacer Nan par 0 afin de transformer en int apres
        donnees_df[donnees_numeric]=donnees_df[donnees_numeric].applymap(np.int64)                          # int ne s'applique pas lors que il y a des NaA
    except:
          print("Erreur lors la transformation en numeric")        
          
    print("Les données sont transforme en numeric avec succès")
    return donnees_df


def nettoyage_to_boolean(donnees_df,donnees_boolean):
    """transformer les données des colonnes définie dans le list
       donnees_boolean to true, false
       dans le fichier csv importe les données boolean est
       presenté en 0,1 mais le boolean en postgresql est true, fals
    """
    try:
        donnees_df[donnees_boolean]=donnees_df[donnees_boolean].replace([0,1],["f","t"])
    except:
          print("Erreur lors la transformation en boolean")        
          
    print("Les données sont transforme en boolean avec succès")
    return donnees_df


def nettoyage_suprim_commune(donnees_df,df_commune):
    """supprimer les communes n'appartien pas au
       departement LOIRE-ATLANTIQUE
    """
    try:
        donnees_df['commune']=donnees_df['commune'].str.upper()
        donnees_df=donnees_df[donnees_df['commune'].isin(df_commune['nom_commune'])]
    except:
          print("Erreur lors la supprsion des communes")        
          
    print("Les commune n'appartien pas au departement sont supprime avec succès")
    return donnees_df


def nettoyage_adress(donnees_df):
    """ les donnees des adress sont concatene pour les plus part des lignes
        dans la colonne nom_rue, cette fonction va separer ces dpnnees en
        numero_rue, nom_rue, type_voie
    """
    donnees_df[["numero_rue","nom_rue","type_voie"]]=donnees_df[["numero_rue","nom_rue","type_voie"]].astype(str)
    donnees_df['Full_adress']=np.where(donnees_df['type_voie']!="0",donnees_df['type_voie']+" "+donnees_df['nom_rue'],donnees_df['nom_rue'])
    donnees_df['Full_adress']=np.where(donnees_df['numero_rue']!="0",donnees_df['numero_rue']+" "+donnees_df['Full_adress'],donnees_df['Full_adress'])
    donnees_df['Full_adress']=donnees_df['Full_adress'].str.upper()
    # extraie les nombre de la colonne full adress a l'aide de findall et les mettre dans le numero du rue
    donnees_df['numero_rue']=donnees_df['Full_adress'].str.findall(r'-?\d+\.?\d*').apply(lambda x : ', '.join(x))
    # definir un pattern les diffferent type de voie:
    pattern=r'RUE|CHEMIN|ROUTE|AVENUE|CHAUSSEE|ALLEE|PASSAGE|BD|IMPASSE|RESIDENCE|BOULEVARD|BIS RUE|TER RUE|LOTISSEMENT|QUAI|SQUARE|LOT|AV'
    donnees_df['type_voie']=donnees_df['Full_adress'].str.findall(pattern,flags=re.IGNORECASE).apply(lambda x : ', '.join(x))    # apply lambda pour transformer la list de str
    # a l'aide de replace et re on va supprimer de la nom de rue le numero, le type et les articl le les...
    donnees_df["nom_rue"]=donnees_df['Full_adress'].str.replace(pattern,'')
    donnees_df["nom_rue"]=donnees_df["nom_rue"].str.replace(r'-?\d+\.?\d*','')
    donnees_df["nom_rue"]=donnees_df["nom_rue"].str.replace(r'DES|LES|DE|LE|,','')             
    print("La separation des donnees de la colonne adress est fait avec succès")
    return donnees_df

###################################################


def insertion_donnees(conn, sql_insertion_table, donnees_df):
    """insertion des donnees
    """
    try:
        cursor = conn.cursor()
        for index,row in donnees_df.iterrows():
            cursor.execute(sql_insertion_table, row)
        conn.commit()
    except psycopg2.Error as e:
        print("Erreur lors de l'insertion des données")
        print(e)
        return
    print("Les données ont été insérées avec succès")
    


def commune(geodonnees):
    """geodonnees a recuprer a partitr de MongoDB en utilisant la fonction
       lire_geo_collection_MongoDb
    """
    x=len(geodonnees['features'])
    comm_df=pandas.DataFrame()
    for i in range(0,x-1):
        commune_donnees=[
            geodonnees['features'][i]['properties']['nom_comm'],
            geodonnees['features'][i]['properties']['insee_com'],
            geodonnees['features'][i]['properties']['postal_code'],
            geodonnees['features'][i]['properties']['code_arr'],
            geodonnees['features'][i]['properties']['geo_point_2d'][0],
            geodonnees['features'][i]['properties']['geo_point_2d'][1]]
        comm_df=comm_df.append([commune_donnees],ignore_index=True)
    comm_df.columns=["nom_commune","code_insee","code_postal","code_arrondissement","longitude","latitude"]
    return(comm_df)




# Fonction permettant d'exécuter un requête SQL sur une BDD définie par sa connexion conn
def executer_requete(requete_sql, conn):
    try:
        cursor = conn.cursor()
        cursor.execute(requete_sql)
        conn.commit()
    except psycopg2.Error as e:
        print("Erreur lors de l'execution de la requête")
        print(e)
        return
    cursor.close()



def sauvgarder_donnees_en_csv(donnees_df,nom_fichier,nom_dossier):
    """sauvgarder les donnees netoyer en local
    """
    try:
        one_directory_up_path =os.path.abspath(os.path.join(os.path.dirname( __file__ ),os.pardir))

        data_path = os.path.join(one_directory_up_path, 'data',f'{nom_dossier}')

        donnees_df.to_csv (os.path.join(data_path,f'{nom_fichier}.csv'), index = False, header=True)          # enregistre les données nettoyer en format csv
    except:
          print("Erreur lors le sauvgardage des données")        
          return
    print("Les données sont sauvgardes avec succès")


def get_file_metadata(nom_dossier, filename, metadata_filename,metadata):
    """
    filename: nome de fichier pour lequele on va récuperer le metadonnées,il doit contenir une extension, i.e. "PID manual.pdf"
    nom_dossier: nom de dossier ou se trove le fichier pour lequele on va récuperer le metadonnées
    metadata_filename: nom de fichier metadonnée a retourné
    cette fonction returne une dictionnaire contien les metadonnées
    """
    try:
        one_directory_up_path =os.path.abspath(os.path.join(os.path.dirname( __file__ ),os.pardir))
        data_path = os.path.join(one_directory_up_path, 'data',f'{nom_dossier}')
        sh = win32com.client.gencache.EnsureDispatch('Shell.Application', 0)
        ns = sh.NameSpace(data_path)
        # Enumeration is necessary because ns.GetDetailsOf only accepts an integer as 2nd argument
        file_metadata = dict()
        item = ns.ParseName(str(filename))
        for ind, attribute in enumerate(metadata):
            attr_value = ns.GetDetailsOf(item, ind)
            if attr_value:
                file_metadata[attribute] = attr_value
        data_path_save = os.path.join(one_directory_up_path, 'data','metadonnees')
        with open(os.path.join(data_path_save,f'{metadata_filename}.json'),'w') as fp:
           json.dump(file_metadata, fp)
    except:
        print("error lors que la récuperation des metadonnées")
    print("les metadonnées sont récuperer avec succées")
    return True



###########################################################
def generate_table(dataframe, max_rows):
    return html.Table([
        html.Thead(
            html.Tr([html.Th(col) for col in dataframe.columns])
        ),
        html.Tbody([
            html.Tr([
                html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
            ]) for i in range(min(len(dataframe), max_rows))
        ])
    ])
