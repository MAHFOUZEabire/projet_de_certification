from utils import *


"""
Dans ce fichier, nous allons:
1- importer des donnees en forma csv a partie d'api et les suavgarder en local dans le dossier data_brut
2- Nettoyer les donnees et les sauvgarder en local dans le dossier data_nettoyer
3- inserer les données dans la base de données dpe_logement
4- Créer un repertoir de metadonnees pour les fichiers CSV et la base de donnees
"""



#################
# 1- importer des donnees:
utilisateur = "postgres"
#mot_passe = os.environ.get('pg_psw')
mot_passe="Line2009"
nom_bdd="dpe_logement"


code_departement="44"
nom_departement="LOIRE-ATLANTIQUE"
#import_donnees(code_departement)               # importer les données a partir de API et sauvgarder en locale


################################################
# 2- Nettoyage des donnees:

nom_fichier="44"
nom_dossier="data_brute"

#donnees_df=lire_donnees_en_df(nom_fichier,nom_dossier)   # Lire des données en pandas df

colonne_supprimer=["usr_diagnostiqueur_id","usr_logiciel_id","tr001_modele_dpe_id","nom_methode_dpe",
                  "version_methode_dpe","nom_methode_etude_thermique","version_methode_etude_thermique",
                  "tv016_departement_id","adresse_proprietaire","adresse_proprietaire_installations_communes",
                  "nombre_circulations_verticales","est_efface","latitude","longitude","geo_score","geo_type",
                  "geo_adresse","geo_id","geo_id","geo_l4","geo_l5","tr001_modele_dpe_type_id",
                  "tr001_modele_dpe_code","tr001_modele_dpe_modele","tr001_modele_dpe_type_libelle",
                  "tr001_modele_dpe_description","tr001_modele_dpe_fichier_vierge","tr001_modele_dpe_est_efface",
                   "tr001_modele_dpe_type","tr001_modele_dpe_type_ordre","tr013_type_erp_est_efface",
                  "tv016_departement_id","tv016_departement_code","tv017_zone_hiver_id","tv017_zone_hiver_code",
                  "tv017_zone_hiver_t_ext_moyen","tv017_zone_hiver_peta_cw","tv017_zone_hiver_dh14","tv017_zone_hiver_prs1",
                  "tv018_zone_ete_id","tv018_zone_ete_code","tv018_zone_ete_sclim_inf_150","tv018_zone_ete_sclim_sup_150",
                  "tv018_zone_ete_rclim_autres_etages","tv018_zone_ete_rclim_dernier_etage","tv016_departement_departement",
                  "tv016_departement_altmin","tv016_departement_altmax","tv016_departement_nref","tv016_departement_dhref",
                  "tv016_departement_pref","tv016_departement_c2","tv016_departement_c3","tv016_departement_c4",
                   "tv016_departement_t_ext_basse","tv016_departement_e","tv016_departement_fecs_solaire_m_i",
                   "tv016_departement_fecs_recente_i_c","tv016_departement_fecs_ancienne_m_i","tv016_departement_fecs_recente_m_i",
                  "tv016_departement_fch","tv016_departement_fecs_ancienne_i_c","tv016_departement_fecs_recente_i_c"]

donnees_boolean=["dpe_vierge","en_souterrain","en_surface","presence_verriere"]
donnees_numeric=["arrondissement","numero_rue","batiment","code_postal","code_insee_commune","code_insee_commune_actualise",
                "tr013_type_erp_code"]


donnees_df=nettoyage_error(donnees_df)

donnees_df=nettoyage_drop_duplicates(donnees_df)

donnees_df=nettoyage_suprim_0value(donnees_df)

donnees_df=nettoyage_suprim_col(donnees_df,colonne_supprimer)

donnees_df=nettoyage_to_numeric(donnees_df,donnees_numeric)

donnees_df=nettoyage_to_boolean(donnees_df,donnees_boolean)

df_commune=commune(nom_departement)

donnees_df=nettoyage_suprim_commune(donnees_df,df_commune)

donnees_df=nettoyage_adress(donnees_df)

nom_fichier='data_nettoyee_44'
nom_dossier='data_nettoyee'

#sauvgarder_donnees_en_csv(donnees_df,nom_fichier,nom_dossier)

###############################################################################

# 3- inserer les données:

conn=ouvrir_connection(nom_bdd, utilisateur, mot_passe, host='localhost', port=5432)

nom_fichier='data_nettoyee_44'
nom_dossier='data_nettoyee'

# lire les données nettoyer a partir du dossier data_nettoyer en pandas data frame
df_donnee_nettoyer=lire_donnees_en_df(nom_fichier,nom_dossier)        


"""
donnee demo: exrtai des données afin de faire le test
donnee_demo= df_donnee_nettoyer.iloc[100:120,:]
sauvgarder_donnees_en_csv(donnee_demo,'donnees_demo','data_demo')
"""

df_donnee_nettoyer=df_donnee_nettoyer.drop(df_donnee_nettoyer.index[100:120],0)


# insertion des données dans la table categorie erp:
# a partir de dictionnaire des donnéées presente sur le sit ADEME ou nous avons importer les données
# nous avons crééer le df categorie erp:
data_erp = {'tr012_categorie_erp_categorie':['1ère Catégorie','2ème Catégorie' ,'3ème Catégorie','4ème Catégorie','5ème Catégorie','NoERP'],
            'tr012_categorie_erp_groupe':['1er Groupe', '1er Groupe','1er Groupe','1er Groupe','2ème Groupe','0'],
            'tr012_categorie_erp_code':['TR012_001','TR012_002','TR012_003','TR012_004','TR012_005','0']
        }

categorie_erp= pandas.DataFrame(data_erp, columns = ['tr012_categorie_erp_categorie','tr012_categorie_erp_groupe','tr012_categorie_erp_code'])

                           
sql_insertion_categorie_erp="""INSERT INTO categorie_erp
                            (categorie_erp,
                            groupe,code)                     
                            VALUES (%(tr012_categorie_erp_categorie)s,
                            %(tr012_categorie_erp_groupe)s,
                            %(tr012_categorie_erp_code)s);
                            """
#insertion_donnees(conn,sql_insertion_categorie_erp,categorie_erp)


# insertion les donnees dans la table type ERP:
# cette table est statique, contient cinq type erp, pour faciliter l'insertion
# j'ai extrait ces donnes en utilisant pandas df et dropduplicate:

def type_erp(df_donnee_nettoyer):
    type_erp=df_donnee_nettoyer[["tr013_type_erp_categorie_id","tr013_type_erp_code","tr013_type_erp_categorie","tr013_type_erp_type"]]
    type_erp=type_erp.drop_duplicates(["tr013_type_erp_categorie_id"])
    type_erp=type_erp.sort_values(by=['tr013_type_erp_categorie_id'])
    type_erp=type_erp.assign(categorie_erp='5ème Catégorie')
    return(type_erp)



sql_insertion_type_erp="""INSERT INTO type_erp
                       (id_type_erp,
                       categorie_type,
                       type_erp_type,
                       categorie_erp)
                       VALUES (
                       %(tr013_type_erp_categorie_id)s,
                       %(tr013_type_erp_categorie)s,
                       %(tr013_type_erp_type)s,
                       %(categorie_erp)s);
                       """

#type_erp=type_erp(df_donnee_nettoyer)
#insertion_donnees(conn,sql_insertion_type_erp,type_erp)



# insertion les donnees dans la table type_bâtiment:

sql_insertion_type_bâtiment="""INSERT INTO type_bâtiment
                            (id_type_bâtiment,
                            code,
                            discription,
                            libelle,
                            secteur_activite,
                            categorie_erp)
                            VALUES (
                            %(tr002_type_batiment_id)s,
                            %(tr002_type_batiment_code)s,
                            %(tr002_type_batiment_description)s,
                            %(tr002_type_batiment_libelle)s,
                            %(secteur_activite)s,
                            %(tr012_categorie_erp_categorie)s);
                            """

# type de batiment est une table statique contient quelque lignes,
# le plus facil est d'extrait ses ligne avec pandas et dropduplicate:

def type_batiment(df_donnee_nettoyer):
    type_batiment=df_donnee_nettoyer[["tr002_type_batiment_id","tr013_type_erp_code","tr002_type_batiment_code",
                                      "tr002_type_batiment_description","tr002_type_batiment_libelle",
                                      "secteur_activite","tr002_type_batiment_ordre","tr002_type_batiment_simulateur",
                                      "tr012_categorie_erp_categorie"]]
    type_batiment=type_batiment.drop_duplicates(["tr002_type_batiment_id"])
    type_batiment=type_batiment.sort_values(by=['tr002_type_batiment_id'])
    return(type_batiment)


#type_batiment=type_batiment(df_donnee_nettoyer)
#insertion_donnees(conn,sql_insertion_type_bâtiment,type_batiment)




# insertion les donnees dans la table info_diagnostique_dpe:

sql_insertion_info_diagnostique_dpe="""INSERT INTO dpe
                                    (numero_dpe,
                                    date_de_réception_dpe_par_le_système,
                                    date_visite_diagnostiqueur,
                                    date_etablissement_dpe,
                                    date_arrete_tarifs_energies,
                                    commentaires_ameliorations_recommandations,
                                    explication_personnalisee)
                                    VALUES (
                                    %(numero_dpe)s,
                                    %(date_reception_dpe)s,
                                    %(date_visite_diagnostiqueur)s,
                                    %(date_etablissement_dpe)s,
                                    %(date_arrete_tarifs_energies)s,
                                    %(commentaires_ameliorations_recommandations)s,
                                    %(explication_personnalisee)s);
                                    """

#insertion_donnees(conn,sql_insertion_info_diagnostique_dpe,df_donnee_nettoyer)


# insertion les donnees dans la table commune:

sql_insertion_commune="""INSERT INTO commune
                      (nom_commune,
                      arrondissement_Id,
                      code_postal
                      )
                      VALUES (
                      %(nom_commune)s,
                      %(code_arrondissement)s,
                      %(code_postal)s
                      );
                      """
nom_departement='LOIRE-ATLANTIQUE'
#commune_df=commune(nom_departement)

#insertion_donnees(conn,sql_insertion_commune,commune_df)

# insertion les donnees dans la adresse:

sql_insertion_adresse="""INSERT INTO adresse
                      (numero_rue,
                      nom_rue,
                      type_voie,
                      nom_commune,
                      escalier,
                      etage,
                      Porte_N)
                      VALUES (
                      %(numero_rue)s,
                      %(nom_rue)s,
                      %(type_voie)s,
                      %(commune)s,
                      %(escalier)s,
                      %(etage)s,
                      %(porte)s);
                      """

#insertion_donnees(conn,sql_insertion_adresse,df_donnee_nettoyer)


# insertion les donnees dans la table logement:

sql_insertion_Logement_description="""INSERT INTO logement_description  
                                   (Id_logement,
                                   id_type_bâtiment,
                                   numero_dpe,
                                   classement_ges,
                                   classe_consommation_energie,
                                   annee_construction,
                                   consommation_energie,
                                   estimation_ges,
                                   nombre_niveaux,
                                   nombre_entrees_avec_sas,
                                   nombre_entrees_sans_sas,
                                   en_souterrain,
                                   en_surface,
                                   nombre_boutiques,
                                   numero_lot,
                                   id_adresse,
                                   etat_avancement)
                                   VALUES (
                                   %(id)s,
                                   %(tr002_type_batiment_id)s,
                                   %(numero_dpe)s,
                                   %(classe_estimation_ges)s,
                                   %(classe_consommation_energie)s,
                                   %(annee_construction)s,
                                   %(consommation_energie)s,
                                   %(estimation_ges)s,
                                   %(nombre_niveaux)s,
                                   %(nombre_entrees_avec_sas)s,
                                   %(nombre_entrees_sans_sas)s,
                                   %(en_souterrain)s,
                                   %(en_surface)s,
                                   %(nombre_boutiques)s,
                                   %(numero_lot)s,
                                   %(id_adresse)s,
                                   %(etat_avancement)s);
                                   """


requet_sql=""" SELECT id_adresse FROM adresse
           """

id_adresse=pandas.read_sql_query(requet_sql, conn)    # recuprer l'id adresse de la table adresse
id_adresse.index = df_donnee_nettoyer.index           # definir le même index au deux df avant de fusione

df_donnee_nettoyer['id_adresse']=id_adresse           # ajouter id_adresse a df_donnee_nettoyer


#insertion_donnees(conn,sql_insertion_Logement_description,df_donnee_nettoyer)
      
# insertion les donnees dans la table Surface:

sql_insertion_Surface="""INSERT INTO surface
                      (id_logement,
                      surface_habitable,
                      surface_thermique_lot,
                      surface_commerciale,
                      surface_thermique_parties_communes,
                      surface_utile,
                      shon,
                      surface_baies_orientees_sud,
                      surface_baies_orientees_nord,
                      surface_baies_orientees_est_ouest,
                      surface_planchers_hauts_deperditifs,
                      surface_planchers_bas_deperditifs,
                      surface_parois_verticales_opaques_deperditives)
                      VALUES (
                      %(id)s,
                      %(surface_habitable)s,
                      %(surface_thermique_lot)s,
                      %(surface_commerciale_contractuelle)s,
                      %(surface_thermique_parties_communes)s,
                      %(surface_utile)s,%(shon)s,
                      %(surface_baies_orientees_sud)s,
                      %(surface_baies_orientees_nord)s,
                      %(surface_baies_orientees_est_ouest)s,
                      %(surface_planchers_hauts_deperditifs)s,
                      %(surface_planchers_bas_deperditifs)s,
                      %(surface_parois_verticales_opaques_deperditives)s);
                      """
#insertion_donnees(conn,sql_insertion_Surface,df_donnee_nettoyer)

# insertion les donnees dans la table presence_verrière:

sql_insertion_presence_verriere="""INSERT INTO vitrage
                                (presence_verriere,
                                surface_verriere,
                                type_vitrage_verriere,
                                Id_logement)
                                VALUES (
                                %(presence_verriere)s,
                                %(surface_verriere)s,
                                %(type_vitrage_verriere)s,
                                %(id)s);
                                """

#insertion_donnees(conn,sql_insertion_presence_verriere,df_donnee_nettoyer)

#########################################################################################################

# 4- Créer un repertoir de metadonnees:

metadata = ['Name', 'Size', 'Item type', 'Date modified', 'Date created']

# 1- metadonnées du fichier csv données brute:
get_file_metadata('data_brute','44.csv','metadata_donnee_brute',metadata)

# 2- metadonnées du fichier csv données nettoyer:
get_file_metadata('data_nettoyee','data_nettoyee_44.csv','metadata_donnee_nettoyer',metadata)

# 3- metadonnées du fichier geojson, données geographique:
#get_file_metadata('data_brute','correspondance-code-insee-code-postal.geojson','metadata_donnee_geographique',metadata)

# 4- metadonnée base de données:
def metadata_bd(conn,info_schema):
    curs = conn.cursor()
    list_info_schema=[]
    curs.execute(f"SELECT * FROM information_schema.{info_schema};")
    for i in curs.fetchall():
        list_info_schema.append(i)
    
    return(list_info_schema)

schema_infos=["information_schema_catalog_name","administrable_role_authorizations","applicable_roles",
              "attributes","character_sets","collations","collation_character_set_applicability",
              "foreign_data_wrapper_options","foreign_data_wrappers","foreign_server_options",
              "sql_features","sql_implementation_info","sql_parts","sql_sizing",
              "foreign_servers","foreign_table_options",
              "foreign_tables","user_mapping_options","user_mappings","parameters","referential_constraints",
              "routines","sequences","table_constraints","tables","triggers","user_defined_types","views",
              "transforms","column_privileges","role_column_grants","role_routine_grants","role_table_grants",
              "role_udt_grants","role_usage_grants","routine_privileges","table_privileges","udt_privileges",
              "usage_privileges","data_type_privileges","enabled_roles","check_constraint_routine_usage","column_domain_usage",
              "column_udt_usage","constraint_column_usage","constraint_table_usage","domain_udt_usage","key_column_usage","view_column_usage",
              "view_routine_usage","view_table_usage","columns","triggered_update_columns","column_options","domain_constraints",
              "domains","element_types","schemata"]
metadonnees_bd={}
for x in schema_infos:
    metadonnees_bd[x] =metadata_bd(conn,x)
    
one_directory_up_path =os.path.abspath(os.path.join(os.path.dirname( __file__ ),os.pardir))

data_path = os.path.join(one_directory_up_path, 'data','metadonnees')

with open(os.path.join(data_path,'metadonnees_bd.json'),'w') as fp:
    json.dump(metadonnees_bd, fp)

############################################################################################

nom_fichier="donnees_demo2"
nom_dossier="data_demo"

donnees_df=lire_donnees_en_df(nom_fichier,nom_dossier)   # Lire des données en pandas df

# Nettoyages des donées
df_donnee_nettoyer=nettoyage_error(donnees_df)

df_donnee_nettoyer=nettoyage_drop_duplicates(df_donnee_nettoyer)

df_donnee_nettoyer=nettoyage_suprim_0value(df_donnee_nettoyer)

df_donnee_nettoyer=nettoyage_suprim_col(df_donnee_nettoyer,colonne_supprimer)

df_donnee_nettoyer=nettoyage_to_numeric(df_donnee_nettoyer,donnees_numeric)

df_donnee_nettoyer=nettoyage_to_boolean(df_donnee_nettoyer,donnees_boolean)

geodonnees=lire_geo_collection_MongoDb("geo_communes","loire_atlantique")

df_commune=commune(geodonnees)

df_donnee_nettoyer=nettoyage_suprim_commune(df_donnee_nettoyer,df_commune)

df_donnee_nettoyer=nettoyage_adress(df_donnee_nettoyer)

#insertion des données

#insertion_donnees(conn,sql_insertion_info_diagnostique_dpe,df_donnee_nettoyer)

insertion_donnees(conn,sql_insertion_adresse,df_donnee_nettoyer)
insertion_donnees(conn,sql_insertion_Surface,df_donnee_nettoyer)
insertion_donnees(conn,sql_insertion_presence_verriere,df_donnee_nettoyer)


