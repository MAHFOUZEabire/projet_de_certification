from utils import *
import warnings
warnings.filterwarnings('ignore')

utilisateur = "postgres"
#mot_passe = os.environ.get('pg_psw')
mot_passe="Line2009"
nom_bdd="dpe_logement"
conn=ouvrir_connection(nom_bdd, utilisateur, mot_passe, host='localhost', port=5432)


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
##########################################################
# lire les données en pandas dataframe:

nom_fichier="donnees_demo4"
nom_dossier="data_demo"

donnees_df=lire_donnees_en_df(nom_fichier,nom_dossier)   # Lire des données en pandas df

###################################
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


###################################################



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
insertion_donnees(conn,sql_insertion_info_diagnostique_dpe,df_donnee_nettoyer)

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


requet_sql= """SELECT id_adresse FROM adresse
           """


nb_id_adresse=len(pandas.read_sql_query(requet_sql, conn))    # recuprer l'id adresse de la table adresse

nb_lignes_inserers=len(df_donnee_nettoyer)

id_adresse=[]
id_=nb_id_adresse
for i in range(0,nb_lignes_inserers):
    id_=id_+1
    id_adresse.append(id_)



insertion_donnees(conn,sql_insertion_adresse,df_donnee_nettoyer)
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










df_donnee_nettoyer['id_adresse']=id_adresse          # ajouter id_adresse a df_donnee_nettoyer



insertion_donnees(conn,sql_insertion_Logement_description,df_donnee_nettoyer)
      
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
insertion_donnees(conn,sql_insertion_Surface,df_donnee_nettoyer)

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

insertion_donnees(conn,sql_insertion_presence_verriere,df_donnee_nettoyer)



