from utils import *



# creation base de donnee DPE_logement

utilisateur = "postgres"
#mot_passe = os.environ.get('pg_psw')
mot_passe="Line2009"
#db_name="dpe_logement"
#db_name="dpe_loire_atlantique"
#create_database(db_name,utilisateur, mot_passe, host='localhost', port=5432)



# Creation tables:


def create_table(conn):

    """Creation des tables
    """
    try:
        cursor = conn.cursor()
        
        # creation de la table Categorie ERP:
        cursor.execute("""CREATE TABLE IF NOT EXISTS categorie_erp(
                      categorie_erp VARCHAR PRIMARY KEY,
                      groupe VARCHAR,
                      code VARCHAR)
                      """)
        # creation de la table type ERP:
        cursor.execute("""CREATE TABLE IF NOT EXISTS type_erp(
                     id_type_erp INTEGER PRIMARY KEY,
                     categorie_type VARCHAR,
                     type_erp_type VARCHAR,
                     categorie_erp VARCHAR,
                     FOREIGN KEY (categorie_erp) REFERENCES categorie_erp(categorie_erp))
                     """)
       
        # creation de la table commune:
        cursor.execute("""CREATE TABLE IF NOT EXISTS commune(
                     nom_commune VARCHAR PRIMARY KEY,
                     arrondissement_Id INTEGER,
                     code_postal VARCHAR
                     )
                     """)
        # creation de la table adresse:
        cursor.execute("""CREATE TABLE IF NOT EXISTS adresse(
                     id_adresse SERIAL PRIMARY KEY,
                     numero_rue VARCHAR,
                     nom_rue VARCHAR,
                     type_voie VARCHAR,
                     nom_commune VARCHAR,
                     escalier VARCHAR,
                     etage VARCHAR,
                     Porte_N VARCHAR,
                     FOREIGN KEY (nom_commune) REFERENCES commune(nom_commune))
                     """)
        # creation de la table Information sur diagnostique DPE:
        cursor.execute("""CREATE TABLE IF NOT EXISTS dpe(
                     numero_dpe VARCHAR PRIMARY KEY,
                     date_de_réception_dpe_par_le_système DATE,
                     date_visite_diagnostiqueur DATE,
                     date_etablissement_dpe DATE,
                     date_arrete_tarifs_energies DATE,
                     commentaires_ameliorations_recommandations TEXT,
                     explication_personnalisee TEXT
                     )
                     """)
         # creation de la table type_bâtiment:
        cursor.execute("""CREATE TABLE IF NOT EXISTS type_bâtiment(
                     id_type_bâtiment INTEGER PRIMARY KEY,
                     code VARCHAR,
                     discription VARCHAR,
                     libelle VARCHAR,
                     secteur_activite VARCHAR,
                     categorie_erp VARCHAR,
                     FOREIGN KEY (categorie_erp) REFERENCES categorie_erp(categorie_erp))
                     """)
        # creation de la table Logement description:
        cursor.execute("""CREATE TABLE IF NOT EXISTS logement_description(
                     id_logement INTEGER PRIMARY KEY,
                     id_type_bâtiment INTEGER,
                     numero_dpe VARCHAR,
                     classement_ges VARCHAR,
                     classe_consommation_energie VARCHAR,
                     annee_construction INTEGER,
                     consommation_energie FLOAT,
                     estimation_ges FLOAT,
                     nombre_niveaux INTEGER,
                     nombre_entrees_avec_sas INTEGER,
                     nombre_entrees_sans_sas INTEGER,
                     en_souterrain BOOLEAN,
                     en_surface BOOLEAN,
                     nombre_boutiques INTEGER,
                     numero_lot VARCHAR,
                     id_adresse INTEGER,
                     etat_avancement TEXT,
                     FOREIGN KEY (Id_type_bâtiment) REFERENCES type_bâtiment(Id_type_bâtiment),
                     FOREIGN KEY (id_adresse) REFERENCES adresse(id_adresse),
                     FOREIGN KEY (numero_dpe) REFERENCES dpe(numero_dpe)
                     )
                     """)
        # creation de la table vitrage:
        cursor.execute("""CREATE TABLE IF NOT EXISTS vitrage(
                     id_presence_verriere SERIAL PRIMARY KEY,
                     presence_verriere BOOLEAN,
                     surface_verriere FLOAT,
                     type_vitrage_verriere VARCHAR,
                     id_logement INTEGER,
                     FOREIGN KEY (Id_logement) REFERENCES logement_description(Id_logement)
                     )
                     """)
        # creation de la table Surface:
        cursor.execute("""CREATE TABLE IF NOT EXISTS surface(
                     id_Surface SERIAL PRIMARY KEY,
                     id_logement INTEGER,
                     surface_habitable FLOAT,
                     surface_thermique_lot FLOAT,
                     Surface_commerciale FLOAT,
                     surface_thermique_parties_communes FLOAT,
                     surface_utile FLOAT,
                     shon FLOAT,
                     surface_baies_orientees_sud FLOAT,
                     surface_baies_orientees_nord FLOAT,
                     surface_baies_orientees_est_ouest FLOAT,
                     surface_planchers_hauts_deperditifs FLOAT,
                     surface_planchers_bas_deperditifs FLOAT,
                     surface_parois_verticales_opaques_deperditives FLOAT,
                     FOREIGN KEY (Id_logement) REFERENCES logement_description(Id_logement)
                     )
                     """)
       

        conn.commit()

    except psycopg2.Error as e:
        print("Erreur lors de la création de la table")
        print(e)
        return
    cursor.close()
    conn.close()
    print("Les tables ont été créer avec succès")
    return True


# create la base de données SQL:

#conn=ouvrir_connection(db_name, utilisateur, mot_passe, host='localhost', port=5432)

#create_table(conn)
    

# create la base de données NoSQL:
insert_collectionMongoDB('geo_communes','MOSELLE_geo','MOSELLE')


