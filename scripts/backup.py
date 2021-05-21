import os
import time
import datetime
from utils import *



def backup_db(bd_nom):
    one_directory_up_path =os.path.abspath(os.path.join(os.path.dirname( __file__ ),os.pardir))
    BACKUP_PATH = os.path.join(one_directory_up_path, 'data','backup_bd')

    # Getting current DateTime to create the separate backup folder like "20180817-123433"
    DATETIME = time.strftime('%Y%m%d-%H%M%S')
    TODAYBACKUPPATH =os.path.join(BACKUP_PATH,DATETIME)
    # Checking if backup folder already exists or not. If not exists will create it
    try:
        os.stat(TODAYBACKUPPATH)
    except:
        x=os.mkdir(TODAYBACKUPPATH)
        print("le dossier today backup est crée")

    os.chdir("C:\\Program Files\\PostgreSQL\\13\\bin")

    dumpcmd=f"pg_dump -h localhost -U postgres -f {os.path.join(BACKUP_PATH,DATETIME,'backup.sql')} {bd_nom} "
    os.system(dumpcmd)
    return(True)

#test:
backup_db("dpe_logement")








#psql -U users -W dpe_batiment

#pour restorer:
#psql -h localhost -U postgres -f C:\\Users\\Somplon\\Desktop\\projet_certification\\data\\backup_bd\\20210502-105359\\backup.sql dpe_batiment

