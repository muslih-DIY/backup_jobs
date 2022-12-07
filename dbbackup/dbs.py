from .dbwrapers.wrapers import or_wraper,pg_wraper
from .config import conf  

def get_db(name:str):
    if name == 'localpg': 
        return pg_wraper.pg2_base_wrap(conf.postgresdb.connector())
    if hasattr(conf,name) and getattr(conf,name):
        return or_wraper.oracle_base_wrap(getattr(conf,name).connector())