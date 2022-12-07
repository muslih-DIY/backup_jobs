from dataclasses import dataclass,field
import os
from .ConfigMod.ReadConfig import config
from . import settings

@dataclass
class oracledb:
    class_type:str
    dbname  :str 
    dbuser  :str 
    host    :str 
    password:str 
    port    :int = 1521
    sid     :str =field(init=False)

    def __post_init__(self):
        self.sid = f"{self.host}:{self.port}/{self.dbname}"
    def connector(self):
        return {
            'user':self.dbuser,
            'password':self.password,
            'sid':self.sid
            }


@dataclass
class postgresdb:
    class_type:str
    dbname  :str 
    dbuser  :str 
    host    :str 
    password:str 
    port    :int = 5432

    def connector(self):
        return {
            'user':self.dbuser,
            'password':self.password,
            'host':self.host,
            'database':self.dbname,
            'port':self.port
            }


def loadconfig():
    """
    load configuration from configuration file location in the settings
    
    """
    conf = config(settings.CONFIG_DIR)
    conf.postgresdb:postgresdb = None
    # conf.oracledb:oracledb = None
    conf.register([postgresdb,oracledb])
    conf.load()
    pgenvconf = {}
    pgenvconf['database']  = os.environ.get('database',conf.postgresdb.database if conf.postgresdb else '')
    pgenvconf['user']      = os.environ.get('dbuser',conf.postgresdb.user if conf.postgresdb else '')
    pgenvconf['host']      = os.environ.get('dbhost',conf.postgresdb.host if conf.postgresdb else '')
    pgenvconf['password']  = os.environ.get('dbpassword',conf.postgresdb.password if conf.postgresdb else '')
    pgenvconf['port']      = os.environ.get('dbport',conf.postgresdb.port if conf.postgresdb else '5432')
    try:
        pgenvconfmodel = postgresdb(**pgenvconf)
        conf.postgresdb = pgenvconfmodel
    except :
        pass

    print('Configuration Loaded..')
    return conf

conf = loadconfig()





