import configparser
from pathlib import Path
import os
from dataclasses import dataclass,field


class config:

    @dataclass
    class _oracledb:
        class_type:str
        dbname  :str 
        dbuser  :str 
        host    :str 
        password:str 
        port    :int = 1521
        sid     :str =field(init=False)

        def __post_init__(self):
            self.sid = f"{self.host}:{self.port}/{self.dbname}"
    
    @dataclass
    class _postgresdb:
        class_type:str
        dbname  :str 
        dbuser  :str 
        host    :str 
        password:str 
        port    :int = 5432
    
    _config_class = {
        'oracledb':_oracledb,
        'postgresdb':_postgresdb
        }

    def __init__(self,path: str=None) -> None:
        if path==None:
            self.PATH = os.path.join(Path(__file__).resolve().parent, 'config.ini')
        
        self.load()
    

    def load(self):
        configfile = self.readConfigDict()
        for conf_name,conf in configfile.items():
            setattr(self,conf_name,self._config_class[conf['class_type']](**conf))


    def readConfig(self,filename=None):
        if filename is None:filename=self.PATH
        config = configparser.ConfigParser()
        config.read(filename)
        return config

    def readConfigDict(self,filename=None):
        if filename is None:filename=self.PATH
        config:configparser.ConfigParser = configparser.ConfigParser()
        config.read(filename)
        return config._sections       
