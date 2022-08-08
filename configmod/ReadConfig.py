import configparser
from pathlib import Path
import os
from dataclasses import dataclass,field

class config:
    
    def __init__(self,path: str=None) -> None:
        if path==None:
            self.PATH = os.path.join(Path(__file__).resolve().parent, 'config.ini')
        
        self.load()
    
    def load(self):
        configfile = self.readConfigDict()
        self._OracleConfig = self._oracledb(**configfile['oracledbsdc'])
        self._LocalPGConfig = self._postgresdb(**configfile['postgresdb'])

    @property
    def OracleConfig(self):
        return self._OracleConfig

    @property
    def LocalPGConfig(self):
        return self._LocalPGConfig 

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
        
    @dataclass
    class _oracledb:
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
        dbname  :str 
        dbuser  :str 
        host    :str 
        password:str 
        port    :int = 5432
    