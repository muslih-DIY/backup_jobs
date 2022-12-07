from ReadConfig import config
from dataclasses import dataclass,field



conf = config()
conf.register([_oracledb,_postgresdb])
conf.load()
print(conf.postgresdb.port)
print(conf.oracledbsdc.__repr__())
print(conf.glob.ip)
