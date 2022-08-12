from ..dbwrapers import or_wraper,pg_wraper
from ..configmod.ReadConfig import config

conf = config()

pgcon = {
        'user':conf.postgresdb.dbuser,
        'password':conf.postgresdb.password,
        'host':conf.postgresdb.host,
        'database':conf.postgresdb.dbname,
        'port':conf.postgresdb.port}

orcon = {
        'user':conf.oracledbsdc.dbuser,
        'password':conf.oracledbsdc.password,
        'sid':conf.oracledbsdc.sid}        

localpg = pg_wraper.pg2_base_wrap(pgcon)

oraclesdc = or_wraper.oracle_base_wrap(orcon)