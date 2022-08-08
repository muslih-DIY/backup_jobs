from ..dbwrapers import or_wraper,pg_wraper
from ..configmod import config

conf = config()

pgcon = {
        'user':conf.LocalPGConfig.dbuser,
        'password':conf.LocalPGConfig.password,
        'host':conf.LocalPGConfig.host,
        'database':conf.LocalPGConfig.dbname,
        'port':conf.LocalPGConfig.port}

orcon = {
        'user':conf.OracleConfig.dbuser,
        'password':conf.OracleConfig.password,
        'sid':conf.OracleConfig.sid}        

localpg = pg_wraper.pg2_base_wrap(pgcon)

oraclesdc = or_wraper.oracle_base_wrap(orcon)