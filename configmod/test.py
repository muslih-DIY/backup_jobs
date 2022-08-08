from ReadConfig import config
conf = config()
print(conf.LocalPGConfig)
print(conf.OracleConfig.__dict__)
input('waiting please enter')
conf.load()
print(conf.LocalPGConfig)
print(conf.OracleConfig.__dict__)