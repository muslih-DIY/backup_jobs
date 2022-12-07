from dbbackup import backup
from datetime import datetime
import sys

if __name__=='__main__':
    args = sys.argv[1:]
    if not args:
        print("Please pass arguments like  details,execute")
        exit()
    print('TIME START :',str(datetime.now()))
    op = args[0]
    try:
        jobclass = args[1]
    except:
        jobclass = None
    
    getattr(backup,op)((jobclass,) if jobclass else None) 

    print('TIME END :',str(datetime.now()))