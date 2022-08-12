from bkpjobs import backup
from datetime import datetime
import sys
if __name__=='__main__':
    args = sys.argv[1:]
    if not args:
        print("Please pass arguments like  details,execute")
        exit()
    print('TIME START :',str(datetime.now()))
    for op in args:
        getattr(backup,op)()
    print('TIME END :',str(datetime.now()))