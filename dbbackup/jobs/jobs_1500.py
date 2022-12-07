
from dataclasses import dataclass
from ..dbbackups import BackupJobs,dbmodel
from ..settings import FAILED_BKP_DIR

@dataclass
class CDR_1500(BackupJobs):
    FromDb:str = 'localpg'
    ToDb :str = 'oraclesdc'    
    FromTable: str  =   'cdr'
    ToTable: str    =   'CDR_1500'
    ClassOfJob:str = '1500'  
    BACKUPDIR: str = FAILED_BKP_DIR  
    def __post_init__(self):

        self.FromColumn =[
            'CALLDATE','CONTAINER_ID','CLID','SRC','DST','DCONTEXT','CHANNEL','DSTCHANNEL','LASTAPP','LASTDATA','DURATION',
            'BILLSEC','DISPOSITION','AMAFLAGS','ACCOUNTCODE','UNIQUEID','USERFIELD','PHONENO','CALLSTAGES','ZONE','REGION_NO',
            'STD_CODE','DOCKET_ID','COMP_FLAG','STD_CODE_BILL','PHONENO_BILL','BILL_FLAG','AGENT_REGION','AGENT_LANG',
            'AGENT_SERVICE','AGENT_PURPOSE','AGENT_CUSCAT','AGENT_CLID','AGENT_TELNO','AGENT_ORGTIME','CDRID_AGENT','REPTIME_AGENT',
            'ZONE_BILL','CHANGED_PHONE','TRANSID']
        self.ToColumn   =   ['ENTRY_DATE']+self.FromColumn

        self.fromcol  =   ','.join(self.FromColumn)

        self.unique_keys = list(map(lambda x : str.lower(x),['UNIQUEID','TRANSID'] ))

        unique_key_str = ' and '.join([f"{key}=%({key})s" for key in self.unique_keys])

        self.pre_pull_querys = (f'update {self.FromTable} set deletflag=1 where deletflag=0',)

        self.pull_query=f"select now()::timestamp(6),{self.fromcol} from {self.FromTable} where deletflag=1"

        self.error_update_query = ' where '.join([f'update {self.FromTable} set deletflag=3 ',unique_key_str]) 
        
        self.final_post_push_querys = (f"delete from {self.FromTable} where deletflag=1",) 
        
        super().__post_init__()



@dataclass
class CDR_1500_BILL(BackupJobs):
    FromDb:str = 'localpg'
    ToDb :str = 'oraclesdc'    
    FromTable: str  =   'ivrs_bill_api'
    ToTable: str    =   'BILL_API_1500'
    ClassOfJob:str = '1500'
    BACKUPDIR: str = FAILED_BKP_DIR  
    def __post_init__(self):

        self.FromColumn =['API_DATE','ZONE','API_HIT','INPUT_NUMBER','FLAG','RESPONSE','TRANSID']

        self.ToColumn   =   ['ENTRY_DATE']+self.FromColumn

        self.fromcol  =   ','.join(self.FromColumn)

        self.unique_keys = list(map(lambda x : str.lower(x),['API_DATE','TRANSID'] ))

        unique_key_str = ' and '.join([f"{key}=%({key})s" for key in self.unique_keys])

        self.pre_pull_querys = (f'update {self.FromTable} set deletflag=1 where deletflag=0',)

        self.pull_query=f"select now()::timestamp(6),{self.fromcol} from {self.FromTable} where deletflag=1"

        self.error_update_query = ' where '.join([f'update {self.FromTable} set deletflag=3 ',unique_key_str]) 
        
        self.final_post_push_querys = (f"delete from {self.FromTable} where deletflag=1",) 
        
        super().__post_init__()
