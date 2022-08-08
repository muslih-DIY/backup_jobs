from .BackupBase import BackupJobs
from dataclasses import dataclass
from .backup_settings import localpg,oraclesdc

@dataclass
class cdr_1500(BackupJobs):
    FromTable: str  =   'cdr_bakup'
    ToTable: str    =   'TEST_CDR_1500'
    def __post_init__(self):
        self.FromDb = localpg
        self.ToDb = oraclesdc
        self.FromColumn = [
        'CALLDATE','CONTAINER_ID','CLID','SRC','DST','DCONTEXT','CHANNEL',
        'DSTCHANNEL','LASTAPP','LASTDATA','DURATION','BILLSEC','DISPOSITION','AMAFLAGS',
        'ACCOUNTCODE','UNIQUEID','USERFIELD','PHONENO','CALLSTAGES','ZONE','REGION_NO',
        'STD_CODE','DOCKET_ID','COMP_FLAG','STD_CODE_BILL','PHONENO_BILL','BILL_FLAG',
        'AGENT_REGION','AGENT_LANG','AGENT_SERVICE','AGENT_PURPOSE','AGENT_CUSCAT',
        'AGENT_CLID','AGENT_TELNO','AGENT_ORGTIME','CDRID_AGENT','REPTIME_AGENT',
        'ZONE_BILL','CHANGED_PHONE','TRANSID']
        self.ToColumn   =   ['ENTRY_DATE']+self.FromColumn
        self.fromcol  =   ','.join(self.FromColumn)
        self.pre_pull_querys = (f'update {self.FromTable} set deletflag=1',)
        self.pull_query=f"select now()::timestamp(6),{self.fromcol} from {self.FromTable} where deletflag=1"
        self.error_update_query = f'update {self.FromTable} set deletflag=3'
        self.final_post_push_querys = (f'delete from {self.FromTable} where deletflag=1',) 
        self.unique_keys = ['uniqueid']  
        super().__post_init__()
           
        




        



